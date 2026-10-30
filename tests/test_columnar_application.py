"""Independent oracle and failure-detection tests for the isolated API verifier."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging

import pytest

from scripts.verify_columnar_application import (
    StorageWarnings, VerificationFailure, checked_call, plan_cases,
    synthetic_originals, verify_variant,
)


UTC = timezone.utc
HOT = datetime(2020, 5, 1, tzinfo=UTC)
NODE = "c0decafe"
OTHER = "deadbeef"
TOPIC = "msh/prototype/%_\\literal/!c0decafe"
MARKER = "columnar-reception-only-witness"


def packet(identity, stamp, payload=None, node=NODE, recipient=OTHER, mesh_id=4242424242):
    if payload is None:
        payload = json.dumps({"from": int(NODE, 16), "to": int(OTHER, 16),
                              "id": mesh_id, "payload": {"text": "Unicode 雪 🛰️", "nullable": None}})
    return (identity, TOPIC, payload, 0, False, int(stamp.timestamp()), stamp, node, recipient, mesh_id)


@pytest.fixture
def fixture():
    packets = [packet(index+1, HOT-timedelta(days=1)+timedelta(seconds=index)) for index in range(220)]
    packets += [packet(index+221, HOT+timedelta(seconds=index)) for index in range(220)]
    ids = {"cross_month": 8001, "self_addressed_tie_a": 8002,
           "self_addressed_tie_b": 8003, "reused_new": 8004,
           "malformed_payload": 8005, "scalar_payload": 8006, "null_payload": 8007}
    packets += [packet(ids["cross_month"], HOT-timedelta(microseconds=1)),
                packet(ids["self_addressed_tie_a"], HOT-timedelta(days=3), recipient=NODE, mesh_id=4242424243),
                packet(ids["self_addressed_tie_b"], HOT-timedelta(days=3), recipient=NODE, mesh_id=4242424243),
                packet(ids["reused_new"], HOT+timedelta(days=2)),
                packet(ids["malformed_payload"], HOT-timedelta(days=2), payload='{"from": malformed'),
                packet(ids["scalar_payload"], HOT-timedelta(days=2), payload='42')]
    null_packet = list(packet(ids["null_payload"], HOT-timedelta(days=2)))
    null_packet[2] = None
    packets.append(tuple(null_packet))
    packets.sort(key=lambda row: (row[6], row[0]))
    patch = {"set": {"rxRssi": None, "prototype_note": '雪 quoted "value"\n', "nullable": None}, "del": ["to"]}
    receptions = [
        (ids["cross_month"], HOT+timedelta(seconds=1), OTHER, 2, -91, 4.25,
         int(HOT.timestamp())+1, 3, 1, None, 0, json.dumps(patch)),
        (ids["self_addressed_tie_a"], HOT-timedelta(days=3)+timedelta(seconds=1), OTHER, 1,
         None, None, None, None, None, None, None, None),
    ]
    topics = [(1, TOPIC), (2, "msh/prototype/"+MARKER+"/!deadbeef")]
    manifest = {"synthetic_witnesses": ids, "synthetic_packet_ids": list(ids.values()),
                "reception_lookback_seconds": 86400,
                "query_terms": {"node_id": NODE, "sender_node_id": NODE,
                                "sender_packet_id": 4242424242, "topic_literal": TOPIC,
                                "receipt_topic_search": MARKER}}
    return packets, receptions, topics, manifest


def test_reconstructed_copy_oracle_preserves_patch_null_unicode_deletion_and_zero(fixture):
    packets, receptions, topics, manifest = fixture
    copies = synthetic_originals(packets, receptions, topics, manifest)
    copy = copies[str(manifest["synthetic_witnesses"]["cross_month"])][0]
    assert "to" not in copy
    assert copy["nullable"] is None and copy["rxRssi"] is None
    assert copy["prototype_note"] == '雪 quoted "value"\n'
    assert copy["transport_mechanism"] == 0
    assert copy["rx_rssi"] == -91 and copy["rx_snr"] == 4.25
    assert "topic" not in copy and "sender" not in copy


def test_flat_extras_cannot_falsely_prove_patch_reconstruction(fixture):
    packets, receptions, topics, manifest = fixture
    invalid = list(receptions[0])
    invalid[-1] = json.dumps({"prototype_note": "ignored without a set patch"})
    with pytest.raises(VerificationFailure, match="patch changed"):
        synthetic_originals(packets, [tuple(invalid), receptions[1]], topics, manifest)


def test_query_oracles_require_receipt_search_and_exact_scalar_decoding(fixture):
    packets, receptions, topics, manifest = fixture
    cases, windows, _counts = plan_cases(packets, manifest, HOT, receptions, topics)
    by_name = {case["name"]: case for case in cases}
    assert by_name["archive_reception_only_search"]["expected_ids"] == [8001]
    scalar = next(case for case in cases if case["expected_packet"] == 8006)
    malformed = next(case for case in cases if case["expected_packet"] == 8005)
    null = next(case for case in cases if case["expected_packet"] == 8007)
    assert scalar["expected_base"]["raw"] == 42
    assert malformed["expected_base"]["raw"] == '{"from": malformed'
    assert "raw" not in null["expected_base"]
    assert windows["tied_timestamp_pagination"]["ids"] == [8003, 8002]
    assert windows["tied_timestamp_pagination"]["limit"] == 1
    assert len(set(windows["boundary_pagination"]["ids"])) == len(windows["boundary_pagination"]["ids"])


def test_receipt_only_search_rejects_canonical_marker_contamination(fixture):
    packets, receptions, topics, manifest = fixture
    changed = list(packets[0])
    changed[2] = json.dumps({"payload": MARKER})
    with pytest.raises(VerificationFailure, match="canonical topic/payload"):
        plan_cases([tuple(changed), *packets[1:]], manifest, HOT, receptions, topics)


def test_swallowed_application_error_cannot_pass_as_empty_result():
    warnings = StorageWarnings()

    class SwallowingStorage:
        async def query_mqtt_messages(self):
            warnings.emit(logging.LogRecord("storage.db.postgres", logging.ERROR, "", 0,
                                            "query failed", (), None))
            return {"messages": [], "next_cursor": None}

    with pytest.raises(VerificationFailure, match="warning/error"):
        asyncio.run(checked_call(SwallowingStorage(), "query_mqtt_messages", {}, warnings, "absent_search"))


def test_pagination_oracle_rejects_duplicate_missing_rows():
    class BrokenPagination:
        async def query_mqtt_messages(self, **arguments):
            return {"messages": [{"mqtt_row_id": 2}, {"mqtt_row_id": 2}], "next_cursor": None}

    windows = {"boundary_pagination": {"start": HOT, "end": HOT, "limit": 200, "ids": [2, 1]}}
    with pytest.raises(VerificationFailure, match="lost, duplicated or reordered"):
        asyncio.run(verify_variant(BrokenPagination(), [], windows, StorageWarnings(), {}))


def test_failed_case_is_preserved_while_later_case_still_runs():
    class PartlyBrokenPagination:
        async def query_mqtt_messages(self, **arguments):
            identities = [2, 2] if arguments["start"] == HOT else [2, 1]
            return {"messages": [{"mqtt_row_id": identity} for identity in identities], "next_cursor": None}

    windows = {
        "broken": {"start": HOT, "end": HOT, "limit": 200, "ids": [2, 1]},
        "working": {"start": HOT+timedelta(seconds=1), "end": HOT, "limit": 200, "ids": [2, 1]},
    }
    report = asyncio.run(verify_variant(PartlyBrokenPagination(), [], windows,
                                       StorageWarnings(), {}, keep_going=True))
    assert report["broken"]["failed"] is True
    assert "sha256" not in report["broken"]
    assert report["working"]["rows"] == 2
    assert len(report["working"]["sha256"]) == 64
