"""Destination identity and interrupted-backup cleanup safety properties."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time

import pytest

from storage.backup_remote import (
    BackupError, DEFAULT_OWNER, MANIFEST_SUFFIX, OWNER_MARKER, SnapshotTarget,
    owned_snapshots, prune_snapshots,
)

OWNER_A = hashlib.sha256(b"database-instance-a").hexdigest()
OWNER_B = hashlib.sha256(b"database-instance-b").hexdigest()


def name(index):
    return f"meshinfo-pg-20260101-000000-{index:012x}.dump"


def publish(directory, index, *, owner=OWNER_A, age_days=0):
    with SnapshotTarget(directory, name(index), 0, owner=owner) as target:
        target.write(b"synthetic archive bytes")
        target.seal()
        metadata = target.publish(now=time.time() - age_days * 86400)
    return directory / name(index), metadata


def test_owner_marker_is_durable_private_and_included_in_manifest(tmp_path):
    path, metadata = publish(tmp_path, 1)
    marker = tmp_path / OWNER_MARKER
    assert json.loads(marker.read_text()) == {"schema": 1, "owner": OWNER_A}
    assert stat.S_IMODE(marker.stat().st_mode) == 0o600
    assert metadata["owner"] == OWNER_A
    assert json.loads(path.with_name(path.name + MANIFEST_SUFFIX).read_text())["owner"] == OWNER_A
    assert owned_snapshots(tmp_path, owner=OWNER_A)[0][0] == path
    assert owned_snapshots(tmp_path, owner=OWNER_B) == []


def test_different_instance_cannot_write_or_reap_existing_partials(tmp_path):
    path, _ = publish(tmp_path, 1)
    partial = tmp_path / (name(2) + ".part")
    partial.write_bytes(b"interrupted archive")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    with pytest.raises(BackupError, match="different database instance"):
        with SnapshotTarget(tmp_path, name(3), 0, owner=OWNER_B):
            pass
    after = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    assert after == before
    assert path.exists() and partial.exists()


def test_missing_owner_marker_cannot_adopt_verified_foreign_snapshots(tmp_path):
    archive, _ = publish(tmp_path, 1)
    (tmp_path / OWNER_MARKER).unlink()
    with pytest.raises(BackupError, match="another database instance"):
        with SnapshotTarget(tmp_path, name(2), 0, owner=OWNER_B):
            pass
    assert archive.exists()
    assert not (tmp_path / OWNER_MARKER).exists()
    with SnapshotTarget(tmp_path, name(2), 0, owner=OWNER_A):
        pass
    assert json.loads((tmp_path / OWNER_MARKER).read_text())["owner"] == OWNER_A


def test_pruning_preserves_other_instance_and_ownerless_manifests(tmp_path):
    directory = tmp_path / "destination"
    old, _ = publish(directory, 1, age_days=20)
    latest, _ = publish(directory, 2)
    ownerless, _ = publish(directory, 3, age_days=20)
    marker = ownerless.with_name(ownerless.name + MANIFEST_SUFFIX)
    metadata = json.loads(marker.read_text())
    metadata.pop("owner")
    marker.write_text(json.dumps(metadata))
    foreign, _ = publish(tmp_path / "foreign", 4, owner=OWNER_B, age_days=20)
    shutil.copyfile(foreign, directory / foreign.name)
    shutil.copyfile(foreign.with_name(foreign.name + MANIFEST_SUFFIX), directory / (foreign.name + MANIFEST_SUFFIX))
    assert len(owned_snapshots(directory)) == 4
    assert prune_snapshots(directory, 4, owner=OWNER_A) == [old.name]
    assert latest.exists() and ownerless.exists() and (directory / foreign.name).exists()


def test_crash_partials_are_reaped_after_lock_before_space_reserve_check(tmp_path, monkeypatch):
    from storage import backup_remote

    publish(tmp_path, 1)
    partial = tmp_path / (name(2) + ".part")
    partial.write_bytes(b"interrupted data" * 100)
    partial_marker = tmp_path / (name(2) + MANIFEST_SUFFIX + ".part")
    partial_marker.write_bytes(b"unfinished marker")
    size = partial.stat().st_size + partial_marker.stat().st_size
    original = SnapshotTarget._check_space

    def space_after_cleanup(self, incoming):
        assert not partial.exists() and not partial_marker.exists()
        return original(self, incoming)

    monkeypatch.setattr(SnapshotTarget, "_check_space", space_after_cleanup)
    with SnapshotTarget(tmp_path, name(3), 0, owner=OWNER_A) as target:
        assert target.orphan_files_removed == 2
        assert target.orphan_bytes_removed == size
        target.write(b"new archive")
        target.seal()
        metadata = target.publish()
        assert metadata["orphan_files_removed"] == 2
        assert metadata["orphan_bytes_removed"] == size
    assert not list(tmp_path.glob("*.part"))


def test_partial_cleanup_preserves_symlinks_directories_unknown_names_and_final_archive(tmp_path):
    archive, _ = publish(tmp_path, 1)
    outside = tmp_path / "precious.bin"
    outside.write_bytes(b"preserve me")
    linked_partial = tmp_path / (name(2) + ".part")
    linked_partial.symlink_to(outside)
    partial_directory = tmp_path / (name(3) + ".part")
    partial_directory.mkdir()
    unrelated = tmp_path / "meshinfo-pg-unrecognized.dump.part"
    unrelated.write_bytes(b"unowned")
    legacy = tmp_path / "meshinfo-pg-20260101-000000.dump.tmp"
    legacy.write_bytes(b"legacy")
    final_marker = archive.with_name(archive.name + MANIFEST_SUFFIX)
    before = final_marker.read_bytes()
    with SnapshotTarget(tmp_path, name(4), 0, owner=OWNER_A) as target:
        assert target.orphan_files_removed == 0
    assert archive.exists() and final_marker.read_bytes() == before
    assert outside.read_bytes() == b"preserve me"
    assert linked_partial.is_symlink() and partial_directory.is_dir()
    assert unrelated.exists() and legacy.exists()


def test_crashed_publication_hardlinks_keep_final_archive_without_false_reclaimed_bytes(tmp_path):
    archive, _ = publish(tmp_path, 1)
    partial = tmp_path / (archive.name + ".part")
    marker = archive.with_name(archive.name + MANIFEST_SUFFIX)
    partial_marker = marker.with_name(marker.name + ".part")
    os.link(archive, partial)
    os.link(marker, partial_marker)
    with SnapshotTarget(tmp_path, name(2), 0, owner=OWNER_A) as target:
        assert target.orphan_files_removed == 2
        assert target.orphan_bytes_removed == 0
    assert archive.read_bytes() == b"synthetic archive bytes"
    assert marker.exists() and not partial.exists() and not partial_marker.exists()


def test_active_destination_lock_prevents_cleanup_of_an_active_archive(tmp_path):
    with SnapshotTarget(tmp_path, name(1), 0, owner=OWNER_A) as active:
        active.write(b"active data")
        with pytest.raises(BlockingIOError):
            with SnapshotTarget(tmp_path, name(2), 0, owner=OWNER_A):
                pass
        assert active.temporary.exists()
    assert not list(tmp_path.glob("*.part"))


@pytest.mark.parametrize("kind", ["invalid_json", "symlink", "directory", "fifo", "oversized"])
def test_invalid_owner_markers_block_before_any_partial_cleanup(tmp_path, kind):
    marker = tmp_path / OWNER_MARKER
    if kind == "symlink":
        destination = tmp_path / "elsewhere.json"
        destination.write_text(json.dumps({"schema": 1, "owner": OWNER_A}))
        marker.symlink_to(destination)
    elif kind == "directory":
        marker.mkdir()
    elif kind == "fifo":
        os.mkfifo(marker)
    else:
        marker.write_text("not-json" if kind == "invalid_json" else " " * 4097)
    partial = tmp_path / (name(1) + ".part")
    partial.write_bytes(b"preserve until ownership established")
    with pytest.raises(BackupError):
        with SnapshotTarget(tmp_path, name(2), 0, owner=OWNER_A):
            pass
    assert partial.exists()


@pytest.mark.parametrize("owner", ["", "f" * 63, "F" * 64, "g" * 64, None, 123])
def test_bad_owner_fingerprint_is_rejected_before_filesystem_mutation(tmp_path, owner):
    with pytest.raises(ValueError, match="owner"):
        SnapshotTarget(tmp_path, name(1), 0, owner=owner)
    assert list(tmp_path.iterdir()) == []


def test_legacy_low_level_default_is_explicitly_namespaced(tmp_path):
    with SnapshotTarget(tmp_path, name(1), 0) as target:
        target.write(b"low-level compatibility")
        target.seal()
        metadata = target.publish()
    assert metadata["owner"] == DEFAULT_OWNER
    assert json.loads((tmp_path / OWNER_MARKER).read_text())["owner"] == DEFAULT_OWNER


def test_unpublished_owner_temporary_is_cleaned_only_after_owner_claim(tmp_path):
    temporary = tmp_path / (".meshinfo-backup-owner-" + "a" * 32 + ".json.part")
    temporary.write_bytes(b"interrupted initial owner marker")
    with SnapshotTarget(tmp_path, name(1), 0, owner=OWNER_A) as target:
        assert target.orphan_files_removed == 1
        assert not temporary.exists()
    assert json.loads((tmp_path / OWNER_MARKER).read_text())["owner"] == OWNER_A
