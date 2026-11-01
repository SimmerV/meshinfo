import { afterEach, describe, expect, it, vi } from "vitest";

vi.hoisted(() => {
  (globalThis as { __env__?: object }).__env__ = { VITE_API_BASE_URL: "http://localhost" };
});

import {
  cleanup,
  mountAt,
  requestsTo,
  stubApi,
} from "../pages/__tests__/channelPillsHarness";
import type { IHistoryPolicy, IStorageHistory } from "../types/storage";
import { HistoryRetentionPanel, RetentionNotice } from "./RetentionNotice";

const forever: IHistoryPolicy = {
  months: 0,
  cutoff: null,
  effective_cutoff: null,
  oldest_available: null,
  newest_available: null,
  available_bounds_kind: "unknown",
};

function history(overrides: Partial<IStorageHistory> = {}): IStorageHistory {
  return {
    generated_at: "2026-10-27T12:00:00Z",
    enforcement_enabled: false,
    configured_enabled: false,
    granularity: "calendar_month",
    scopes: {
      packets: { ...forever },
      telemetry: { ...forever },
      chat: { ...forever },
      traceroutes: { ...forever },
      neighbors: { ...forever },
    },
    ...overrides,
  };
}

afterEach(() => cleanup());

describe("History retention visibility", () => {
  it("shows lossless compression separately from forever retention", async () => {
    stubApi({ history: history({
      archive: { enabled: true, after_months: 1, compressed_partitions: 3, history_remains_queryable: true },
      maintenance: { archive: { status: "blocked", updated_at: "2026-10-27T12:00:00Z", scopes: {} } },
    }) });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.querySelector("summary")?.textContent).toBe("History retention: Forever");
    expect(document.body.textContent).toContain("Compression does not delete history");
    expect(document.body.textContent).toContain("3 historical months are compressed");
    expect(document.body.textContent).toContain("Existing history remains available");
  });

  it("keeps compressed history visible when future compression is disabled", async () => {
    stubApi({ history: history({
      archive: { enabled: false, after_months: 6, compressed_partitions: 1, history_remains_queryable: true },
    }) });
    await mountAt("/stats", <HistoryRetentionPanel />);
    expect(document.body.textContent).toContain("Automatic history compression is off");
    expect(document.body.textContent).toContain("1 historical month is compressed");
    expect(document.body.textContent).toContain("Compressed history stays available in the usual searches");
  });

  it.each([true, false])("shows pending cleanup without exposing operator details (enabled=%s)", async (enabled) => {
    const fixture = history({
      archive: { enabled, after_months: 6, compressed_partitions: 0, history_remains_queryable: true },
      maintenance: { archive: { status: "cleanup_pending", updated_at: "2026-10-27T12:00:00Z", scopes: {} } },
    });
    // Even an unexpectedly overbroad response must not render operator fields.
    Object.assign(fixture.maintenance!.archive, {
      job_id: "private-job", admission_free_bytes: 123456789,
      cleanup_reason: "private-capacity-reason", source_name: "private-month",
    });
    stubApi({ history: fixture });
    await mountAt("/stats", <HistoryRetentionPanel />);
    const text = document.body.textContent ?? "";
    expect(text).toContain("History compression cleanup is pending. Existing history remains available.");
    expect(text).not.toContain("needs operator attention");
    for (const privateValue of ["cleanup_pending", "private-job", "123456789", "private-capacity-reason", "private-month"]) {
      expect(text).not.toContain(privateValue);
    }
  });

  it("describes forever as a policy without promising that all past data exists", async () => {
    stubApi({ history: history() });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.querySelector("summary")?.textContent).toBe("History retention: Forever");
    expect(document.body.textContent).toContain("No age-based expiration");
    expect(document.body.textContent).toContain("disk use to grow");
    expect(document.body.textContent).not.toContain("Oldest available");
    expect(document.body.textContent).not.toContain("preview only");
  });

  it("shows a finite policy as preview while enforcement is unavailable, even when configured on", async () => {
    const fixture = history({ configured_enabled: true });
    fixture.scopes.packets = {
      ...forever,
      months: 6,
      cutoff: "2026-04-27T12:00:00Z",
      effective_cutoff: "2026-04-01T00:00:00Z",
    };
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.querySelector("summary")?.textContent).toContain("6 calendar months · preview only");
    expect(document.body.textContent).toContain("automatic expiration is off");
    expect(document.body.textContent).toContain("up to one additional month");
    expect(document.body.textContent).toContain("Packets first received before April 1, 2026 (UTC) are eligible");
  });

  it("explains that later reception copies share a finite packet policy's expiration", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets = {
      ...forever,
      months: 6,
      cutoff: "2026-04-27T12:00:00Z",
      effective_cutoff: "2026-04-01T00:00:00Z",
    };
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    const text = document.body.textContent ?? "";
    expect(text).toContain("Packets first received before April 1, 2026 (UTC) are eligible");
    expect(text).toContain("All reception copies expire with their packet, including copies received after the cutoff");
    expect(text).toContain("A later copy does not extend the packet’s retention");
    expect(text).not.toContain("Only history before");
  });

  it("distinguishes packet groups from individual records in a mixed-policy overview", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets.months = 3;
    fixture.scopes.telemetry.months = 6;
    stubApi({ history: fixture });
    await mountAt("/stats", <HistoryRetentionPanel />);
    const text = document.body.textContent ?? "";
    expect(text).toContain("Packet age starts at its first receipt on this server");
    expect(text).toContain("All reception copies expire with their packet, including copies received after the cutoff");
    expect(text).toContain("Other history uses each record’s receipt time");
  });

  it("reads the selected scope override and the backend enforcement state", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.chat.months = 1;
    fixture.scopes.packets.months = 12;
    stubApi({ history: fixture });
    await mountAt("/chat", <RetentionNotice scope="chat" />);
    expect(document.querySelector("summary")?.textContent).toBe("History retention: 1 calendar month");
    expect(document.body.textContent).toContain("Automatic expiration is enabled");
    expect(document.body.textContent).not.toContain("12 calendar months");
    expect(document.body.textContent).not.toContain("preview only");
  });

  it("labels partition estimates and formats month boundaries in UTC", async () => {
    const fixture = history();
    fixture.scopes.packets = {
      ...forever,
      oldest_available: "2026-08-31T17:00:00-07:00",
      newest_available: "2026-11-01T00:00:00Z",
      available_bounds_kind: "partition",
    };
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.body.textContent).toContain("Estimated oldest storage month: September 2026");
    expect(document.body.textContent).toContain("records may begin later");
    expect(document.body.textContent).not.toContain("Oldest available");
    expect(document.body.textContent).not.toContain("August");
  });

  it("only calls exact bounds oldest available and hides unmeasured or invalid bounds", async () => {
    const fixture = history();
    fixture.scopes.chat = {
      ...forever,
      oldest_available: "2026-08-31T17:00:00-07:00",
      available_bounds_kind: "exact",
    };
    fixture.scopes.packets.oldest_available = "2020-01-01T00:00:00Z";
    fixture.scopes.telemetry = {
      ...forever,
      oldest_available: "not-a-date",
      available_bounds_kind: "exact",
    };
    stubApi({ history: fixture });
    await mountAt("/stats", <HistoryRetentionPanel />);
    expect(document.body.textContent).toContain("Oldest available: September 1, 2026 (UTC)");
    expect(document.body.textContent).not.toContain("2020");
    expect(document.body.textContent).not.toContain("Invalid Date");
  });

  it("shares one request among notices and lists policies for all history families", async () => {
    const fixture = history();
    fixture.scopes.packets.months = 6;
    stubApi({ history: fixture });
    await mountAt("/stats", <><RetentionNotice scope="packets" /><RetentionNotice scope="chat" /><HistoryRetentionPanel /></>);
    expect(requestsTo("/v1/storage/history")).toHaveLength(1);
    const rows = document.querySelectorAll("dt");
    expect([...rows].map((row) => row.textContent)).toEqual([
      "Packets and receptions", "Telemetry", "Chat", "Traceroutes", "Neighbor history",
    ]);
    expect(document.body.textContent).toContain("No history is deleted by these policies");
    expect(document.body.textContent).toContain("Current node state is retained separately");
  });

  it.each([404, 503])("keeps the surrounding page available when metadata responds %s", async (historyStatus) => {
    stubApi({ historyStatus });
    await mountAt("/logs", <><p>Packet search remains here</p><RetentionNotice scope="packets" /><HistoryRetentionPanel /></>);
    expect(document.body.textContent).toContain("Packet search remains here");
    expect(document.body.textContent).toContain("History retention information is unavailable");
    expect(document.body.textContent).toContain("does not change the configured retention policy");
    expect(document.body.textContent).not.toContain("History retention: Forever");
  });

  it("reports that enabled expiration has not completed a cycle yet", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets.months = 6;
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.body.textContent).toContain("has not reported a completed cycle yet");
  });

  it("shows a blocked scope without suggesting successful deletion", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets.months = 6;
    fixture.maintenance = {
      retention: {
        updated_at: "2026-10-27T12:00:00Z",
        status: "partial",
        scopes: { packets: { status: "blocked" }, telemetry: { status: "complete" } },
      },
    };
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.body.textContent).toContain("Expiration maintenance: blocked");
    expect(document.body.textContent).toContain("Some older history may remain");
    expect(document.body.textContent).not.toContain("Expiration maintenance: partial");
  });

  it("does not show another scope's maintenance failure for a forever policy", async () => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets.months = 6;
    fixture.maintenance = {
      retention: { updated_at: "2026-10-27T12:00:00Z", status: "error", scopes: {} },
    };
    stubApi({ history: fixture });
    await mountAt("/chat", <RetentionNotice scope="chat" />);
    expect(document.querySelector("summary")?.textContent).toBe("History retention: Forever");
    expect(document.body.textContent).not.toContain("Expiration maintenance");
  });

  it("hides old maintenance failures when expiration has been disabled", async () => {
    const fixture = history();
    fixture.scopes.packets.months = 6;
    fixture.maintenance = {
      retention: { updated_at: "2026-10-27T12:00:00Z", status: "error", scopes: {} },
    };
    stubApi({ history: fixture });
    await mountAt("/stats", <HistoryRetentionPanel />);
    expect(document.body.textContent).toContain("Preview only");
    expect(document.body.textContent).not.toContain("Expiration maintenance");
  });

  it.each([
    ["2026-10-27T11:57:00Z", false],
    ["2026-10-27T11:56:59Z", true],
  ])("checks maintenance freshness against the report's server clock (%s)", async (updated_at, stale) => {
    const fixture = history({ enforcement_enabled: true, configured_enabled: true });
    fixture.scopes.packets.months = 6;
    fixture.maintenance = {
      retention: { updated_at, status: "complete", scopes: { packets: { status: "complete" } } },
    };
    stubApi({ history: fixture });
    await mountAt("/logs", <RetentionNotice scope="packets" />);
    expect(document.body.textContent?.includes("No recent expiration maintenance result")).toBe(stale);
  });
});
