export type HistoryScope =
  | "packets"
  | "telemetry"
  | "chat"
  | "traceroutes"
  | "neighbors";

/** Public history metadata only; storage capacity and paths stay operator-only. */
export interface IHistoryPolicy {
  /** Zero means no age-based expiration. */
  months: number;
  cutoff: string | null;
  effective_cutoff: string | null;
  oldest_available: string | null;
  newest_available: string | null;
  available_bounds_kind: "partition" | "exact" | "unknown";
}

export interface IStorageHistory {
  generated_at: string;
  enforcement_enabled: boolean;
  configured_enabled: boolean;
  granularity: "calendar_month";
  scopes: Record<HistoryScope, IHistoryPolicy>;
  archive?: {
    enabled: boolean;
    after_months: number;
    compressed_partitions: number;
    history_remains_queryable: boolean;
  };
  maintenance?: Record<string, {
    updated_at: string;
    status: string;
    scopes: Partial<Record<HistoryScope, { status: string }>>;
  }>;
}
