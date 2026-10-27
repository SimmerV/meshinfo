import { useGetStorageHistoryQuery } from "../slices/apiSlice";
import type { HistoryScope, IHistoryPolicy, IStorageHistory } from "../types/storage";

const SCOPE_LABELS: Record<HistoryScope, string> = {
  packets: "Packets and receptions",
  telemetry: "Telemetry",
  chat: "Chat",
  traceroutes: "Traceroutes",
  neighbors: "Neighbor history",
};

const QUERY_OPTIONS = {
  pollingInterval: 300_000,
  skipPollingIfUnfocused: true,
  refetchOnMountOrArgChange: 300,
  refetchOnFocus: true,
  refetchOnReconnect: true,
};

function duration(policy: IHistoryPolicy): string {
  return policy.months === 0
    ? "Forever"
    : `${policy.months} calendar month${policy.months === 1 ? "" : "s"}`;
}

function utcDate(value: string | null, monthOnly = false): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  return date.toLocaleDateString(undefined, {
    timeZone: "UTC",
    year: "numeric",
    month: "long",
    ...(monthOnly ? {} : { day: "numeric" as const }),
  });
}

function oldestHistory(policy: IHistoryPolicy): string | null {
  if (policy.available_bounds_kind === "unknown") return null;
  const partition = policy.available_bounds_kind === "partition";
  const date = utcDate(policy.oldest_available, partition);
  if (!date) return null;
  return partition
    ? `Estimated oldest storage month: ${date} (UTC; records may begin later).`
    : `Oldest available: ${date} (UTC).`;
}

function PolicyDetails({
  history,
  policy,
}: {
  history: IStorageHistory;
  policy: IHistoryPolicy;
}) {
  const oldest = oldestHistory(policy);
  const effectiveCutoff = utcDate(policy.effective_cutoff);
  return (
    <div className="mt-2 space-y-1">
      <p>
        {policy.months === 0
          ? "No age-based expiration is configured for this history. Keeping history forever allows disk use to grow as data arrives."
          : history.enforcement_enabled
            ? `Keeps at least ${duration(policy).toLowerCase()}. Older history expires automatically.`
            : `Configured to keep at least ${duration(policy).toLowerCase()}. Preview only: automatic expiration is off.`}
      </p>
      {policy.months > 0 && history.granularity === "calendar_month" && (
        <p>
          Expiration follows UTC calendar-month boundaries, so history may be
          kept for up to one additional month.
          {effectiveCutoff && ` Only history before ${effectiveCutoff} (UTC) is eligible.`}
        </p>
      )}
      {oldest && <p>{oldest}</p>}
      <p>Retention is based on when this server received the data. Current node state is kept separately.</p>
    </div>
  );
}

/** A failed metadata request never blocks browsing the underlying history. */
export function RetentionNotice({ scope }: { scope: HistoryScope }) {
  const { data, isError } = useGetStorageHistoryQuery(undefined, QUERY_OPTIONS);
  const policy = data?.scopes?.[scope];
  if (isError) {
    return <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">History retention information is unavailable.</p>;
  }
  if (!data || !policy) return null;
  return (
    <details className="mt-1 max-w-prose text-xs text-gray-600 dark:text-gray-400">
      <summary className="cursor-pointer hover:text-gray-900 dark:hover:text-gray-200">
        History retention: {duration(policy)}
        {policy.months > 0 && !data.enforcement_enabled ? " · preview only" : ""}
      </summary>
      <PolicyDetails history={data} policy={policy} />
    </details>
  );
}

/** Central read-only overview, including history without a dedicated page. */
export function HistoryRetentionPanel() {
  const { data, isError, isLoading } = useGetStorageHistoryQuery(undefined, QUERY_OPTIONS);
  const hasFinitePolicy = Object.values(data?.scopes ?? {}).some((policy) => policy.months > 0);
  return (
    <section
      aria-labelledby="history-retention-title"
      className="mb-5 rounded-xl border border-gray-200 dark:border-gray-800 bg-white/70 dark:bg-gray-950/30 p-4 sm:p-5"
    >
      <h2 id="history-retention-title" className="text-base font-semibold text-gray-900 dark:text-gray-100">
        History retention
      </h2>
      <p className="mt-1 text-xs text-gray-600 dark:text-gray-400">
        Retained history remains available through the usual searches. Policies are set by this server’s operator.
      </p>
      {isError ? (
        <p className="mt-3 text-sm text-gray-600 dark:text-gray-400">Retention information is temporarily unavailable. This does not change the configured retention policy.</p>
      ) : isLoading ? (
        <p className="mt-3 text-sm text-gray-600 dark:text-gray-400">Loading retention policy…</p>
      ) : data?.scopes ? (
        <>
          {hasFinitePolicy && !data.enforcement_enabled && (
            <p className="mt-3 text-sm text-amber-700 dark:text-amber-300">Preview only: automatic expiration is off. No history is deleted by these policies.</p>
          )}
          <dl className="mt-3 divide-y divide-gray-200 dark:divide-gray-800">
            {(Object.keys(SCOPE_LABELS) as HistoryScope[]).map((scope) => {
              const policy = data.scopes[scope];
              if (!policy) return null;
              const oldest = oldestHistory(policy);
              return (
                <div key={scope} className="py-2 sm:grid sm:grid-cols-[12rem_1fr] sm:gap-4">
                  <dt className="text-sm font-medium text-gray-800 dark:text-gray-200">{SCOPE_LABELS[scope]}</dt>
                  <dd className="mt-1 text-sm text-gray-600 dark:text-gray-400 sm:mt-0">
                    <span>{duration(policy)}</span>
                    {oldest && <span className="mt-0.5 block text-xs">{oldest}</span>}
                  </dd>
                </div>
              );
            })}
          </dl>
          <p className="mt-3 text-xs text-gray-600 dark:text-gray-400">
            Age is measured from server receipt in UTC. Current node state is retained separately.
            {hasFinitePolicy && data.granularity === "calendar_month"
              ? " Expiration uses whole calendar months, which may keep up to one additional month of history."
              : " Keeping history forever allows disk use to grow as data arrives."}
          </p>
        </>
      ) : null}
    </section>
  );
}
