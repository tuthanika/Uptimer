import type { Env } from '../env';
import type { Trace } from '../observability/trace';
import {
  refreshPublicMonitorRuntimeSnapshot,
  type MonitorRuntimeUpdate,
} from '../public/monitor-runtime';
import { rebuildPublicMonitorRuntimeSnapshot } from '../public/monitor-runtime-bootstrap';
import { readMonitorRuntimeUpdateFragments } from '../snapshots/public-monitor-fragments';

export const MONITOR_RUNTIME_UPDATE_FRAGMENT_MAX_AGE_SECONDS = 5 * 60;
export const MONITOR_RUNTIME_UPDATE_FRAGMENT_FUTURE_TOLERANCE_SECONDS = 60;

export type InternalRuntimeFragmentsRefreshResult = {
  ok: boolean;
  refreshed: boolean;
  updateCount: number;
  invalidCount: number;
  staleCount: number;
  skip?: 'no_updates';
  error?: boolean;
};

function readBoundedPositiveIntegerEnv(
  env: Env,
  key: keyof Env,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = env[key];
  if (typeof raw !== 'string') {
    return fallback;
  }
  const parsed = Number.parseInt(raw.trim(), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, parsed));
}

function latestUpdateTimestamp(updates: readonly MonitorRuntimeUpdate[]): number | null {
  let latest: number | null = null;
  for (const update of updates) {
    if (latest === null || update.checked_at > latest) {
      latest = update.checked_at;
    }
  }
  return latest;
}

export async function refreshMonitorRuntimeSnapshotFromUpdateFragments(opts: {
  env: Env;
  now: number;
  trace?: Trace | null;
}): Promise<InternalRuntimeFragmentsRefreshResult> {
  const maxAgeSeconds = readBoundedPositiveIntegerEnv(
    opts.env,
    'UPTIMER_MONITOR_RUNTIME_UPDATE_FRAGMENT_MAX_AGE_SECONDS',
    MONITOR_RUNTIME_UPDATE_FRAGMENT_MAX_AGE_SECONDS,
    30,
    15 * 60,
  );
  const minGeneratedAt = Math.max(0, opts.now - maxAgeSeconds);
  const maxGeneratedAt = opts.now + MONITOR_RUNTIME_UPDATE_FRAGMENT_FUTURE_TOLERANCE_SECONDS;

  try {
    opts.trace?.setLabel('route', 'internal/runtime-fragments-refresh');
    opts.trace?.setLabel('now', opts.now);
    opts.trace?.setLabel('fragment_max_age_s', maxAgeSeconds);

    const fragmentRead = opts.trace
      ? await opts.trace.timeAsync(
          'runtime_fragments_read',
          async () =>
            await readMonitorRuntimeUpdateFragments(opts.env.DB, {
              minGeneratedAt,
              maxGeneratedAt,
            }),
        )
      : await readMonitorRuntimeUpdateFragments(opts.env.DB, {
          minGeneratedAt,
          maxGeneratedAt,
        });

    opts.trace?.setLabel('runtime_update_fragment_count', fragmentRead.updates.length);
    opts.trace?.setLabel('runtime_update_fragment_invalid_count', fragmentRead.invalidCount);
    opts.trace?.setLabel('runtime_update_fragment_stale_count', fragmentRead.staleCount);

    if (fragmentRead.updates.length === 0) {
      opts.trace?.setLabel('skip', 'no_updates');
      return {
        ok: true,
        refreshed: false,
        updateCount: 0,
        invalidCount: fragmentRead.invalidCount,
        staleCount: fragmentRead.staleCount,
        skip: 'no_updates',
      };
    }

    const refreshNow = Math.max(opts.now, latestUpdateTimestamp(fragmentRead.updates) ?? opts.now);
    await (opts.trace
      ? opts.trace.timeAsync(
          'runtime_fragments_refresh_snapshot',
          async () =>
            await refreshPublicMonitorRuntimeSnapshot({
              db: opts.env.DB,
              now: refreshNow,
              updates: fragmentRead.updates,
              rebuild: async () => await rebuildPublicMonitorRuntimeSnapshot(opts.env.DB, refreshNow),
            }),
        )
      : refreshPublicMonitorRuntimeSnapshot({
          db: opts.env.DB,
          now: refreshNow,
          updates: fragmentRead.updates,
          rebuild: async () => await rebuildPublicMonitorRuntimeSnapshot(opts.env.DB, refreshNow),
        }));

    return {
      ok: true,
      refreshed: true,
      updateCount: fragmentRead.updates.length,
      invalidCount: fragmentRead.invalidCount,
      staleCount: fragmentRead.staleCount,
    };
  } catch (err) {
    console.warn('internal runtime fragments refresh failed', err);
    opts.trace?.setLabel('error', '1');
    return {
      ok: false,
      refreshed: false,
      updateCount: 0,
      invalidCount: 0,
      staleCount: 0,
      error: true,
    };
  }
}
