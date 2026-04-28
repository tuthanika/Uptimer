import {
  parseMonitorRuntimeUpdate,
  type MonitorRuntimeUpdate,
} from '../public/monitor-runtime';
import type { PublicHomepageResponse } from '../schemas/public-homepage';
import type { PublicStatusResponse } from '../schemas/public-status';
import {
  readPublicSnapshotFragments,
  type PublicSnapshotFragmentRow,
  type PublicSnapshotFragmentWrite,
} from './public-fragments';

export const STATUS_MONITOR_FRAGMENTS_KEY = 'status:monitors';
export const HOMEPAGE_MONITOR_FRAGMENTS_KEY = 'homepage:monitors';
export const STATUS_ENVELOPE_FRAGMENT_KEY = 'status:envelope';
export const HOMEPAGE_ENVELOPE_FRAGMENT_KEY = 'homepage:envelope';
export const MONITOR_RUNTIME_UPDATE_FRAGMENTS_KEY = 'monitor-runtime:updates';
export const PUBLIC_SNAPSHOT_ENVELOPE_FRAGMENT_KEY = 'envelope';

const MONITOR_FRAGMENT_PREFIX = 'monitor:';

function assertMonitorId(monitorId: number): void {
  if (!Number.isInteger(monitorId) || monitorId <= 0) {
    throw new Error('public monitor fragment id must be a positive integer');
  }
}

function toSelectedMonitorIdSet(monitorIds?: Iterable<number>): Set<number> | null {
  if (!monitorIds) {
    return null;
  }

  const selected = new Set<number>();
  for (const monitorId of monitorIds) {
    assertMonitorId(monitorId);
    selected.add(monitorId);
  }
  return selected;
}

export function toPublicMonitorFragmentKey(monitorId: number): string {
  assertMonitorId(monitorId);
  return `${MONITOR_FRAGMENT_PREFIX}${monitorId}`;
}

export function parsePublicMonitorFragmentKey(fragmentKey: string): number | null {
  if (!fragmentKey.startsWith(MONITOR_FRAGMENT_PREFIX)) {
    return null;
  }
  const parsed = Number.parseInt(fragmentKey.slice(MONITOR_FRAGMENT_PREFIX.length), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function shouldWriteMonitorFragment(
  selectedMonitorIds: ReadonlySet<number> | null,
  monitorId: number,
): boolean {
  return selectedMonitorIds === null || selectedMonitorIds.has(monitorId);
}

function buildMonitorFragmentWrite(opts: {
  snapshotKey: string;
  fragmentKey: string;
  generatedAt: number;
  bodyJson: string;
  updatedAt: number;
}): PublicSnapshotFragmentWrite {
  return {
    snapshotKey: opts.snapshotKey,
    fragmentKey: opts.fragmentKey,
    generatedAt: opts.generatedAt,
    bodyJson: opts.bodyJson,
    updatedAt: opts.updatedAt,
  };
}

export function buildStatusMonitorFragmentWrites(
  payload: PublicStatusResponse,
  updatedAt: number,
  monitorIds?: Iterable<number>,
): PublicSnapshotFragmentWrite[] {
  const selectedMonitorIds = toSelectedMonitorIdSet(monitorIds);
  const writes: PublicSnapshotFragmentWrite[] = [];

  for (const monitor of payload.monitors) {
    if (!shouldWriteMonitorFragment(selectedMonitorIds, monitor.id)) {
      continue;
    }
    writes.push(
      buildMonitorFragmentWrite({
        snapshotKey: STATUS_MONITOR_FRAGMENTS_KEY,
        fragmentKey: toPublicMonitorFragmentKey(monitor.id),
        generatedAt: payload.generated_at,
        bodyJson: JSON.stringify(monitor),
        updatedAt,
      }),
    );
  }

  return writes;
}

export function buildHomepageMonitorFragmentWrites(
  payload: PublicHomepageResponse,
  updatedAt: number,
  monitorIds?: Iterable<number>,
): PublicSnapshotFragmentWrite[] {
  const selectedMonitorIds = toSelectedMonitorIdSet(monitorIds);
  const writes: PublicSnapshotFragmentWrite[] = [];

  for (const monitor of payload.monitors) {
    if (!shouldWriteMonitorFragment(selectedMonitorIds, monitor.id)) {
      continue;
    }
    writes.push(
      buildMonitorFragmentWrite({
        snapshotKey: HOMEPAGE_MONITOR_FRAGMENTS_KEY,
        fragmentKey: toPublicMonitorFragmentKey(monitor.id),
        generatedAt: payload.generated_at,
        bodyJson: JSON.stringify(monitor),
        updatedAt,
      }),
    );
  }

  return writes;
}

export type PublicStatusEnvelopeFragment = Omit<PublicStatusResponse, 'monitors'>;
export type PublicHomepageEnvelopeFragment = Omit<PublicHomepageResponse, 'monitors'>;

export function toStatusEnvelopeFragment(
  payload: PublicStatusResponse,
): PublicStatusEnvelopeFragment {
  const { monitors: _monitors, ...envelope } = payload;
  return envelope;
}

export function toHomepageEnvelopeFragment(
  payload: PublicHomepageResponse,
): PublicHomepageEnvelopeFragment {
  const { monitors: _monitors, ...envelope } = payload;
  return envelope;
}

export function buildStatusEnvelopeFragmentWrite(
  payload: PublicStatusResponse,
  updatedAt: number,
): PublicSnapshotFragmentWrite {
  return buildMonitorFragmentWrite({
    snapshotKey: STATUS_ENVELOPE_FRAGMENT_KEY,
    fragmentKey: PUBLIC_SNAPSHOT_ENVELOPE_FRAGMENT_KEY,
    generatedAt: payload.generated_at,
    bodyJson: JSON.stringify(toStatusEnvelopeFragment(payload)),
    updatedAt,
  });
}

export function buildHomepageEnvelopeFragmentWrite(
  payload: PublicHomepageResponse,
  updatedAt: number,
): PublicSnapshotFragmentWrite {
  return buildMonitorFragmentWrite({
    snapshotKey: HOMEPAGE_ENVELOPE_FRAGMENT_KEY,
    fragmentKey: PUBLIC_SNAPSHOT_ENVELOPE_FRAGMENT_KEY,
    generatedAt: payload.generated_at,
    bodyJson: JSON.stringify(toHomepageEnvelopeFragment(payload)),
    updatedAt,
  });
}

function toCompactRuntimeUpdate(update: MonitorRuntimeUpdate): unknown[] {
  return [
    update.monitor_id,
    update.interval_sec,
    update.created_at,
    update.checked_at,
    update.check_status,
    update.next_status,
    update.latency_ms,
  ];
}

export function buildMonitorRuntimeUpdateFragmentWrites(
  updates: readonly MonitorRuntimeUpdate[],
  updatedAt: number,
): PublicSnapshotFragmentWrite[] {
  const latestUpdateByMonitorId = new Map<number, MonitorRuntimeUpdate>();
  for (const update of updates) {
    assertMonitorId(update.monitor_id);
    const previous = latestUpdateByMonitorId.get(update.monitor_id);
    if (!previous || update.checked_at >= previous.checked_at) {
      latestUpdateByMonitorId.set(update.monitor_id, update);
    }
  }

  return [...latestUpdateByMonitorId.values()].map((update) =>
    buildMonitorFragmentWrite({
      snapshotKey: MONITOR_RUNTIME_UPDATE_FRAGMENTS_KEY,
      fragmentKey: toPublicMonitorFragmentKey(update.monitor_id),
      generatedAt: update.checked_at,
      bodyJson: JSON.stringify(toCompactRuntimeUpdate(update)),
      updatedAt,
    }),
  );
}

export type MonitorRuntimeUpdateFragmentReadOptions = {
  minGeneratedAt?: number;
  maxGeneratedAt?: number;
};

export type MonitorRuntimeUpdateFragmentReadResult = {
  updates: MonitorRuntimeUpdate[];
  invalidCount: number;
  staleCount: number;
};

function shouldSkipRuntimeUpdateFragmentByTime(
  row: PublicSnapshotFragmentRow,
  opts: MonitorRuntimeUpdateFragmentReadOptions,
): boolean {
  return (
    (opts.minGeneratedAt !== undefined && row.generated_at < opts.minGeneratedAt) ||
    (opts.maxGeneratedAt !== undefined && row.generated_at > opts.maxGeneratedAt)
  );
}

export function parseMonitorRuntimeUpdateFragmentRows(
  rows: readonly PublicSnapshotFragmentRow[],
  opts: MonitorRuntimeUpdateFragmentReadOptions = {},
): MonitorRuntimeUpdateFragmentReadResult {
  const latestUpdateByMonitorId = new Map<number, MonitorRuntimeUpdate>();
  let invalidCount = 0;
  let staleCount = 0;

  for (const row of rows) {
    if (shouldSkipRuntimeUpdateFragmentByTime(row, opts)) {
      staleCount += 1;
      continue;
    }

    const monitorId = parsePublicMonitorFragmentKey(row.fragment_key);
    if (monitorId === null) {
      invalidCount += 1;
      continue;
    }

    let raw: unknown;
    try {
      raw = JSON.parse(row.body_json) as unknown;
    } catch {
      invalidCount += 1;
      continue;
    }

    const update = parseMonitorRuntimeUpdate(raw);
    if (
      !update ||
      update.monitor_id !== monitorId ||
      update.checked_at !== row.generated_at
    ) {
      invalidCount += 1;
      continue;
    }

    const previous = latestUpdateByMonitorId.get(update.monitor_id);
    if (!previous || update.checked_at >= previous.checked_at) {
      latestUpdateByMonitorId.set(update.monitor_id, update);
    }
  }

  return {
    updates: [...latestUpdateByMonitorId.values()].sort((a, b) => a.monitor_id - b.monitor_id),
    invalidCount,
    staleCount,
  };
}

export async function readMonitorRuntimeUpdateFragments(
  db: D1Database,
  opts: MonitorRuntimeUpdateFragmentReadOptions = {},
): Promise<MonitorRuntimeUpdateFragmentReadResult> {
  const rows = await readPublicSnapshotFragments(db, MONITOR_RUNTIME_UPDATE_FRAGMENTS_KEY);
  return parseMonitorRuntimeUpdateFragmentRows(rows, opts);
}
