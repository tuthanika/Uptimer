import type { Trace } from '../observability/trace';
import type { PublicHomepageResponse } from '../schemas/public-homepage';
import type { PublicStatusResponse } from '../schemas/public-status';
import type { MonitorRuntimeUpdate } from '../public/monitor-runtime';
import {
  buildHomepageMonitorFragmentWrites,
  buildStatusMonitorFragmentWrites,
} from '../snapshots/public-monitor-fragments';
import { writePublicSnapshotFragments } from '../snapshots/public-fragments';

export type PublicMonitorFragmentRefreshResult = {
  ok: boolean;
  writeCount: number;
  statusWriteCount: number;
  homepageWriteCount: number;
  monitorCount: number;
};

function uniqueRuntimeUpdateMonitorIds(
  runtimeUpdates: readonly MonitorRuntimeUpdate[] | undefined,
): number[] | undefined {
  if (!runtimeUpdates || runtimeUpdates.length === 0) {
    return undefined;
  }

  const ids = new Set<number>();
  for (const update of runtimeUpdates) {
    if (Number.isInteger(update.monitor_id) && update.monitor_id > 0) {
      ids.add(update.monitor_id);
    }
  }

  return [...ids];
}

export async function refreshPublicMonitorFragmentsFromPayloads(opts: {
  db: D1Database;
  now: number;
  homepagePayload?: PublicHomepageResponse | null;
  statusPayload?: PublicStatusResponse | null;
  runtimeUpdates?: readonly MonitorRuntimeUpdate[];
  trace?: Trace | null;
}): Promise<PublicMonitorFragmentRefreshResult> {
  const monitorIds = uniqueRuntimeUpdateMonitorIds(opts.runtimeUpdates);
  const statusWrites = opts.statusPayload
    ? buildStatusMonitorFragmentWrites(opts.statusPayload, opts.now, monitorIds)
    : [];
  const homepageWrites = opts.homepagePayload
    ? buildHomepageMonitorFragmentWrites(opts.homepagePayload, opts.now, monitorIds)
    : [];
  const writes = [...statusWrites, ...homepageWrites];

  opts.trace?.setLabel('monitor_fragment_write_count', writes.length);
  opts.trace?.setLabel('monitor_fragment_status_write_count', statusWrites.length);
  opts.trace?.setLabel('monitor_fragment_homepage_write_count', homepageWrites.length);
  opts.trace?.setLabel('monitor_fragment_monitor_count', monitorIds?.length ?? 0);

  if (writes.length === 0) {
    return {
      ok: true,
      writeCount: 0,
      statusWriteCount: 0,
      homepageWriteCount: 0,
      monitorCount: monitorIds?.length ?? 0,
    };
  }

  if (opts.trace) {
    await opts.trace.timeAsync(
      'monitor_fragment_writes_batch',
      async () => await writePublicSnapshotFragments(opts.db, writes),
    );
  } else {
    await writePublicSnapshotFragments(opts.db, writes);
  }

  return {
    ok: true,
    writeCount: writes.length,
    statusWriteCount: statusWrites.length,
    homepageWriteCount: homepageWrites.length,
    monitorCount: monitorIds?.length ?? 0,
  };
}
