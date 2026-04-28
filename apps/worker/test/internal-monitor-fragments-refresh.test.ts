import { describe, expect, it } from 'vitest';

import { refreshPublicMonitorFragmentsFromPayloads } from '../src/internal/monitor-fragments-refresh-core';
import type { MonitorRuntimeUpdate } from '../src/public/monitor-runtime';
import { createFakeD1Database } from './helpers/fake-d1';

function statusMonitor(id: number) {
  return {
    id,
    name: `Monitor ${id}`,
    type: 'http' as const,
    group_name: 'Core',
    group_sort_order: 0,
    sort_order: id,
    uptime_rating_level: 4 as const,
    status: 'up' as const,
    is_stale: false,
    last_checked_at: 1_700_000_000,
    last_latency_ms: 42,
    heartbeats: [
      {
        checked_at: 1_700_000_000,
        status: 'up' as const,
        latency_ms: 42,
      },
    ],
    uptime_30d: {
      range_start_at: 1_697_408_000,
      range_end_at: 1_700_000_000,
      total_sec: 2_592_000,
      downtime_sec: 0,
      unknown_sec: 0,
      uptime_sec: 2_592_000,
      uptime_pct: 100,
    },
    uptime_days: [
      {
        day_start_at: 1_699_920_000,
        total_sec: 86_400,
        downtime_sec: 0,
        unknown_sec: 0,
        uptime_sec: 86_400,
        uptime_pct: 100,
      },
    ],
  };
}

function homepageMonitor(id: number) {
  return {
    id,
    name: `Monitor ${id}`,
    type: 'http' as const,
    group_name: 'Core',
    status: 'up' as const,
    is_stale: false,
    last_checked_at: 1_700_000_000,
    heartbeat_strip: {
      checked_at: [1_700_000_000],
      status_codes: 'u',
      latency_ms: [42],
    },
    uptime_30d: { uptime_pct: 100 },
    uptime_day_strip: {
      day_start_at: [1_699_920_000],
      downtime_sec: [0],
      unknown_sec: [0],
      uptime_pct_milli: [100_000],
    },
  };
}

function statusPayload() {
  return {
    generated_at: 1_700_000_000,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 4 as const,
    overall_status: 'up' as const,
    banner: {
      source: 'monitors' as const,
      status: 'operational' as const,
      title: 'All Systems Operational',
      down_ratio: null,
    },
    summary: { up: 2, down: 0, maintenance: 0, paused: 0, unknown: 0 },
    monitors: [statusMonitor(1), statusMonitor(2)],
    active_incidents: [],
    maintenance_windows: { active: [], upcoming: [] },
  };
}

function homepagePayload() {
  return {
    generated_at: 1_700_000_000,
    bootstrap_mode: 'full' as const,
    monitor_count_total: 2,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 4 as const,
    overall_status: 'up' as const,
    banner: {
      source: 'monitors' as const,
      status: 'operational' as const,
      title: 'All Systems Operational',
      down_ratio: null,
    },
    summary: { up: 2, down: 0, maintenance: 0, paused: 0, unknown: 0 },
    monitors: [homepageMonitor(1), homepageMonitor(2)],
    active_incidents: [],
    maintenance_windows: { active: [], upcoming: [] },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

function update(monitorId: number): MonitorRuntimeUpdate {
  return {
    monitor_id: monitorId,
    interval_sec: 60,
    created_at: 1_699_999_000,
    checked_at: 1_700_000_000,
    check_status: 'up',
    next_status: 'up',
    latency_ms: 42,
  };
}

describe('internal/monitor-fragments-refresh-core', () => {
  it('writes only changed monitor fragments from homepage and status payloads', async () => {
    const writes: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshot_fragments',
        run: (args) => {
          writes.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const result = await refreshPublicMonitorFragmentsFromPayloads({
      db,
      now: 1_700_000_005,
      homepagePayload: homepagePayload(),
      statusPayload: statusPayload(),
      runtimeUpdates: [update(2)],
    });

    expect(result).toEqual({
      ok: true,
      writeCount: 2,
      statusWriteCount: 1,
      homepageWriteCount: 1,
      monitorCount: 1,
    });
    expect(writes.map((args) => [args[0], args[1]])).toEqual([
      ['status:monitors', 'monitor:2'],
      ['homepage:monitors', 'monitor:2'],
    ]);
    expect(JSON.parse(writes[0]![3] as string).id).toBe(2);
    expect(JSON.parse(writes[1]![3] as string).id).toBe(2);
  });

  it('writes all monitor fragments when no runtime updates bound the refresh', async () => {
    const writes: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshot_fragments',
        run: (args) => {
          writes.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const result = await refreshPublicMonitorFragmentsFromPayloads({
      db,
      now: 1_700_000_005,
      homepagePayload: homepagePayload(),
      statusPayload: null,
    });

    expect(result).toMatchObject({
      ok: true,
      writeCount: 2,
      statusWriteCount: 0,
      homepageWriteCount: 2,
      monitorCount: 0,
    });
    expect(writes.map((args) => args[1])).toEqual(['monitor:1', 'monitor:2']);
  });
});
