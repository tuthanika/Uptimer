import { describe, expect, it } from 'vitest';

import {
  applyMonitorRuntimeUpdates,
  materializeMonitorRuntimeTotals,
  readPublicMonitorRuntimeSnapshot,
  runtimeEntryToHeartbeats,
  type PublicMonitorRuntimeSnapshot,
} from '../src/public/monitor-runtime';
import { createFakeD1Database } from './helpers/fake-d1';

describe('public/monitor-runtime', () => {
  it('advances totals and heartbeat strips incrementally for healthy checks', () => {
    const snapshot: PublicMonitorRuntimeSnapshot = {
      version: 1,
      generated_at: 60,
      day_start_at: 0,
      monitors: [
        {
          monitor_id: 1,
          created_at: 0,
          interval_sec: 60,
          range_start_at: 0,
          materialized_at: 60,
          last_checked_at: 60,
          last_status_code: 'u',
          last_outage_open: false,
          total_sec: 0,
          downtime_sec: 0,
          unknown_sec: 0,
          uptime_sec: 0,
          heartbeat_gap_sec: '',
          heartbeat_latency_ms: [42],
          heartbeat_status_codes: 'u',
        },
      ],
    };

    const next = applyMonitorRuntimeUpdates(snapshot, 120, [
      {
        monitor_id: 1,
        interval_sec: 60,
        created_at: 0,
        checked_at: 120,
        check_status: 'up',
        next_status: 'up',
        latency_ms: 40,
      },
    ]);

    expect(next.generated_at).toBe(120);
    expect(next.monitors[0]).toMatchObject({
      total_sec: 60,
      downtime_sec: 0,
      unknown_sec: 0,
      uptime_sec: 60,
      materialized_at: 120,
      last_checked_at: 120,
      last_status_code: 'u',
      last_outage_open: false,
      heartbeat_gap_sec: '1o',
      heartbeat_latency_ms: [40, 42],
      heartbeat_status_codes: 'uu',
    });
  });

  it('preserves downtime precedence over unknown tail when an outage is open', () => {
    const totals = materializeMonitorRuntimeTotals(
      {
        monitor_id: 1,
        created_at: 0,
        interval_sec: 60,
        range_start_at: 0,
        materialized_at: 120,
        last_checked_at: 120,
        last_status_code: 'x',
        last_outage_open: true,
        total_sec: 120,
        downtime_sec: 60,
        unknown_sec: 0,
        uptime_sec: 60,
        heartbeat_gap_sec: '1o',
        heartbeat_latency_ms: [null, 42],
        heartbeat_status_codes: 'xu',
      },
      180,
    );

    expect(totals.total_sec).toBe(180);
    expect(totals.downtime_sec).toBe(120);
    expect(totals.unknown_sec).toBe(0);
    expect(totals.uptime_sec).toBe(60);
    expect(totals.uptime_pct).toBeCloseTo(100 / 3, 12);
  });

  it('decodes runtime heartbeat strips back into public heartbeat rows', () => {
    const heartbeats = runtimeEntryToHeartbeats({
      monitor_id: 1,
      created_at: 0,
      interval_sec: 60,
      range_start_at: 0,
      materialized_at: 120,
      last_checked_at: 120,
      last_status_code: 'u',
      last_outage_open: false,
      total_sec: 60,
      downtime_sec: 0,
      unknown_sec: 0,
      uptime_sec: 60,
      heartbeat_gap_sec: '1o,1o',
      heartbeat_latency_ms: [40, null, 22],
      heartbeat_status_codes: 'udm',
    });

    expect(heartbeats).toEqual([
      { checked_at: 120, latency_ms: 40, status: 'up' },
      { checked_at: 60, latency_ms: null, status: 'down' },
      { checked_at: 0, latency_ms: 22, status: 'maintenance' },
    ]);
  });

  it('accepts legacy runtime snapshots and normalizes them on read', async () => {
    const db = createFakeD1Database([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: 120,
          body_json: JSON.stringify({
            version: 1,
            generated_at: 120,
            day_start_at: 0,
            monitors: [
              {
                monitor_id: 1,
                created_at: null,
                interval_sec: 60,
                range_start_at: 0,
                materialized_at: 120,
                last_checked_at: 120,
                last_status_code: 'u',
                last_outage_open: false,
                total_sec: 60,
                downtime_sec: 0,
                unknown_sec: 0,
                uptime_sec: 60,
                heartbeat_checked_at: [120, 60, 0],
                heartbeat_latency_ms: [40, null, 22],
                heartbeat_status_codes: 'udm',
              },
            ],
          }),
        }),
      },
    ]);

    const snapshot = await readPublicMonitorRuntimeSnapshot(db, 120);
    expect(snapshot?.monitors[0]?.created_at).toBeNull();
    expect(snapshot?.monitors[0]?.heartbeat_gap_sec).toBe('1o,1o');
    expect(snapshot?.monitors[0] && runtimeEntryToHeartbeats(snapshot.monitors[0])).toEqual([
      { checked_at: 120, latency_ms: 40, status: 'up' },
      { checked_at: 60, latency_ms: null, status: 'down' },
      { checked_at: 0, latency_ms: 22, status: 'maintenance' },
    ]);
  });
});
