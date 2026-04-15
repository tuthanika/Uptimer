import { describe, expect, it } from 'vitest';

import { computePublicHomepagePayload } from '../src/public/homepage';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

describe('computePublicHomepagePayload', () => {
  it('builds compact homepage monitor cards with the expected strips and uptime summary', async () => {
    const now = 1_728_000_000;

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'from monitors m',
        all: () => [
          {
            id: 1,
            name: 'API',
            type: 'http',
            group_name: 'Core',
            group_sort_order: 0,
            sort_order: 0,
            interval_sec: 60,
            created_at: now - 40 * 86_400,
            state_status: 'up',
            last_checked_at: now - 30,
          },
        ],
      },
      {
        match: 'select distinct mwm.monitor_id',
        all: () => [],
      },
      {
        match: 'select checked_at, latency_ms, status from check_results',
        all: () => [
          { checked_at: now - 60, latency_ms: 42, status: 'up' },
          { checked_at: now - 120, latency_ms: null, status: 'down' },
        ],
      },
      {
        match: 'json_group_array(day_start_at)',
        raw: () => [
          [
            1,
            JSON.stringify([now - 2 * 86_400, now - 86_400]),
            JSON.stringify([0, 60]),
            JSON.stringify([0, 0]),
            JSON.stringify([100_000, 99_931]),
            172_800,
            172_740,
          ],
        ],
      },
      {
        match: (sql) => sql.startsWith('select key, value from settings'),
        all: () => [
          { key: 'site_title', value: 'Status Hub' },
          { key: 'site_description', value: 'Production services' },
          { key: 'site_locale', value: 'en' },
          { key: 'site_timezone', value: 'UTC' },
          { key: 'uptime_rating_level', value: '4' },
        ],
      },
      {
        match: 'from incidents',
        all: () => [],
      },
      {
        match: 'from maintenance_windows',
        all: () => [],
      },
    ];

    const payload = await computePublicHomepagePayload(createFakeD1Database(handlers), now);

    expect(payload.generated_at).toBe(now);
    expect(payload.bootstrap_mode).toBe('full');
    expect(payload.monitor_count_total).toBe(1);
    expect(payload.uptime_rating_level).toBe(4);
    expect(payload.summary).toEqual({
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    });
    expect(payload.banner).toEqual({
      source: 'monitors',
      status: 'operational',
      title: 'All Systems Operational',
    });

    expect(payload.monitors).toHaveLength(1);
    expect(payload.monitors[0]).toMatchObject({
      id: 1,
      name: 'API',
      type: 'http',
      group_name: 'Core',
      status: 'up',
      is_stale: false,
      last_checked_at: now - 30,
      heartbeat_strip: {
        checked_at: [now - 60, now - 120],
        status_codes: 'ud',
        latency_ms: [42, null],
      },
      uptime_day_strip: {
        day_start_at: [now - 2 * 86_400, now - 86_400],
        downtime_sec: [0, 60],
        unknown_sec: [0, 0],
        uptime_pct_milli: [100_000, 99_931],
      },
    });
    expect(payload.monitors[0]?.uptime_30d?.uptime_pct).toBeCloseTo(99.965, 3);
  });

  it('includes today uptime when all monitors are created after UTC day start', async () => {
    const dayStart = 1_728_000_000;
    const now = dayStart + 36_600; // 10h 10m into current UTC day
    const createdAt = now - 600; // created 10m ago

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'from monitors m',
        all: () => [
          {
            id: 1,
            name: 'API',
            type: 'http',
            group_name: null,
            interval_sec: 60,
            created_at: createdAt,
            state_status: 'up',
            last_checked_at: now - 30,
          },
        ],
      },
      {
        match: 'select distinct mwm.monitor_id',
        all: () => [],
      },
      {
        match: (sql) => sql.startsWith('select key, value from settings'),
        all: () => [
          { key: 'site_title', value: 'Status Hub' },
          { key: 'site_description', value: 'Production services' },
          { key: 'site_locale', value: 'en' },
          { key: 'site_timezone', value: 'UTC' },
          { key: 'uptime_rating_level', value: '4' },
        ],
      },
      {
        match: 'from incidents',
        all: () => [],
      },
      {
        match: 'from maintenance_windows',
        all: () => [],
      },
      {
        match: 'select checked_at, latency_ms, status from check_results',
        all: () => [{ checked_at: now - 120, latency_ms: 42, status: 'up' }],
      },
      {
        match: 'json_group_array(day_start_at)',
        raw: () => [],
      },
      {
        match: 'with input(monitor_id, interval_sec, created_at, last_checked_at) as (',
        all: () => [
          {
            monitor_id: 1,
            start_at: now - 120,
            total_sec: 120,
            downtime_sec: 0,
            unknown_sec: 0,
          },
        ],
      },
      {
        match: 'select monitor_id, started_at, ended_at',
        all: () => [],
      },
      {
        match: 'select monitor_id, checked_at, status from check_results',
        all: () => [{ monitor_id: 1, checked_at: now - 120, status: 'up' }],
      },
    ];

    const payload = await computePublicHomepagePayload(createFakeD1Database(handlers), now);

    expect(payload.monitors).toHaveLength(1);
    expect(payload.monitors[0]?.uptime_30d?.uptime_pct).toBeCloseTo(100, 6);
    expect(payload.monitors[0]?.uptime_day_strip).toMatchObject({
      day_start_at: [dayStart],
      downtime_sec: [0],
      unknown_sec: [0],
      uptime_pct_milli: [100_000],
    });
  });

  it('reuses base snapshot monitor metadata and historical uptime strips without querying monitor rows', async () => {
    const now = 1_728_000_000;
    const previousDay = now - 86_400;

    const baseSnapshot = {
      generated_at: now - 60,
      bootstrap_mode: 'full' as const,
      monitor_count_total: 1,
      site_title: 'Status Hub',
      site_description: 'Production services',
      site_locale: 'en' as const,
      site_timezone: 'UTC',
      uptime_rating_level: 4 as const,
      overall_status: 'up' as const,
      banner: {
        source: 'monitors' as const,
        status: 'operational' as const,
        title: 'All Systems Operational',
      },
      summary: {
        up: 1,
        down: 0,
        maintenance: 0,
        paused: 0,
        unknown: 0,
      },
      monitors: [
        {
          id: 1,
          name: 'API',
          type: 'http' as const,
          group_name: 'Core',
          status: 'up' as const,
          is_stale: false,
          last_checked_at: previousDay + 60,
          heartbeat_strip: {
            checked_at: [previousDay + 60],
            status_codes: 'u',
            latency_ms: [42],
          },
          uptime_30d: { uptime_pct: 100 },
          uptime_day_strip: {
            day_start_at: [previousDay],
            downtime_sec: [0],
            unknown_sec: [0],
            uptime_pct_milli: [100_000],
          },
        },
      ],
      active_incidents: [],
      maintenance_windows: {
        active: [],
        upcoming: [],
      },
      resolved_incident_preview: null,
      maintenance_history_preview: null,
    };

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'count(*) as monitor_count_total',
        first: () => ({
          monitor_count_total: 1,
          max_updated_at: now - 60,
        }),
      },
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: now - 30,
          body_json: JSON.stringify({
            version: 1,
            generated_at: now - 30,
            day_start_at: now,
            monitors: [
              {
                monitor_id: 1,
                created_at: now - 40 * 86_400,
                interval_sec: 60,
                range_start_at: now,
                materialized_at: now - 30,
                last_checked_at: now - 30,
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
          }),
        }),
      },
    ];

    const payload = await computePublicHomepagePayload(createFakeD1Database(handlers), now, {
      baseSnapshotBodyJson: JSON.stringify(baseSnapshot),
    });

    expect(payload.summary).toEqual({
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    });
    expect(payload.monitors[0]?.uptime_day_strip).toEqual(baseSnapshot.monitors[0]?.uptime_day_strip);
    expect(payload.monitors[0]?.uptime_30d).toEqual({ uptime_pct: 100 });
  });
});
