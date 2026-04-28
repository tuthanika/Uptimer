import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/public/monitor-runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/public/monitor-runtime')>();
  return {
    ...actual,
    refreshPublicMonitorRuntimeSnapshot: vi.fn(async () => ({
      version: 1,
      generated_at: 1_700_000_060,
      day_start_at: 1_699_920_000,
      monitors: [],
    })),
  };
});

vi.mock('../src/public/monitor-runtime-bootstrap', () => ({
  rebuildPublicMonitorRuntimeSnapshot: vi.fn(async () => ({
    version: 1,
    generated_at: 1_700_000_060,
    day_start_at: 1_699_920_000,
    monitors: [],
  })),
}));

import type { Env } from '../src/env';
import worker from '../src/index';
import { refreshPublicMonitorRuntimeSnapshot } from '../src/public/monitor-runtime';
import { createFakeD1Database } from './helpers/fake-d1';

describe('internal runtime fragment refresh route', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('requires internal bearer auth', async () => {
    const env = {
      DB: createFakeD1Database([]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/refresh/runtime-fragments', {
        method: 'POST',
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(403);
  });

  it('refreshes the monitor runtime snapshot from fresh compact update fragments', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(new Date('2026-04-15T05:18:20.000Z').valueOf());
    const env = {
      DB: createFakeD1Database([
        {
          match: 'from public_snapshot_fragments',
          all: (args) => {
            expect(args).toEqual(['monitor-runtime:updates']);
            return [
              {
                fragment_key: 'monitor:1',
                generated_at: 1_776_230_280,
                body_json: '[1,60,1776230000,1776230280,"up","up",21]',
                updated_at: 1_776_230_300,
              },
            ];
          },
        },
      ]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/refresh/runtime-fragments', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
        },
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      refreshed: true,
      update_count: 1,
      invalid_count: 0,
      stale_count: 0,
    });
    expect(refreshPublicMonitorRuntimeSnapshot).toHaveBeenCalledWith(
      expect.objectContaining({
        db: env.DB,
        now: 1_776_230_300,
        updates: [
          {
            monitor_id: 1,
            interval_sec: 60,
            created_at: 1_776_230_000,
            checked_at: 1_776_230_280,
            check_status: 'up',
            next_status: 'up',
            latency_ms: 21,
          },
        ],
      }),
    );
  });

  it('skips refresh when no fresh update fragments are available', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(new Date('2026-04-15T05:18:20.000Z').valueOf());
    const env = {
      DB: createFakeD1Database([
        {
          match: 'from public_snapshot_fragments',
          all: () => [
            {
              fragment_key: 'monitor:1',
              generated_at: 1_776_229_000,
              body_json: '[1,60,1776229000,1776229000,"up","up",21]',
              updated_at: 1_776_229_000,
            },
          ],
        },
      ]),
      ADMIN_TOKEN: 'test-admin-token',
    } as unknown as Env;

    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/refresh/runtime-fragments', {
        method: 'POST',
        headers: {
          Authorization: 'Bearer test-admin-token',
        },
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      refreshed: false,
      update_count: 0,
      invalid_count: 0,
      stale_count: 1,
      skip: 'no_updates',
    });
    expect(refreshPublicMonitorRuntimeSnapshot).not.toHaveBeenCalled();
  });
});
