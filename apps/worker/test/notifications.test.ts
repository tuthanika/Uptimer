import { describe, expect, it } from 'vitest';

import { listMaintenanceSuppressedMonitorIds } from '../src/scheduler/notifications';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

describe('scheduler/notifications', () => {
  it('chunks maintenance suppression lookups under the D1 variable limit', async () => {
    const ids = Array.from({ length: 150 }, (_, index) => index + 1);
    const argLengths: number[] = [];
    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'select distinct mwm.monitor_id',
        all: (args) => {
          argLengths.push(args.length);
          return args.slice(1).map((monitorId) => ({ monitor_id: monitorId as number }));
        },
      },
    ];

    const suppressed = await listMaintenanceSuppressedMonitorIds(
      createFakeD1Database(handlers),
      1_776_230_340,
      ids,
    );

    expect(argLengths.length).toBeGreaterThan(1);
    expect(Math.max(...argLengths)).toBeLessThanOrEqual(100);
    expect([...suppressed]).toEqual(ids);
  });

  it('fails open when maintenance suppression lookups error', async () => {
    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'select distinct mwm.monitor_id',
        all: () => {
          throw new Error('boom');
        },
      },
    ];

    const suppressed = await listMaintenanceSuppressedMonitorIds(
      createFakeD1Database(handlers),
      1_776_230_340,
      [1, 2, 3],
    );

    expect([...suppressed]).toEqual([]);
  });
});
