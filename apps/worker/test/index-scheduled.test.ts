import { afterEach, describe, expect, it, vi } from 'vitest';

const runDailyRollup = vi.fn();
const runRetention = vi.fn();
const runScheduledTick = vi.fn();

vi.mock('../src/scheduler/daily-rollup', () => ({
  runDailyRollup,
}));

vi.mock('../src/scheduler/retention', () => ({
  runRetention,
}));

vi.mock('../src/scheduler/scheduled', () => ({
  runScheduledTick,
}));

import worker from '../src/index';
import type { Env } from '../src/env';

describe('worker scheduled dispatch', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('runs daily rollup on the midnight cron only', async () => {
    const controller = {
      cron: '0 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const ctx = { waitUntil: vi.fn() } as unknown as ExecutionContext;

    await worker.scheduled(controller, env, ctx);

    expect(runDailyRollup).toHaveBeenCalledWith(env, controller, ctx);
    expect(runRetention).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });

  it('runs retention on the separate post-midnight cron only', async () => {
    const controller = {
      cron: '30 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 30, 0),
    } as ScheduledController;
    const env = {} as Env;
    const ctx = { waitUntil: vi.fn() } as unknown as ExecutionContext;

    await worker.scheduled(controller, env, ctx);

    expect(runRetention).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });

  it('runs the minute scheduler for non-daily crons', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 1, 0),
    } as ScheduledController;
    const env = {} as Env;
    const ctx = { waitUntil: vi.fn() } as unknown as ExecutionContext;

    await worker.scheduled(controller, env, ctx);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runRetention).not.toHaveBeenCalled();
  });
});
