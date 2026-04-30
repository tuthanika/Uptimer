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

function createExecutionContext(): {
  ctx: ExecutionContext;
  waitUntil: ReturnType<typeof vi.fn>;
  waitUntilPromises: Promise<unknown>[];
} {
  const waitUntilPromises: Promise<unknown>[] = [];
  const waitUntil = vi.fn((promise: Promise<unknown>) => {
    waitUntilPromises.push(promise);
  });
  return {
    ctx: { waitUntil } as unknown as ExecutionContext,
    waitUntil,
    waitUntilPromises,
  };
}

describe('worker scheduled dispatch', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('runs only the minute scheduler for ordinary consolidated cron ticks', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 1, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runRetention).not.toHaveBeenCalled();
    expect(waitUntil).not.toHaveBeenCalled();
  });

  it('queues daily rollup at UTC midnight on the consolidated minute cron', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runDailyRollup).toHaveBeenCalledWith(env, controller, ctx);
    expect(runRetention).not.toHaveBeenCalled();
  });

  it('queues retention at UTC 00:30 on the consolidated minute cron', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 30, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runRetention).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).not.toHaveBeenCalled();
  });

  it('keeps the legacy daily rollup cron compatible during trigger propagation', async () => {
    const controller = {
      cron: '0 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runDailyRollup).toHaveBeenCalledWith(env, controller, ctx);
    expect(runRetention).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });

  it('keeps the legacy retention cron compatible during trigger propagation', async () => {
    const controller = {
      cron: '30 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 30, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runRetention).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });
});
