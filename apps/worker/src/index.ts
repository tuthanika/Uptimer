import type { Env } from './env';

export default {
  fetch: async (request: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
    const mod = await import('./fetch-handler');
    return mod.handleFetch(request, env, ctx);
  },
  scheduled: async (controller: ScheduledController, env: Env, ctx: ExecutionContext) => {
    if (controller.cron === '0 0 * * *') {
      const [{ runRetention }, { runDailyRollup }] = await Promise.all([
        import('./scheduler/retention'),
        import('./scheduler/daily-rollup'),
      ]);
      await runRetention(env, controller);
      await runDailyRollup(env, controller, ctx);
      return;
    }

    const { runScheduledTick } = await import('./scheduler/scheduled');
    await runScheduledTick(env, ctx);
  },
} satisfies ExportedHandler<Env>;
