import type { Env } from './env';

async function handleInternalHomepageRefresh(
  request: Request,
  env: Env,
): Promise<Response> {
  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  const token = (await request.text()).trim();
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Forbidden', { status: 403 });
  }

  const now = Math.floor(Date.now() / 1000);
  try {
    const [{ computePublicHomepagePayload }, { refreshPublicHomepageSnapshotIfNeeded }] =
      await Promise.all([import('./public/homepage'), import('./snapshots')]);
    const refreshed = await refreshPublicHomepageSnapshotIfNeeded({
      db: env.DB,
      now,
      compute: () => computePublicHomepagePayload(env.DB, now),
    });

    const res = new Response(JSON.stringify({ ok: true, refreshed }), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
    return res;
  } catch (err) {
    console.warn('internal refresh: homepage failed', err);
    return new Response(JSON.stringify({ ok: false }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  }
}

export default {
  fetch: async (request: Request, env: Env, ctx: ExecutionContext): Promise<Response> => {
    const url = new URL(request.url);
    if (url.pathname === '/api/v1/internal/refresh/homepage') {
      return handleInternalHomepageRefresh(request, env);
    }

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
