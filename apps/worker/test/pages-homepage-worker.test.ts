import { afterEach, describe, expect, it, vi } from 'vitest';

import pageWorker from '../../web/public/_worker.js';

type CacheMatcher = (request: Request) => Response | undefined;

function installDefaultCacheMock(match: CacheMatcher) {
  const put = vi.fn(async () => undefined);

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      default: {
        async match(request: Request) {
          return match(request)?.clone();
        },
        put,
      },
    },
  });

  return { put };
}

function makeEnv(indexHtml = '<!doctype html><html><head></head><body><div id="root"></div></body></html>') {
  return {
    ASSETS: {
      fetch: vi.fn(async () => new Response(indexHtml, { status: 200 })),
    },
    UPTIMER_API_ORIGIN: 'https://api.example.com',
  };
}

describe('pages homepage worker', () => {
  const originalCaches = (globalThis as { caches?: unknown }).caches;
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    if (originalCaches === undefined) {
      delete (globalThis as { caches?: unknown }).caches;
    } else {
      Object.defineProperty(globalThis, 'caches', {
        configurable: true,
        value: originalCaches,
      });
    }

    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('serves cached injected HTML without calling the homepage API', async () => {
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/'
        ? new Response('<html>cached homepage</html>', { status: 200 })
        : undefined,
    );
    const env = makeEnv();
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('cached homepage');
    expect(env.ASSETS.fetch).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('recomputes cache-control on cache hit with generated-at header', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-17T00:01:00.000Z'));

    try {
      const nowSec = Math.floor(Date.now() / 1000);
      const generatedAt = nowSec - 10;

      installDefaultCacheMock((request) =>
        request.url === 'https://status.example.com/'
          ? new Response('<html>cached homepage</html>', {
              status: 200,
              headers: {
                'X-Uptimer-Generated-At': String(generatedAt),
                'Cache-Control': 'public, max-age=600',
              },
            })
          : undefined,
      );
      const env = makeEnv();
      const fetchSpy = vi.fn();
      globalThis.fetch = fetchSpy as never;

      const res = await pageWorker.fetch(
        new Request('https://status.example.com/', {
          headers: { Accept: 'text/html' },
        }),
        env,
        { waitUntil: vi.fn() },
      );

      expect(await res.text()).toContain('cached homepage');
      expect(res.headers.get('X-Uptimer-Generated-At')).toBeNull();
      expect(res.headers.get('Cache-Control')).toBe(
        'public, max-age=30, stale-while-revalidate=20, stale-if-error=20',
      );
      expect(env.ASSETS.fetch).not.toHaveBeenCalled();
      expect(fetchSpy).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('falls back to the cached injected homepage when snapshot fetch fails', async () => {
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: { 'X-Uptimer-Generated-At': '0' },
          })
        : undefined,
    );
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () => {
      throw new Error('network failed');
    }) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('fallback homepage');
  });

  it('injects the precomputed homepage artifact and updates the html cache on success', async () => {
    const { put } = installDefaultCacheMock(() => undefined);
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () =>
      new Response(
        JSON.stringify({
          generated_at: 1_728_000_000,
          preload_html: '<div id="uptimer-preload"><main>artifact preload</main></div>',
          snapshot: { site_title: 'Status Hub' },
          meta_title: 'Status Hub',
          meta_description: 'Production',
        }),
        { status: 200 },
      ),
    ) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn((promise) => promise) },
    );

    const html = await res.text();
    expect(html).toContain('__UPTIMER_INITIAL_HOMEPAGE__');
    expect(html).not.toContain('__UPTIMER_INITIAL_STATUS__');
    expect(html).toContain('artifact preload');
    expect(put).toHaveBeenCalledTimes(1);
  });
});
