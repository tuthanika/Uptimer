const SNAPSHOT_MAX_AGE_SECONDS = 60;
const PREFERRED_MAX_AGE_SECONDS = 30;
const FALLBACK_HTML_MAX_AGE_SECONDS = 600;
const HOMEPAGE_CACHE_GENERATED_AT_HEADER = 'X-Uptimer-Generated-At';

function acceptsHtml(request) {
  const accept = request.headers.get('Accept') || '';
  return accept.includes('text/html');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function safeJsonForInlineScript(value) {
  return JSON.stringify(value).replace(/</g, '\\u003c');
}

function readGeneratedAtHeader(res) {
  const raw = res.headers.get(HOMEPAGE_CACHE_GENERATED_AT_HEADER);
  if (!raw) return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeSnapshotText(value, fallback) {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

const HOMEPAGE_PRELOAD_STYLE_TAG = `<style id="uptimer-preload-style">
#uptimer-preload{min-height:100vh;background:#f8fafc;color:#0f172a;font:400 14px/1.45 ui-sans-serif,system-ui,sans-serif}
#uptimer-preload *{box-sizing:border-box}
#uptimer-preload .uw{max-width:80rem;margin:0 auto;padding:0 16px}
#uptimer-preload .uh{position:sticky;top:0;z-index:20;background:rgba(255,255,255,.95);backdrop-filter:blur(12px);border-bottom:1px solid rgba(226,232,240,.8)}
#uptimer-preload .uhw{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 0}
#uptimer-preload .ut{min-width:0}
#uptimer-preload .un{font-size:20px;font-weight:700;line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .ud{margin-top:4px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .sb{display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;border:1px solid transparent}
#uptimer-preload .sb-up{background:#ecfdf5;color:#047857;border-color:#a7f3d0}
#uptimer-preload .sb-down{background:#fef2f2;color:#b91c1c;border-color:#fecaca}
#uptimer-preload .sb-maintenance{background:#eff6ff;color:#1d4ed8;border-color:#bfdbfe}
#uptimer-preload .sb-paused{background:#fffbeb;color:#b45309;border-color:#fde68a}
#uptimer-preload .sb-unknown{background:#f8fafc;color:#475569;border-color:#cbd5e1}
#uptimer-preload .um{padding:24px 0 40px}
#uptimer-preload .bn{margin:0 0 24px;border:1px solid #e2e8f0;border-radius:18px;padding:20px;background:#fff;box-shadow:0 10px 30px rgba(15,23,42,.04)}
#uptimer-preload .bt{color:#475569}
#uptimer-preload .bu{margin-top:4px;font-size:12px;color:#94a3b8}
#uptimer-preload .sec{margin-top:24px}
#uptimer-preload .sh{margin:0 0 12px;font-size:16px;font-weight:700}
#uptimer-preload .st{display:grid;gap:12px}
#uptimer-preload .sg{margin-top:20px}
#uptimer-preload .sgh{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
#uptimer-preload .sgt{font-size:13px;font-weight:700;color:#475569}
#uptimer-preload .sgc{font-size:12px;color:#94a3b8}
#uptimer-preload .grid{display:grid;gap:12px}
#uptimer-preload .card{border:1px solid rgba(226,232,240,.9);border-radius:16px;padding:14px;background:#fff}
#uptimer-preload .row{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
#uptimer-preload .lhs{min-width:0;display:flex;align-items:flex-start;gap:10px}
#uptimer-preload .dot{display:block;width:10px;height:10px;border-radius:999px;margin-top:5px}
#uptimer-preload .dot-up{background:#10b981}
#uptimer-preload .dot-down{background:#ef4444}
#uptimer-preload .dot-maintenance{background:#3b82f6}
#uptimer-preload .dot-paused{background:#f59e0b}
#uptimer-preload .dot-unknown{background:#94a3b8}
#uptimer-preload .mn{font-size:15px;font-weight:700;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#uptimer-preload .mt{margin-top:3px;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.08em}
#uptimer-preload .rhs{display:flex;align-items:center;gap:8px;white-space:nowrap}
#uptimer-preload .up{font-size:12px;color:#94a3b8}
#uptimer-preload .lbl{margin:12px 0 6px;font-size:11px;color:#94a3b8}
#uptimer-preload .strip{height:20px;border-radius:8px;background:#e2e8f0;overflow:hidden}
#uptimer-preload .usv{display:block;width:100%;height:100%}
#uptimer-preload .ft{margin-top:12px;font-size:11px;color:#94a3b8}
#uptimer-preload .ih{padding-top:24px;border-top:1px solid #e2e8f0}
@media (min-width:640px){#uptimer-preload .grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
html.dark #uptimer-preload{background:#0f172a;color:#f8fafc}
html.dark #uptimer-preload .uh{background:rgba(15,23,42,.95);border-bottom-color:rgba(51,65,85,.9)}
html.dark #uptimer-preload .ud,#uptimer-preload .sgt{color:#cbd5e1}
html.dark #uptimer-preload .bn,html.dark #uptimer-preload .card{background:#1e293b;border-color:rgba(51,65,85,.95);box-shadow:none}
html.dark #uptimer-preload .bt{color:#cbd5e1}
html.dark #uptimer-preload .bu,#uptimer-preload .sgc,#uptimer-preload .up,#uptimer-preload .lbl,#uptimer-preload .ft{color:#94a3b8}
html.dark #uptimer-preload .mt{color:#94a3b8}
html.dark #uptimer-preload .strip{background:#334155}
html.dark #uptimer-preload .ih{border-top-color:#334155}
</style>`;

function computeCacheControl(ageSeconds) {
  const remaining = Math.max(0, SNAPSHOT_MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(PREFERRED_MAX_AGE_SECONDS, remaining);
  const stale = Math.max(0, remaining - maxAge);
  return `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`;
}

function buildHomepageCacheHit(cached, ageSeconds) {
  // In the Cloudflare runtime, cached/fetched Responses can have immutable headers.
  // Always rebuild before mutating.
  const hit = new Response(cached.body, cached);
  hit.headers.set('Cache-Control', computeCacheControl(ageSeconds));
  hit.headers.delete(HOMEPAGE_CACHE_GENERATED_AT_HEADER);
  return hit;
}

function upsertHeadTag(html, pattern, tag) {
  if (pattern.test(html)) {
    return html.replace(pattern, tag);
  }
  return html.replace('</head>', `  ${tag}\n</head>`);
}

function injectStatusMetaTags(html, artifact, url) {
  const siteTitle = normalizeSnapshotText(artifact?.meta_title, 'Uptimer');
  const siteDescription = normalizeSnapshotText(
    artifact?.meta_description,
    'Real-time status and incident updates.',
  )
    .replace(/\s+/g, ' ')
    .trim();
  const pageUrl = new URL('/', url).toString();

  const escapedTitle = escapeHtml(siteTitle);
  const escapedDescription = escapeHtml(siteDescription);
  const escapedUrl = escapeHtml(pageUrl);

  let injected = html;
  injected = upsertHeadTag(injected, /<title>[^<]*<\/title>/i, `<title>${escapedTitle}</title>`);
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']description["'][^>]*>/i,
    `<meta name="description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:type["'][^>]*>/i,
    '<meta property="og:type" content="website" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:title["'][^>]*>/i,
    `<meta property="og:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:description["'][^>]*>/i,
    `<meta property="og:description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:site_name["'][^>]*>/i,
    `<meta property="og:site_name" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:url["'][^>]*>/i,
    `<meta property="og:url" content="${escapedUrl}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:card["'][^>]*>/i,
    '<meta name="twitter:card" content="summary" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:title["'][^>]*>/i,
    `<meta name="twitter:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:description["'][^>]*>/i,
    `<meta name="twitter:description" content="${escapedDescription}" />`,
  );

  return injected;
}

function buildMinimalIndexHtml(title = 'Uptimer') {
  const escapedTitle = escapeHtml(title);
  return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>${escapedTitle}</title></head><body><div id="root"></div></body></html>`;
}

async function fetchIndexHtml(env, url) {
  const indexUrl = new URL('/index.html', url);

  // Do not pass the original navigation request as init. In Pages runtime the
  // navigation request can carry redirect mode = manual; if we forward that
  // into `env.ASSETS.fetch`, we might accidentally return a redirect response
  // (and cache it), causing ERR_TOO_MANY_REDIRECTS.
  const req = new Request(indexUrl.toString(), {
    method: 'GET',
    headers: { Accept: 'text/html' },
    redirect: 'follow',
  });

  return env.ASSETS.fetch(req);
}

async function fetchPublicHomepageArtifact(env) {
  const apiOrigin = env.UPTIMER_API_ORIGIN;
  if (typeof apiOrigin !== 'string' || apiOrigin.length === 0) return null;

  const statusUrl = new URL('/api/v1/public/homepage-artifact', apiOrigin);

  // Keep HTML fast: if the API is slow, fall back to a static HTML shell.
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 800);

  try {
    const resp = await fetch(statusUrl.toString(), {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });

    if (!resp.ok) return null;

    const data = await resp.json();
    if (!data || typeof data !== 'object') return null;

    if (typeof data.preload_html !== 'string') return null;
    if (!data.snapshot || typeof data.snapshot !== 'object') return null;

    return data;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

export default {
  async fetch(request, env, ctx) {
    try {
      try {
        ctx.passThroughOnException?.();
      } catch {
        // ignore
      }

      const url = new URL(request.url);

      // HTML requests: serve SPA entry for client-side routes.
      const wantsHtml = request.method === 'GET' && acceptsHtml(request);

      // Special-case the status page for HTML injection.
      const isStatusPage = url.pathname === '/' || url.pathname === '/index.html';
      if (wantsHtml && isStatusPage) {
        const cacheKey = new Request(url.origin + '/', { method: 'GET' });
        let cached = null;
        try {
          cached = await caches.default.match(cacheKey);
        } catch {
          cached = null;
        }

        const now = Math.floor(Date.now() / 1000);
        if (cached) {
          const cachedGeneratedAt = readGeneratedAtHeader(cached);
          if (cachedGeneratedAt === null) {
            return cached;
          }

        const cachedAge = Math.max(0, now - cachedGeneratedAt);
        if (cachedAge <= SNAPSHOT_MAX_AGE_SECONDS) {
          return buildHomepageCacheHit(cached, cachedAge);
        }
      }

      const base = await fetchIndexHtml(env, url);
      const html = await base.text();

      const artifact = await fetchPublicHomepageArtifact(env);
      if (!artifact) {
        if (cached) {
          const cachedGeneratedAt = readGeneratedAtHeader(cached);
          if (cachedGeneratedAt === null) return cached;
          return buildHomepageCacheHit(cached, Math.max(0, now - cachedGeneratedAt));
        }

        const headers = new Headers(base.headers);
        headers.set('Content-Type', 'text/html; charset=utf-8');
        headers.append('Vary', 'Accept');
        headers.delete('Location');

          return new Response(html, { status: 200, headers });
        }

        const generatedAt =
          typeof artifact.generated_at === 'number' ? artifact.generated_at : now;
        const age = Math.max(0, now - generatedAt);

        let injected = html.replace(
          '<div id="root"></div>',
          `${artifact.preload_html}<div id="root"></div>`,
        );

        injected = injectStatusMetaTags(injected, artifact, url);

        injected = injected.replace(
          '</head>',
          `  ${HOMEPAGE_PRELOAD_STYLE_TAG}\n  <script>globalThis.__UPTIMER_INITIAL_HOMEPAGE__=${safeJsonForInlineScript(artifact.snapshot)};</script>\n</head>`,
        );

        const headers = new Headers(base.headers);
        headers.set('Content-Type', 'text/html; charset=utf-8');
        headers.set('Cache-Control', computeCacheControl(age));
        headers.append('Vary', 'Accept');
        headers.delete('Location');

        const resp = new Response(injected, { status: 200, headers });

        const cacheHeaders = new Headers(headers);
        cacheHeaders.set('Cache-Control', `public, max-age=${FALLBACK_HTML_MAX_AGE_SECONDS}`);
        cacheHeaders.set(HOMEPAGE_CACHE_GENERATED_AT_HEADER, `${generatedAt}`);
        cacheHeaders.delete('Set-Cookie');
        const cacheResp = new Response(injected, { status: 200, headers: cacheHeaders });

        try {
          ctx.waitUntil(caches.default.put(cacheKey, cacheResp).catch(() => undefined));
        } catch {
          // Ignore cache write failures. The injected HTML response is still usable and
          // the worker should never throw a 1101 just because the cache rejected a put.
        }
        return resp;
      }

      // Default: serve static assets.
      const assetResp = await env.ASSETS.fetch(request);

      // SPA fallback for client-side routes.
      if (wantsHtml && assetResp.status === 404) {
        const indexResp = await fetchIndexHtml(env, url);
        const html = await indexResp.text();

        const headers = new Headers(indexResp.headers);
        headers.set('Content-Type', 'text/html; charset=utf-8');
        headers.append('Vary', 'Accept');
        headers.delete('Location');

        return new Response(html, { status: 200, headers });
      }

      return assetResp;
    } catch (err) {
      try {
        console.error('pages worker: unhandled error', err);
      } catch {
        // ignore
      }

      // Best-effort fallback for HTML navigation so we never surface a 1101.
      try {
        const wantsHtml = request.method === 'GET' && acceptsHtml(request);
        const url = new URL(request.url);
        const isStatusPage = url.pathname === '/' || url.pathname === '/index.html';

        if (wantsHtml) {
          if (isStatusPage) {
            try {
              const cacheKey = new Request(url.origin + '/', { method: 'GET' });
              const cached = await caches.default.match(cacheKey);
              if (cached) {
                const now = Math.floor(Date.now() / 1000);
                const cachedGeneratedAt = readGeneratedAtHeader(cached);
                if (cachedGeneratedAt === null) return cached;
                return buildHomepageCacheHit(cached, Math.max(0, now - cachedGeneratedAt));
              }
            } catch {
              // ignore
            }
          }

          try {
            const indexResp = await fetchIndexHtml(env, url);
            const html = await indexResp.text();
            const headers = new Headers(indexResp.headers);
            headers.set('Content-Type', 'text/html; charset=utf-8');
            headers.append('Vary', 'Accept');
            headers.delete('Location');
            headers.set('Cache-Control', 'no-store');
            return new Response(html, { status: 200, headers });
          } catch {
            // ignore
          }

          return new Response(buildMinimalIndexHtml('Uptimer'), {
            status: 200,
            headers: {
              'Content-Type': 'text/html; charset=utf-8',
              'Cache-Control': 'no-store',
              Vary: 'Accept',
            },
          });
        }
      } catch {
        // ignore
      }

      return new Response('Internal Server Error', { status: 500 });
    }
  },
};
