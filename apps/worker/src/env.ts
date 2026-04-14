export interface Env {
  DB: D1Database;
  ADMIN_TOKEN: string;

  // Dev/prod deploy helper: Workers.dev origin for this Worker (e.g. https://<name>.<subdomain>.workers.dev).
  // Used by the scheduler to self-invoke internal refresh endpoints to split CPU budget across invocations.
  UPTIMER_SELF_ORIGIN?: string;

  // Optional dev-only trace secret. If set, trace headers are honored only when
  // callers present `X-Uptimer-Trace-Token`.
  UPTIMER_TRACE_TOKEN?: string;
  TRACE_TOKEN?: string;

  // In-memory, per-instance rate limit for admin endpoints.
  // Keep optional so older deployments don't break.
  ADMIN_RATE_LIMIT_MAX?: string;
  ADMIN_RATE_LIMIT_WINDOW_SEC?: string;
}
