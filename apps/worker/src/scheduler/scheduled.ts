import pLimit from 'p-limit';

import {
  expectedStatusJsonSchema,
  httpHeadersJsonSchema,
  parseDbJsonNullable,
} from '@uptimer/db/json';
import type { HttpResponseMatchMode, MonitorStatus } from '@uptimer/db/schema';

import type { Env } from '../env';
import {
  computeNextState,
  type MonitorStateSnapshot,
  type NextState,
  type OutageAction,
} from '../monitor/state-machine';
import type { CheckOutcome } from '../monitor/types';
import { readSettings } from '../settings';
import { acquireLease } from './lock';
import type { NotifyContext } from './notifications';

const LOCK_NAME = 'scheduler:tick';
const LOCK_LEASE_SECONDS = 55;

const CHECK_CONCURRENCY = 5;
const PERSIST_BATCH_SIZE = 25;

async function refreshHomepageSnapshotInline(env: Env, now: number): Promise<void> {
  const [{ computePublicHomepagePayload }, { refreshPublicHomepageSnapshot }] = await Promise.all([
    import('../public/homepage'),
    import('../snapshots'),
  ]);

  await refreshPublicHomepageSnapshot({
    db: env.DB,
    now,
    compute: () => computePublicHomepagePayload(env.DB, now),
  });
}

type HomepageRefreshServiceResult = {
  refreshed: boolean | null;
};

async function refreshHomepageSnapshotViaService(env: Env): Promise<HomepageRefreshServiceResult> {
  if (!env.SELF) {
    throw new Error('SELF service binding missing');
  }
  if (!env.ADMIN_TOKEN) {
    throw new Error('ADMIN_TOKEN missing');
  }

  const res = await env.SELF.fetch(
    new Request('http://internal/api/v1/internal/refresh/homepage', {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'X-Uptimer-Refresh-Source': 'scheduled',
      },
      body: env.ADMIN_TOKEN,
    }),
  );

  const bodyText = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`service refresh failed: HTTP ${res.status} ${bodyText}`.trim());
  }
  let refreshed: boolean | null = null;
  if (bodyText) {
    try {
      const parsed = JSON.parse(bodyText) as { refreshed?: unknown };
      refreshed = typeof parsed.refreshed === 'boolean' ? parsed.refreshed : null;
    } catch {
      refreshed = null;
    }
  }

  return {
    refreshed,
  };
}

type CachedMonitorHttpJson = {
  http_headers_json: string | null;
  expected_status_json: string | null;
  httpHeaders: Record<string, string> | null;
  expectedStatus: number[] | null;
};

const cachedMonitorHttpJsonById = new Map<number, CachedMonitorHttpJson>();
let httpCheckModulePromise: Promise<typeof import('../monitor/http')> | null = null;
let tcpCheckModulePromise: Promise<typeof import('../monitor/tcp')> | null = null;

type DueMonitorRow = {
  id: number;
  name: string;
  type: string;
  target: string;
  interval_sec: number;
  timeout_ms: number;
  http_method: string | null;
  http_headers_json: string | null;
  http_body: string | null;
  expected_status_json: string | null;
  response_keyword: string | null;
  response_keyword_mode: HttpResponseMatchMode | null;
  response_forbidden_keyword: string | null;
  response_forbidden_keyword_mode: HttpResponseMatchMode | null;
  state_status: string | null;
  state_last_error: string | null;
  last_changed_at: number | null;
  consecutive_failures: number | null;
  consecutive_successes: number | null;
};

async function getHttpCheckModule() {
  httpCheckModulePromise ??= import('../monitor/http');
  return await httpCheckModulePromise;
}

async function getTcpCheckModule() {
  tcpCheckModulePromise ??= import('../monitor/tcp');
  return await tcpCheckModulePromise;
}

async function hasActiveWebhookChannels(db: D1Database): Promise<boolean> {
  const cachedResult = activeWebhookPresenceCacheByDb.get(db);
  if (
    cachedResult &&
    Date.now() - cachedResult.fetchedAtMs < ACTIVE_WEBHOOK_PRESENCE_CACHE_TTL_MS
  ) {
    return cachedResult.hasActive;
  }

  const cached = hasActiveWebhookChannelsStatementByDb.get(db);
  const statement = cached ?? db.prepare(HAS_ACTIVE_WEBHOOK_CHANNELS_SQL);
  if (!cached) {
    hasActiveWebhookChannelsStatementByDb.set(db, statement);
  }

  const { results } = await statement.all<unknown>();
  const hasActive = (results?.length ?? 0) > 0;
  activeWebhookPresenceCacheByDb.set(db, { fetchedAtMs: Date.now(), hasActive });
  return hasActive;
}

const listDueMonitorsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const persistStatementTemplatesByDb = new WeakMap<D1Database, PersistStatementTemplates>();
const hasActiveWebhookChannelsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const activeWebhookPresenceCacheByDb = new WeakMap<
  D1Database,
  { fetchedAtMs: number; hasActive: boolean }
>();

const HAS_ACTIVE_WEBHOOK_CHANNELS_SQL = `
  SELECT 1 AS present
  FROM notification_channels
  WHERE is_active = 1 AND type = 'webhook'
  LIMIT 1
`;
const ACTIVE_WEBHOOK_PRESENCE_CACHE_TTL_MS = 60_000;

const LIST_DUE_MONITORS_SQL = `
  SELECT
    m.id,
    m.name,
    m.type,
    m.target,
    m.interval_sec,
    m.timeout_ms,
    m.http_method,
    m.http_headers_json,
    m.http_body,
    m.expected_status_json,
    m.response_keyword,
    m.response_keyword_mode,
    m.response_forbidden_keyword,
    m.response_forbidden_keyword_mode,
    s.status AS state_status,
    s.last_error AS state_last_error,
    s.last_changed_at,
    s.consecutive_failures,
    s.consecutive_successes
  FROM monitors m
  LEFT JOIN monitor_state s ON s.monitor_id = m.id
  WHERE m.is_active = 1
    AND (s.status IS NULL OR s.status != 'paused')
    AND (s.last_checked_at IS NULL OR s.last_checked_at <= ?1 - m.interval_sec)
  ORDER BY m.id
`;

const PERSIST_STATEMENTS_SQL = {
  insertCheckResult: `
    INSERT INTO check_results (
      monitor_id,
      checked_at,
      status,
      latency_ms,
      http_status,
      error,
      location,
      attempt
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
  `,
  upsertMonitorState: `
    INSERT INTO monitor_state (
      monitor_id,
      status,
      last_checked_at,
      last_changed_at,
      last_latency_ms,
      last_error,
      consecutive_failures,
      consecutive_successes
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    ON CONFLICT(monitor_id) DO UPDATE SET
      status = excluded.status,
      last_checked_at = excluded.last_checked_at,
      last_changed_at = excluded.last_changed_at,
      last_latency_ms = excluded.last_latency_ms,
      last_error = excluded.last_error,
      consecutive_failures = excluded.consecutive_failures,
      consecutive_successes = excluded.consecutive_successes
  `,
  openOutageIfMissing: `
    INSERT INTO outages (monitor_id, started_at, ended_at, initial_error, last_error)
    SELECT ?1, ?2, NULL, ?3, ?4
    WHERE NOT EXISTS (
      SELECT 1 FROM outages WHERE monitor_id = ?5 AND ended_at IS NULL
    )
  `,
  closeOutage: `
    UPDATE outages
    SET ended_at = ?1
    WHERE monitor_id = ?2 AND ended_at IS NULL
  `,
  updateOutageLastError: `
    UPDATE outages
    SET last_error = ?1
    WHERE monitor_id = ?2 AND ended_at IS NULL
  `,
} as const;

type CompletedDueMonitor = {
  row: DueMonitorRow;
  checkedAt: number;
  prevStatus: MonitorStatus | null;
  outcome: CheckOutcome;
  next: NextState;
  outageAction: OutageAction;
  stateLastError: string | null;
  maintenanceSuppressed: boolean;
};

function toHttpMethod(
  value: string | null,
): 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD' | null {
  const normalized = (value ?? 'GET').toUpperCase();
  switch (normalized) {
    case 'GET':
    case 'POST':
    case 'PUT':
    case 'PATCH':
    case 'DELETE':
    case 'HEAD':
      return normalized;
    default:
      return null;
  }
}

function toMonitorStatus(value: string | null): MonitorStatus | null {
  switch (value) {
    case 'up':
    case 'down':
    case 'maintenance':
    case 'paused':
    case 'unknown':
      return value;
    default:
      return null;
  }
}

async function listDueMonitors(db: D1Database, checkedAt: number): Promise<DueMonitorRow[]> {
  const cached = listDueMonitorsStatementByDb.get(db);
  const statement = cached ?? db.prepare(LIST_DUE_MONITORS_SQL);
  if (!cached) {
    listDueMonitorsStatementByDb.set(db, statement);
  }

  const { results } = await statement.bind(checkedAt).all<DueMonitorRow>();

  return results ?? [];
}

function computeStateLastError(
  nextStatus: MonitorStatus,
  outcome: CheckOutcome,
  prevLastError: string | null,
): string | null {
  if (nextStatus === 'down') {
    return outcome.status === 'up' ? prevLastError : outcome.error;
  }
  if (nextStatus === 'up') {
    return outcome.status === 'up' ? null : outcome.error;
  }
  return outcome.status === 'up' ? null : outcome.error;
}

type PersistStatementTemplates = {
  insertCheckResult: D1PreparedStatement;
  upsertMonitorState: D1PreparedStatement;
  openOutageIfMissing: D1PreparedStatement;
  closeOutage: D1PreparedStatement;
  updateOutageLastError: D1PreparedStatement;
};

function buildPersistStatements(
  completed: CompletedDueMonitor,
  templates: PersistStatementTemplates,
): D1PreparedStatement[] {
  const { row, checkedAt, outcome, next, outageAction, stateLastError } = completed;
  const checkError = outcome.status === 'up' ? null : outcome.error;

  const statements: D1PreparedStatement[] = [];

  statements.push(
    templates.insertCheckResult.bind(
      row.id,
      checkedAt,
      outcome.status,
      outcome.latencyMs,
      outcome.httpStatus,
      checkError,
      null,
      outcome.attempts,
    ),
  );

  statements.push(
    templates.upsertMonitorState.bind(
      row.id,
      next.status,
      checkedAt,
      next.lastChangedAt,
      outcome.latencyMs,
      stateLastError,
      next.consecutiveFailures,
      next.consecutiveSuccesses,
    ),
  );

  if (outageAction === 'open') {
    statements.push(
      templates.openOutageIfMissing.bind(
        row.id,
        checkedAt,
        checkError ?? 'down',
        checkError ?? 'down',
        row.id,
      ),
    );
  } else if (outageAction === 'close') {
    statements.push(templates.closeOutage.bind(checkedAt, row.id));
  } else if (outageAction === 'update' && checkError) {
    statements.push(templates.updateOutageLastError.bind(checkError, row.id));
  }

  return statements;
}

async function runDueMonitor(
  row: DueMonitorRow,
  checkedAt: number,
  maintenanceSuppressed: boolean,
  stateMachineConfig: { failuresToDownFromUp: number; successesToUpFromDown: number },
): Promise<CompletedDueMonitor> {
  const prevStatus = toMonitorStatus(row.state_status);
  const prev: MonitorStateSnapshot | null =
    prevStatus === null
      ? null
      : {
          status: prevStatus,
          lastChangedAt: row.last_changed_at,
          consecutiveFailures: row.consecutive_failures ?? 0,
          consecutiveSuccesses: row.consecutive_successes ?? 0,
        };

  let outcome: CheckOutcome;

  try {
    if (row.type === 'http') {
      const httpMethod = toHttpMethod(row.http_method);
      if (!httpMethod) {
        outcome = {
          status: 'unknown',
          latencyMs: null,
          httpStatus: null,
          error: 'Invalid http_method',
          attempts: 1,
        };
      } else {
        const cached = cachedMonitorHttpJsonById.get(row.id);
        const cachedMatches =
          cached &&
          cached.http_headers_json === row.http_headers_json &&
          cached.expected_status_json === row.expected_status_json;

        const httpHeaders = cachedMatches
          ? cached.httpHeaders
          : parseDbJsonNullable(httpHeadersJsonSchema, row.http_headers_json, {
              field: 'http_headers_json',
            });
        const expectedStatus = cachedMatches
          ? cached.expectedStatus
          : parseDbJsonNullable(expectedStatusJsonSchema, row.expected_status_json, {
              field: 'expected_status_json',
            });

        if (!cachedMatches) {
          cachedMonitorHttpJsonById.set(row.id, {
            http_headers_json: row.http_headers_json,
            expected_status_json: row.expected_status_json,
            httpHeaders,
            expectedStatus,
          });
        }

        const { runHttpCheck } = await getHttpCheckModule();
        outcome = await runHttpCheck({
          url: row.target,
          timeoutMs: row.timeout_ms,
          method: httpMethod,
          headers: httpHeaders,
          body: row.http_body,
          expectedStatus,
          responseKeyword: row.response_keyword,
          responseKeywordMode: row.response_keyword_mode,
          responseForbiddenKeyword: row.response_forbidden_keyword,
          responseForbiddenKeywordMode: row.response_forbidden_keyword_mode,
        });
      }
    } else if (row.type === 'tcp') {
      const { runTcpCheck } = await getTcpCheckModule();
      outcome = await runTcpCheck({ target: row.target, timeoutMs: row.timeout_ms });
    } else {
      outcome = {
        status: 'unknown',
        latencyMs: null,
        httpStatus: null,
        error: `Unsupported monitor type: ${String(row.type)}`,
        attempts: 1,
      };
    }
  } catch (err) {
    outcome = {
      status: 'unknown',
      latencyMs: null,
      httpStatus: null,
      error: err instanceof Error ? err.message : String(err),
      attempts: 1,
    };
  }

  const { next, outageAction } = computeNextState(prev, outcome, checkedAt, stateMachineConfig);
  const stateLastError = computeStateLastError(next.status, outcome, row.state_last_error);

  return {
    row,
    checkedAt,
    prevStatus,
    outcome,
    next,
    outageAction,
    stateLastError,
    maintenanceSuppressed,
  };
}

async function persistCompletedMonitors(
  db: D1Database,
  completed: CompletedDueMonitor[],
): Promise<void> {
  const cached = persistStatementTemplatesByDb.get(db);
  const templates = cached ?? {
    insertCheckResult: db.prepare(PERSIST_STATEMENTS_SQL.insertCheckResult),
    upsertMonitorState: db.prepare(PERSIST_STATEMENTS_SQL.upsertMonitorState),
    openOutageIfMissing: db.prepare(PERSIST_STATEMENTS_SQL.openOutageIfMissing),
    closeOutage: db.prepare(PERSIST_STATEMENTS_SQL.closeOutage),
    updateOutageLastError: db.prepare(PERSIST_STATEMENTS_SQL.updateOutageLastError),
  };
  if (!cached) {
    persistStatementTemplatesByDb.set(db, templates);
  }

  for (let i = 0; i < completed.length; i += PERSIST_BATCH_SIZE) {
    const chunk = completed.slice(i, i + PERSIST_BATCH_SIZE);
    const statements: D1PreparedStatement[] = [];

    for (const monitor of chunk) {
      statements.push(...buildPersistStatements(monitor, templates));
    }

    if (statements.length > 0) {
      await db.batch(statements);
    }
  }
}

export async function runScheduledTick(env: Env, ctx: ExecutionContext): Promise<void> {
  const now = Math.floor(Date.now() / 1000);
  const checkedAt = Math.floor(now / 60) * 60;
  const queueHomepageRefresh = () =>
    env.SELF
      ? refreshHomepageSnapshotViaService(env).catch(async (err) => {
          console.warn('homepage snapshot: service refresh failed', err);
          await refreshHomepageSnapshotInline(env, now).catch((fallbackErr) => {
            console.warn('homepage snapshot: refresh failed', fallbackErr);
          });
        })
      : refreshHomepageSnapshotInline(env, now).catch((err) => {
          console.warn('homepage snapshot: refresh failed', err);
        });

  const acquired = await acquireLease(env.DB, LOCK_NAME, now, LOCK_LEASE_SECONDS);
  if (!acquired) {
    return;
  }

  const [settings, due, hasWebhookNotifications] = await Promise.all([
    readSettings(env.DB),
    listDueMonitors(env.DB, checkedAt),
    hasActiveWebhookChannels(env.DB),
  ]);

  let notificationsModule: typeof import('./notifications') | null = null;
  let notify: NotifyContext | null = null;
  if (hasWebhookNotifications) {
    notificationsModule = await import('./notifications');
    notify = await notificationsModule.createNotifyContext(env, ctx);
    if (notify) {
      await notificationsModule.emitMaintenanceWindowNotifications(env, notify, now);
    }
  }

  const stateMachineConfig = {
    failuresToDownFromUp: settings.state_failures_to_down_from_up,
    successesToUpFromDown: settings.state_successes_to_up_from_down,
  };

  if (due.length === 0) {
    ctx.waitUntil(queueHomepageRefresh());
    return;
  }

  // Maintenance suppression is monitor-scoped.
  const dueMonitorIds = due.map((m) => m.id);
  const suppressedMonitorIds =
    notify === null || notificationsModule === null
      ? new Set<number>()
      : await notificationsModule.listMaintenanceSuppressedMonitorIds(env.DB, now, dueMonitorIds);

  const limit = pLimit(CHECK_CONCURRENCY);
  const settled = await Promise.allSettled(
    due.map((m) =>
      limit(() => runDueMonitor(m, checkedAt, suppressedMonitorIds.has(m.id), stateMachineConfig)),
    ),
  );

  const rejected = settled.filter((r) => r.status === 'rejected');
  const completed = settled
    .filter((r): r is PromiseFulfilledResult<CompletedDueMonitor> => r.status === 'fulfilled')
    .map((r) => r.value);

  if (completed.length > 0) {
    await persistCompletedMonitors(env.DB, completed);

    if (notificationsModule) {
      for (const monitor of completed) {
        notificationsModule.queueMonitorNotification(env, notify, monitor);
      }
    }
  }

  if (rejected.length === 0 && completed.every((monitor) => monitor.outcome.status === 'up')) {
    ctx.waitUntil(queueHomepageRefresh());
    return;
  }

  let httpCount = 0;
  let tcpCount = 0;
  let assertionCount = 0;
  let attemptTotal = 0;
  let downCount = 0;
  let unknownCount = 0;
  for (const monitor of completed) {
    attemptTotal += monitor.outcome.attempts;
    if (monitor.outcome.status === 'down') downCount += 1;
    else if (monitor.outcome.status === 'unknown') unknownCount += 1;

    if (monitor.row.type === 'http') {
      httpCount += 1;
      if (monitor.row.response_keyword || monitor.row.response_forbidden_keyword) {
        assertionCount += 1;
      }
    } else if (monitor.row.type === 'tcp') {
      tcpCount += 1;
    }
  }

  if (rejected.length > 0) {
    console.error(
      `scheduled: ${rejected.length}/${settled.length} monitors failed at ${checkedAt} attempts=${attemptTotal} http=${httpCount} tcp=${tcpCount} assertions=${assertionCount} down=${downCount} unknown=${unknownCount}`,
      rejected[0],
    );
  } else if (downCount > 0 || unknownCount > 0) {
    console.warn(
      `scheduled: processed ${settled.length} monitors at ${checkedAt} attempts=${attemptTotal} http=${httpCount} tcp=${tcpCount} assertions=${assertionCount} down=${downCount} unknown=${unknownCount}`,
    );
  }

  ctx.waitUntil(queueHomepageRefresh());
}
