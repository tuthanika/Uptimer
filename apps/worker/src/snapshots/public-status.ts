import { AppError } from '../middleware/errors';
import { publicStatusResponseSchema, type PublicStatusResponse } from '../schemas/public-status';

const SNAPSHOT_KEY = 'status';
const MAX_AGE_SECONDS = 60;

export function getSnapshotKey() {
  return SNAPSHOT_KEY;
}

export function getSnapshotMaxAgeSeconds() {
  return MAX_AGE_SECONDS;
}

function safeJsonParse(text: string): unknown | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function looksLikeStatusPayload(value: unknown): value is PublicStatusResponse {
  if (!isRecord(value)) return false;

  return (
    typeof value.generated_at === 'number' &&
    typeof value.site_title === 'string' &&
    typeof value.overall_status === 'string' &&
    Array.isArray(value.monitors) &&
    Array.isArray(value.active_incidents) &&
    isRecord(value.banner) &&
    isRecord(value.summary) &&
    isRecord(value.maintenance_windows)
  );
}

function looksLikeSerializedStatusPayload(text: string): boolean {
  const trimmed = text.trim();
  return (
    trimmed.startsWith('{"generated_at":') &&
    trimmed.includes('"site_title"') &&
    trimmed.includes('"overall_status"') &&
    trimmed.includes('"monitors"') &&
    trimmed.includes('"summary"') &&
    trimmed.includes('"maintenance_windows"')
  );
}

function looksLikeCompleteJsonObject(text: string): boolean {
  const trimmed = text.trim();
  if (trimmed.length < 2) return false;
  if (trimmed[0] !== '{' || trimmed[trimmed.length - 1] !== '}') return false;

  const stack: ('{' | '[')[] = [];
  let inString = false;

  for (let index = 0; index < trimmed.length; index += 1) {
    const ch = trimmed[index] as string;

    if (inString) {
      if (ch === '\\') {
        index += 1;
        continue;
      }
      if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === '{' || ch === '[') {
      stack.push(ch as '{' | '[');
      continue;
    }

    if (ch === '}' || ch === ']') {
      const open = stack.pop();
      if (!open) return false;
      if (ch === '}' && open !== '{') return false;
      if (ch === ']' && open !== '[') return false;

      // The root object must end at the end of the string.
      if (stack.length === 0 && index !== trimmed.length - 1) {
        return false;
      }
    }
  }

  return !inString && stack.length === 0;
}

export async function readStatusSnapshot(
  db: D1Database,
  now: number,
): Promise<{ data: PublicStatusResponse; age: number } | null> {
  try {
    const row = await db
      .prepare(
        `
        SELECT generated_at, body_json
        FROM public_snapshots
        WHERE key = ?1
      `,
      )
      .bind(SNAPSHOT_KEY)
      .first<{ generated_at: number; body_json: string }>();

    if (!row) return null;

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    const parsed = safeJsonParse(row.body_json);
    if (looksLikeStatusPayload(parsed)) {
      return { data: parsed, age };
    }

    const data = publicStatusResponseSchema.parse(parsed);
    return { data, age };
  } catch (err) {
    // Backward compatible: if the table doesn't exist yet or snapshot is invalid,
    // callers should fall back to live computation.
    console.warn('public snapshot: read failed, falling back to live', err);
    return null;
  }
}

export async function readStatusSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  try {
    const row = await db
      .prepare(
        `
        SELECT generated_at, body_json
        FROM public_snapshots
        WHERE key = ?1
      `,
      )
      .bind(SNAPSHOT_KEY)
      .first<{ generated_at: number; body_json: string }>();

    if (!row) return null;

    const age = Math.max(0, now - row.generated_at);
    if (age > MAX_AGE_SECONDS) return null;

    if (
      looksLikeSerializedStatusPayload(row.body_json) &&
      looksLikeCompleteJsonObject(row.body_json)
    ) {
      return { bodyJson: row.body_json, age };
    }

    const parsed = safeJsonParse(row.body_json);
    if (looksLikeStatusPayload(parsed)) {
      return { bodyJson: row.body_json, age };
    }

    const data = publicStatusResponseSchema.parse(parsed);
    return { bodyJson: JSON.stringify(data), age };
  } catch (err) {
    console.warn('public snapshot: read failed, falling back to live', err);
    return null;
  }
}

export async function writeStatusSnapshot(
  db: D1Database,
  now: number,
  payload: PublicStatusResponse,
): Promise<void> {
  const bodyJson = JSON.stringify(payload);
  await db
    .prepare(
      `
      INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT(key) DO UPDATE SET
        generated_at = excluded.generated_at,
        body_json = excluded.body_json,
        updated_at = excluded.updated_at
    `,
    )
    .bind(SNAPSHOT_KEY, payload.generated_at, bodyJson, now)
    .run();
}

export function applyStatusCacheHeaders(res: Response, ageSeconds: number): void {
  // Guarantee freshness bound <= 60s. Prefer <= 30s in normal cases.
  //
  // We ensure (max-age + stale-*) never exceeds MAX_AGE_SECONDS.
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export function toSnapshotPayload(value: unknown): PublicStatusResponse {
  const parsed = publicStatusResponseSchema.safeParse(value);
  if (!parsed.success) {
    throw new AppError(500, 'INTERNAL', 'Failed to generate status snapshot');
  }
  return parsed.data;
}
