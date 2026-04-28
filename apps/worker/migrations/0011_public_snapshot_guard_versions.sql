-- DB-backed public snapshot guard versions/state.
-- Used by scheduled public snapshot refresh fast paths to prove small cached guard
-- state is still current across Workers isolates.
CREATE TABLE IF NOT EXISTS public_snapshot_guard_versions (
  key TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  state_json TEXT
);
