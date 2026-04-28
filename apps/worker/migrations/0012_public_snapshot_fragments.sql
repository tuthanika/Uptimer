-- D1-backed fragment storage for public snapshots.
--
-- This table lets scheduled/internal paths update small precomputed pieces
-- (for example one monitor's status/homepage runtime fragment) without
-- rewriting the monolithic status/homepage JSON snapshot rows every minute.
CREATE TABLE IF NOT EXISTS public_snapshot_fragments (
  snapshot_key TEXT NOT NULL,
  fragment_key TEXT NOT NULL,
  generated_at INTEGER NOT NULL,
  body_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  PRIMARY KEY (snapshot_key, fragment_key)
);

CREATE INDEX IF NOT EXISTS idx_public_snapshot_fragments_snapshot_generated
  ON public_snapshot_fragments (snapshot_key, generated_at);
