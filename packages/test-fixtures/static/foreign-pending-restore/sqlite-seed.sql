CREATE TABLE threads (
  id TEXT PRIMARY KEY,
  model_provider TEXT,
  cwd TEXT NOT NULL DEFAULT '',
  archived INTEGER NOT NULL DEFAULT 0,
  first_user_message TEXT NOT NULL DEFAULT '',
  model TEXT,
  has_user_event INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL DEFAULT 0,
  protected_marker TEXT NOT NULL DEFAULT ''
);
INSERT INTO threads (
  id, model_provider, cwd, archived, first_user_message, model,
  has_user_event, updated_at, updated_at_ms, protected_marker
)
VALUES (
  'fixture-thread', 'relay', 'C:\synthetic\fixture', 0, '', NULL,
  0, 1700000000, 1700000000123, 'fixture-db-sentinel'
);
