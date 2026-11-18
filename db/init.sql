-- Initialization for the device fault log platform (runs once on an empty data dir).
--
-- 1) device_fault_logs: hot table that keeps receiving writes while the index
--    is being built online.
-- 2) index_build_jobs:  ledger used by the stateless API instances to
--    coordinate online index builds (single-winner claim + fencing).

CREATE TABLE IF NOT EXISTS device_fault_logs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    severity    TEXT        NOT NULL DEFAULT 'error',
    message     TEXT        NOT NULL
);

-- Preload historical fault logs so that the online index build is non-trivial.
INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)
SELECT 'dev-' || lpad((floor(random() * 2000))::int::text, 5, '0'),
       now() - (random() * interval '180 days'),
       (ARRAY['info', 'warn', 'error', 'critical'])[floor(random() * 4 + 1)],
       'preloaded fault event #' || g
FROM generate_series(1, 4000000) AS g;

-- One row per managed index. The UNIQUE constraint on index_name is what
-- guarantees that concurrent build requests across all API instances produce
-- exactly one claimed build.
CREATE TABLE IF NOT EXISTS index_build_jobs (
    id          UUID PRIMARY KEY,
    index_name  TEXT        NOT NULL UNIQUE,
    status      TEXT        NOT NULL CHECK (status IN ('building', 'ready', 'failed')),
    builder     TEXT,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per acknowledged fault event. event_id is a hash of the exact log
-- set, so a later extension/merge yields a different id and can never inherit
-- this acknowledgment. The API also creates this table on startup so that
-- databases initialized before this feature keep working.
CREATE TABLE IF NOT EXISTS fault_event_acks (
    device_id       TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    first_log_id    BIGINT      NOT NULL,
    last_log_id     BIGINT      NOT NULL,
    log_ids         BIGINT[]    NOT NULL,
    acked_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_by TEXT,
    PRIMARY KEY (device_id, event_id)
);

VACUUM ANALYZE device_fault_logs;
