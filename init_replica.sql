-- Runs once on READ_REPLICA — subscribes to production's CDC stream
-- Tables are created automatically via the subscription's initial snapshot

-- Runs once on PRODUCTION — creates tables + CDC publication
-- ▼ EDIT THIS LINE to add/remove tables ▼
\set tables 'user_log_mobile,user_log_desktop'

CREATE TABLE IF NOT EXISTS user_log_mobile (
    id       BIGSERIAL PRIMARY KEY,
    uid      INT         NOT NULL,
    activity TEXT        NOT NULL,
    ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
    device   TEXT        NOT NULL,
    screen   TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mobile_ts ON user_log_mobile (ts);

CREATE TABLE IF NOT EXISTS user_log_desktop (
    id       BIGSERIAL PRIMARY KEY,
    uid      INT         NOT NULL,
    activity TEXT        NOT NULL,
    ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
    device   TEXT        NOT NULL,
    screen   TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_desktop_ts ON user_log_desktop (ts);

CREATE SUBSCRIPTION cdc_sub
CONNECTION 'host=production port=5432 dbname=cdc_db user=admin password=admin123'
PUBLICATION cdc_pub;
