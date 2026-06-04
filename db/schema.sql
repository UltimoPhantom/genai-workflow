-- ─── Extensions ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- gen_random_uuid()

-- ─── jobs ────────────────────────────────────────────────────────────────────
-- One row per submitted manuscript. Source of truth for overall job status.
CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key   TEXT UNIQUE NOT NULL,   -- dedupes client POST retries
    status            TEXT NOT NULL DEFAULT 'PENDING'
                          CHECK (status IN ('PENDING','PROCESSING','STITCHING','COMPLETED','FAILED')),
    manuscript_key    TEXT NOT NULL,           -- MinIO object path for input .txt
    final_audio_key   TEXT,                    -- MinIO object path for stitched output
    error             TEXT,                    -- terminal error message if FAILED
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);

-- ─── tasks ───────────────────────────────────────────────────────────────────
-- One row per (job, stage, chunk). Atomic claims happen here.
-- Stages: PARSE (one per job), TTS (one per speaker line), STITCH (one per job).
CREATE TABLE IF NOT EXISTS tasks (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    stage         TEXT NOT NULL CHECK (stage IN ('PARSE','TTS','STITCH')),
    chunk_index   INT  NOT NULL DEFAULT 0,     -- preserves speaker-line order for stitch
    status        TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING','IN_PROGRESS','DONE','FAILED')),
    input_hash    TEXT,                         -- sha256(chunk_text) → TTS cache key
    speaker       TEXT,                         -- e.g. "ALICE" extracted from line
    chunk_text    TEXT,                         -- the actual text for this chunk
    output_key    TEXT,                         -- MinIO object path for this chunk's audio
    attempts      INT  NOT NULL DEFAULT 0,
    locked_until  TIMESTAMPTZ,                  -- lease expiry for crash recovery
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tasks_job_stage_idx    ON tasks (job_id, stage);
CREATE INDEX IF NOT EXISTS tasks_status_idx       ON tasks (status);
-- Partial index: fast reaper query for expired leases
CREATE INDEX IF NOT EXISTS tasks_reaper_idx
    ON tasks (locked_until)
    WHERE status = 'IN_PROGRESS';

-- ─── outbox ──────────────────────────────────────────────────────────────────
-- Transactional outbox: events written in the same tx as job/task changes,
-- then published to RabbitMQ by the outbox relay. Eliminates "DB committed
-- but broker publish lost" gap.
CREATE TABLE IF NOT EXISTS outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    routing_key  TEXT        NOT NULL,   -- e.g. "jobs.parse", "jobs.tts", "jobs.stitch"
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ            -- NULL = not yet sent
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

-- ─── updated_at trigger ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE TRIGGER tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
