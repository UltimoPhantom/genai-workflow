# Implementation Plan

Ordered, incremental build. Each phase is runnable/verifiable before the next.
See `DESIGN.md` for the why behind every choice.

## Target layout
```
genai-workflow/
├── docker-compose.yml          # rabbitmq, postgres, redis, minio, gateway, worker(s)
├── .env                        # shared config (URLs, creds, knobs)
├── DESIGN.md  IMPLEMENTATION_PLAN.md  README.md
├── db/
│   └── schema.sql              # jobs, tasks, outbox + indexes
├── gateway/
│   ├── Dockerfile  requirements.txt
│   └── app.py                  # FastAPI: POST /jobs, GET /jobs/{id}, GET /jobs/{id}/dlq
└── worker/
    ├── Dockerfile  requirements.txt
    ├── main.py                 # consumer bootstrap (parse/tts/stitch), reaper, outbox relay
    ├── topology.py             # declare exchanges/queues/DLX/retry queues
    ├── db.py                   # psycopg pool + atomic claim/commit helpers
    ├── store.py                # MinIO (boto3) helpers
    ├── locks.py                # Redis fair semaphore + SETNX guards + TTS cache
    ├── stages/
    │   ├── parse.py            # split by speaker line, 15% 500 injection
    │   ├── tts.py              # semaphore + cache + simulate synth
    │   └── stitch.py           # all-done check + concat + notify
    └── config.py
```

## Phase 0 — Infra up (no app logic)
- [ ] `docker-compose.yml` with rabbitmq (mgmt UI), postgres, redis, minio.
- [ ] `db/schema.sql` (jobs, tasks, outbox, indexes incl. partial index on `locked_until`).
- [ ] Healthchecks + `depends_on: condition: service_healthy`.
- ✅ Verify: `docker compose up` → all 4 healthy; RabbitMQ UI :15672, MinIO console :9001.

## Phase 1 — Ingestion happy path
- [ ] Gateway `POST /jobs` (Idempotency-Key) → MinIO put + `INSERT job` + outbox `JobCreated` (one tx).
- [ ] Outbox relay in worker publishes pending outbox rows → `parse.queue`.
- [ ] `GET /jobs/{id}` returns job + tasks.
- ✅ Verify: POST a manuscript → row PENDING, file in MinIO, message in parse.queue.

## Phase 2 — Topology + parse stage
- [ ] `topology.py`: exchanges, `parse/tts/stitch` queues, DLX + tiered retry queues + DLQs.
- [ ] Parse consumer: atomic claim → split by speaker line → insert TTS task rows → outbox `TtsRequested` per chunk → commit → ack. Inject 15% 500.
- ✅ Verify: job moves PENDING→PROCESSING; one TTS task per speaker line; occasional parse retry visible.

## Phase 3 — TTS stage (semaphore + cache)
- [ ] `locks.py`: ZSET fair semaphore (acquire/heartbeat/release), `SETNX` cache lock.
- [ ] TTS consumer: claim → acquire sem(3) → hash-cache check → simulate synth + upload + write cache → release → outbox next → commit → ack.
- ✅ Verify: concurrency never >3 in logs; identical text → "cache hit" second time.

## Phase 4 — Retry / DLQ / backoff
- [ ] Failure path → republish to tiered retry queue by attempt; 3 strikes → DLQ.
- [ ] Release semaphore before backoff.
- [ ] `GET /jobs/{id}/dlq` (or a DLQ inspection endpoint).
- ✅ Verify: `POISON` manuscript → 1s/4s/16s backoff → tts.dlq; other jobs still complete.

## Phase 5 — Stitch & notify
- [ ] All-chunks-done check with atomic job flip to STITCHING.
- [ ] Concat chunks by `chunk_index` → upload final → COMPLETED → webhook/log.
- ✅ Verify: job reaches COMPLETED with `final_audio_key`; notify event logged.

## Phase 6 — Crash recovery + reaper
- [ ] Lease reaper: reset stale `IN_PROGRESS` tasks past `locked_until`.
- [ ] Run 2+ worker replicas.
- ✅ Verify: `docker kill` a worker mid-TTS → another resumes → job COMPLETES.

## Phase 7 — Polish & proof
- [ ] `README.md`: run instructions + a `make demo` / script exercising each requirement.
- [ ] Small load script: submit N jobs incl. duplicates + a poison pill.
- [ ] Structured logs (job_id, task_id, stage, attempt) so the pipeline is observable.

## Open knobs (defaults, all in `.env`)
- `PARSE_FAIL_RATE=0.15`, `TTS_MAX_CONCURRENCY=3`, `MAX_RETRIES=3`,
  `BACKOFF_MS=1000,4000,16000`, `LEASE_SECONDS=60`, `WORKER_PREFETCH=4`,
  `POISON_TOKEN=POISON`.
