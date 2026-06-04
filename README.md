# Distributed Multi-Modal GenAI Pipeline

A locally runnable, event-driven microservices pipeline that turns text manuscripts
into simulated audio dramas. Built with Python · RabbitMQ · PostgreSQL · Redis · MinIO.

## Architecture

```
POST /jobs
    │
    ▼
Gateway ──► [outbox] ──► RabbitMQ
                              │
               ┌──────────────┴────────────────┐
               ▼                               │
         parse.queue                           │
         (Worker: parse stage)                 │
               │  15% random failure           │
               │  → retry.1s / 4s / 16s        │
               │  → parse.dlq (after 3 retries)│
               ▼                               │
         per-chunk outbox events               │
               │                               │
               ▼                               │
         tts.queue (one per chunk)             │
         (Worker: TTS stage)                   │
               │  Redis semaphore (max=3)       │
               │  Content cache (sha256 hash)   │
               │  → retry / tts.dlq             │
               ▼                               │
         remaining==0 → outbox ──────────────►─┘
                              │
                              ▼
                        stitch.queue
                        (Worker: stitch stage)
                              │
                              ▼
                        MinIO final asset
                        job status → COMPLETED
                        WEBHOOK log emitted
```

Choreography is **broker + DB only** — no Temporal, Airflow, Celery, or Step Functions.

See [DESIGN.md](DESIGN.md) for the full architecture doc, governing principle, state
model, and exhaustive gotcha/edge-case catalog.

## Stack

| Service    | Role                              | Port (host) |
|------------|-----------------------------------|-------------|
| postgres   | Job/task state, outbox table      | 5433        |
| redis      | TTS semaphore + content cache     | 6379        |
| rabbitmq   | Message broker + retry/DLQ queues | 5672 / 15672 (mgmt) |
| minio      | Manuscript + audio chunk storage  | 9000 / 9001 (console) |
| gateway    | REST API (FastAPI)                | 8000        |
| worker     | Pipeline stages (×5 replicas)     | —           |

> **Why 5 workers?** `pika` consumes one message at a time per process, so the
> worker count *is* the max parallel TTS attempts. With 5 workers a burst of
> work produces 5 simultaneous semaphore acquires → exactly 3 proceed and 2
> block, which is what makes the global concurrency cap actually observable.

## Running

**Prerequisites:** Docker + Docker Compose (v2)

```bash
# 1. Clone and start everything
git clone <repo>
cd genai-workflow
docker compose up --build -d

# 2. Wait for all services to be healthy (~30s)
docker compose ps

# 3. Run the end-to-end demo
chmod +x demo.sh
./demo.sh
```

## API

### Submit a job

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-1" \
  -d '{"manuscript": "ALICE: It was a dark night.\nBOB: Indeed it was."}'
```

Response:
```json
{"job_id": "uuid", "status": "PENDING"}
```

### Poll job status

```bash
curl -s http://localhost:8000/jobs/<job_id> | python3 -m json.tool
```

Response (completed):
```json
{
  "job_id": "...",
  "status": "COMPLETED",
  "final_audio_key": "audio/final/.../output.mp3",
  "tasks": [
    {"chunk_index": 0, "speaker": "ALICE", "status": "DONE", "attempts": 1, ...},
    ...
  ]
}
```

### Health check

```bash
curl http://localhost:8000/health
```

## Manuscript format

Speaker-line format (preferred):
```
ALICE: It was a dark and stormy night.
BOB: Are you sure about that?
ALICE: Absolutely certain.
```

Plain prose also works — falls back to paragraph-per-chunk with speaker `NARRATOR`.

### Poison pill

A manuscript containing the string `POISON` is immediately routed to `parse.dlq`
without retrying (deterministic failure path for testing).

## Configuration (`.env`)

| Variable              | Default   | Description                                  |
|-----------------------|-----------|----------------------------------------------|
| `PARSE_FAIL_RATE`     | `0.15`    | Fraction of parse attempts that fail (0–1)   |
| `TTS_MAX_CONCURRENCY` | `3`       | Global cap on simultaneous TTS operations    |
| `TTS_SIMULATE_SECONDS`| `2`       | Sleep per chunk (simulates vendor latency)   |
| `LEASE_SECONDS`       | `30`      | Task lock lease; reaper recovers expired leases (synthesis is ~2s, so 30s is a wide safety margin) |
| `REAPER_INTERVAL_SECONDS` | `10`  | How often the reaper sweeps for expired leases |
| `MAX_RETRIES`         | `3`       | Attempts before DLQ                          |
| `BACKOFF_MS`          | `1000,4000,16000` | Per-attempt delay (tiered queues)    |
| `WORKER_PREFETCH`     | `1`       | Fair round-robin; high prefetch lets one worker hoard the queue and masks the concurrency cap |
| `POISON_TOKEN`        | `POISON`  | Substring that triggers poison-pill routing  |

## Resilience mechanisms

| Mechanism | What it protects against |
|-----------|--------------------------|
| Transactional outbox | Partial write: DB state committed before broker message sent |
| Outbox relay `FOR UPDATE SKIP LOCKED` | N worker replicas each running a relay → without it, every event is published N times |
| Atomic DB claim (`UPDATE … WHERE status='PENDING' OR lease expired`) | Duplicate delivery / two workers racing |
| Task lease + reaper **that re-emits** | Worker crash mid-processing: row reset AND a fresh event published (a reset row with no message would never reprocess) |
| Stuck-job reconciler | Defense-in-depth: job with all chunks DONE but not flipped → reconciled to STITCHING |
| Job-row `FOR UPDATE` on completion check | Two final chunks committing concurrently both undercounting → job stuck in PROCESSING forever |
| Tiered retry queues (1s/4s/16s) | Head-of-line blocking in a single TTL queue |
| Dead Letter Queue + terminal `FAILED` | Messages exhausting retries; job marked FAILED (not left hanging) so status polling sees a terminal state |
| Redis ZSET semaphore | Global TTS concurrency cap; leak-proof on crash (stale slots age out) |
| Semaphore heartbeat thread | Long synthesis not losing its slot |
| Self-healing cache stampede lock | Short-TTL SETNX lock; if the synthesiser crashes, a survivor takes over instead of waiting forever |
| TTS content cache | Duplicate text → reuse MinIO key, never re-hit the vendor |
| Atomic STITCHING flip | Two workers both seeing remaining==0 → only one stitches |
| Idempotency-Key header | Client retrying POST /jobs → same job returned |
| Poison pill detection | Deterministic-fail manuscript → skip retries, go straight to DLQ |

## Observability

Workers emit structured logs. Key events to `grep` for:

```bash
# Happy path
docker compose logs worker | grep "COMPLETED\|WEBHOOK\|parse DONE\|TTS DONE"

# Retries
docker compose logs worker | grep "retry\|simulated 500"

# DLQ arrivals
docker compose logs worker | grep "dlq"

# Semaphore
docker compose logs worker | grep "sem acquired\|sem released"

# Cache hits
docker compose logs worker | grep "CACHE HIT"

# Reaper
docker compose logs worker | grep "Reaper reclaimed"
```

## Demo script

`./demo.sh` exercises all six verifiable requirements in sequence (11 assertions, exits non-zero on any failure):

1. **Happy path** — full pipeline to COMPLETED
2. **Idempotency** — same Idempotency-Key returns original job, not a duplicate
3. **Semaphore cap** — 6 jobs of unique text submitted at once; asserts the cap reaches `3/3` *and* that requests are throttled (`sem FULL` events)
4. **Cache hit** — two jobs sharing identical lines; second skips synthesis
5. **Poison pill / DLQ** — manuscript with `POISON` routed to DLQ; asserts terminal `FAILED` state, a physical message in `parse.dlq`, and that a normal job after it still completes (queue not blocked)
6. **Crash recovery** — catches a worker mid-synthesis, `docker kill`s it, and asserts the job still reaches COMPLETED with a reaper reclaim logged

> The semaphore and crash tests use **unique text per run** on purpose: cached
> text returns instantly, so to actually exercise concurrency and the kill
> window the synthesis must really run (cache miss → 2s sleep).
