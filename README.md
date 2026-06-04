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
| worker     | Pipeline stages (×2 replicas)     | —           |

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
| `LEASE_SECONDS`       | `60`      | Task lock lease; reaper resets expired leases|
| `MAX_RETRIES`         | `3`       | Attempts before DLQ                          |
| `BACKOFF_MS`          | `1000,4000,16000` | Per-attempt delay (tiered queues)    |
| `POISON_TOKEN`        | `POISON`  | Substring that triggers poison-pill routing  |

## Resilience mechanisms

| Mechanism | What it protects against |
|-----------|--------------------------|
| Transactional outbox | Partial write: DB state committed before broker message sent |
| Atomic DB claim (`UPDATE … WHERE status='PENDING'`) | Duplicate delivery / two workers racing |
| Task lease + reaper | Worker crashes after broker ack but before completing |
| Tiered retry queues (1s/4s/16s) | Head-of-line blocking in a single TTL queue |
| Dead Letter Queue | Messages exhausting all retries |
| Redis ZSET semaphore | Global TTS concurrency cap; leak-proof on crash |
| Semaphore heartbeat thread | Long synthesis not losing its slot |
| TTS content cache + SETNX lock | Duplicate text → reuse MinIO key; cache stampede prevention |
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

`./demo.sh` exercises all five verifiable requirements in sequence:

1. **Happy path** — full pipeline to COMPLETED
2. **Idempotency** — same Idempotency-Key returns original job, not a duplicate
3. **Semaphore cap** — 4 concurrent jobs; logs confirm max 3 simultaneous TTS slots
4. **Cache hit** — two jobs sharing identical lines; second skips synthesis
5. **Poison pill / DLQ** — manuscript with `POISON` token routed to DLQ

Crash recovery can be tested manually:
```bash
# While a job is in-progress, kill a worker
docker kill genai-workflow-worker-1
# Reaper fires within 30s, reclaims expired leases, worker-2 picks up
docker compose logs worker | grep "Reaper reclaimed"
```
