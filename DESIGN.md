# Distributed Multi-Modal GenAI Pipeline — Design

> Take-home: build the async engine for a platform that turns a text manuscript
> into a produced "audio drama", using only core infra primitives (no Temporal /
> Airflow / Step Functions / Celery).

## 0. Chosen stack & top-level decisions

| Decision | Choice | Why |
|---|---|---|
| Language | **Python (FastAPI workers + gateway)** | Fastest path for a 48h build; rich async + AMQP/Redis/boto3 ecosystem. |
| Broker | **RabbitMQ** | Native dead-letter exchanges, per-message TTL, manual ack — DLQ + backoff are first-class, not hand-rolled (as they would be in Kafka). |
| State DB | **Postgres** | Atomic `UPDATE ... WHERE` claims + transactions are the backbone of idempotency & crash recovery. |
◊| Cache/Lock | **Redis** | Distributed semaphore, idempotency guard, TTS content cache. |
| Object store | **MinIO** | Local S3 for manuscripts + intermediate/final audio. |
| Choreography | **Stage-per-queue** | `parse → tts → stitch`, each stage emits the next event. Independent retry/DLQ/scaling per stage. |
| TTS unit | **By speaker line** (`ALICE: ...`) | Maps to the audio-drama framing; gives natural parallel chunks + meaningful stitch order. |

---

## 1. The governing principle

> **The broker is for *delivery*. The database is for *truth*. Never act on a
> message alone — always reconcile it against DB state via an atomic claim.**

We assume **at-least-once delivery** (exactly-once does not exist in distributed
systems). Therefore *every* consumer must be idempotent. The **DB state machine +
atomic claims** is what replaces the banned orchestrator.

---

## 2. Architecture

```
   POST /jobs ─► API Gateway (FastAPI) ─► MinIO (save .txt)
                      │                 └► Postgres (INSERT job PENDING)
                      │                 └► RabbitMQ publish JobCreated
   GET /jobs/{id} ◄───┘  (reads job + per-task status from Postgres)

   RabbitMQ:  parse.queue → tts.queue → stitch.queue
              + per-stage retry queues (tiered TTL) + per-stage DLQ

   Worker(s) (N replicas) consume each stage, talk to:
     Postgres (claim/commit task state)  Redis (semaphore, locks, TTS cache)
     MinIO (read input / write audio)    RabbitMQ (publish next event)
```

Each worker process registers consumers for `parse.queue`, `tts.queue`,
`stitch.queue`. Finishing a stage **publishes the next event** — no central brain.

---

## 3. State model (the real orchestrator)

### `jobs`
| column | notes |
|---|---|
| `id` uuid PK | |
| `idempotency_key` text UNIQUE | dedupes client POST retries |
| `status` | `PENDING → PROCESSING → COMPLETED / FAILED` |
| `manuscript_key` | MinIO path to input `.txt` |
| `final_audio_key` | MinIO path to stitched output |
| `error` | last terminal error (for FAILED) |
| `created_at / updated_at` | DB-clock timestamps |

### `tasks` (per chunk, per stage)
| column | notes |
|---|---|
| `id` uuid PK, `job_id` FK | |
| `stage` | `PARSE / TTS / STITCH` |
| `chunk_index` | ordering for stitch |
| `status` | `PENDING / IN_PROGRESS / DONE / FAILED` |
| `input_hash` | sha256 of chunk text → TTS cache key |
| `output_key` | MinIO path to this chunk's audio |
| `attempts` | retry counter |
| `locked_until` | **lease** for crash recovery (DB clock) |

### `outbox` (transactional publish)
| column | notes |
|---|---|
| `id`, `routing_key`, `payload`, `published_at` | written in the **same tx** as the job/task change; a relay publishes then stamps `published_at`. Removes the "DB committed but broker publish lost" gap. |

**The atomic claim** (idempotency + crash recovery in one statement):
```sql
UPDATE tasks
   SET status='IN_PROGRESS', locked_until = now() + interval '60 seconds',
       attempts = attempts + 1
 WHERE id = :id
   AND (status='PENDING' OR (status='IN_PROGRESS' AND locked_until < now()));
-- rows affected = 0  →  someone else owns it / it's done  →  ACK & skip
```

---

## 4. Per-message lifecycle & ACK placement

ACK placement is the crux of crash safety. **Ack only after the effect is durably committed.**

1. Receive msg → **atomic claim** in DB. Claim fails → `ack` & return.
2. Do work (download from MinIO, simulate, upload result to MinIO).
3. **Commit DB**: task `DONE` + `output_key` + write next event to `outbox` (same tx).
4. `ack`. (Outbox relay publishes the next event independently.)

Death between any step → message unacked → RabbitMQ redelivers → re-claim. The
lease covers death *after* commit but *before* ack (next stage + a redelivery may
both exist → next-stage idempotency absorbs the duplicate).

---

## 5. The four hard requirements — implementation

### 5.1 Idempotent consumers
- Atomic DB claim (§3) is the primary guard: redelivered `JobCreated` finds the
  job already `PROCESSING`/`COMPLETED` → 0 rows → ack & skip.
- Plus a short-TTL Redis `SETNX job:{id}:lock` as a cheap first-line guard against
  two workers racing the same instant.

### 5.2 TTS concurrency = 3 globally  →  Redis **fair semaphore (ZSET)**
- ❌ `INCR/DECR` **leaks** on crash (counter never decremented → stuck < 3 forever).
- ✅ Sorted-set semaphore:
  - **acquire**: `ZREMRANGEBYSCORE` to trim entries older than lease TTL; `ZCARD`;
    if `< 3`, `ZADD` self with score `now()`.
  - **heartbeat**: refresh own score while working.
  - **release**: `ZREM` self.
  - **crash**: stale entry ages out & is trimmed → slot auto-reclaimed. Leak-proof.
- A worker that **Nacks TTS for retry must release its slot before backing off**,
  or the backoff wait burns one of the 3 global slots.

### 5.3 Cost/idempotency cache (Constraint B)
- Cache key = `sha256(chunk_text)`. Hit → return existing MinIO URL, **no vendor call**.
- ❌ Naive get-then-call → **cache stampede**: two workers miss simultaneously,
  both call the vendor.
- ✅ `SETNX tts:lock:{hash}` around the (miss → call → write-cache) path; the loser
  waits and reads the winner's result. Cache check is **atomic with** the vendor call.

### 5.4 DLQ + 3 retries + exponential backoff  →  **tiered delay queues**
- On failure, republish to `tts.retry.1s / .4s / .16s` keyed by attempt #. Each
  retry queue has a message-TTL and dead-letters **back** to the main queue when TTL
  expires → that is the backoff delay.
- After attempt 3 → route to `tts.dlq` (parking lot; inspected via API).
- ❌ A **single** shared TTL queue causes head-of-line blocking (RabbitMQ only
  expires from the queue head → a 16s msg blocks a 1s msg behind it). Tiered queues
  avoid this — **this is a graded gotcha**.
- "Without blocking the rest of the queue": the failing msg leaves the main queue
  during its backoff; `prefetch` lets healthy msgs flow past.

### 5.5 Crash recovery — two layers
- (a) Manual ack after commit → broker redelivers unacked on `docker kill`.
- (b) **Lease reaper**: periodic sweep resets `tasks` that are `IN_PROGRESS` with
  `locked_until < now()` back to `PENDING` / republishes. Covers death-after-ack.

---

## 6. Pipeline stages

1. **Ingestion (gateway)** — save `.txt` to MinIO, `INSERT job PENDING` + outbox
   `JobCreated` in one tx.
2. **Parse (simulated LLM)** — download, split **by speaker line** into segments,
   create one `TTS` task row per segment, inject **15% 500 error** (transient →
   retried). Emits one `TtsRequested` per segment.
3. **TTS (simulated vendor)** — acquire semaphore (max 3), check hash cache, else
   "synthesize" (sleep) + upload chunk audio, write cache, release semaphore.
4. **Stitch & Notify** — when **all** TTS tasks for a job are `DONE`, the task that
   flips the count to 0 atomically claims the job (`UPDATE jobs SET status='STITCHING'
   WHERE id=? AND status='PROCESSING'`), concatenates chunks **in `chunk_index`
   order**, uploads final asset, sets `COMPLETED`, fires webhook/logs event.

---

## 7. Edge cases / gotchas / implicit assumptions (the email's real ask)

**Ingestion**
- 🔴 Partial write across MinIO + DB + broker (no shared tx) → **Transactional Outbox**.
- 🟡 Duplicate POST (client retry) → `Idempotency-Key` header → unique constraint → return existing job.
- 🟡 "Large" manuscript → message carries only `job_id` + `manuscript_key`; workers stream from MinIO, never embed the blob.

**Parse**
- 🟡 The 15% 500 is **transient** (retry succeeds) vs a **poison pill** (permanent → DLQ). Same retry machinery, distinguished only by retry exhaustion.

**TTS**
- 🔴 Semaphore leak on crash → ZSET + heartbeat (§5.2).
- 🔴 Cache stampede → `SETNX` per hash (§5.3).
- 🟡 Stale cached MinIO URL (object deleted) → verify object exists, or treat as content-addressed & never GC within job lifetime. Documented tradeoff.
- 🟡 Semaphore vs backoff interaction → release slot before backing off.

**Stitch**
- 🔴 Partial completion → stitch only when all chunks DONE; the completion check
  races (two workers see count 0) → guard with atomic job state flip.
- 🟡 Reassemble by `chunk_index`, not completion order.

**Cross-cutting**
- 🟡 Poison pill blocking queue → prefetch + retry queues remove it from head.
- 🟡 Webhook delivery → **assumption:** best-effort logged event for MVP; prod would need its own outbox + retry. Stated explicitly.
- 🟡 Lease clock skew → use DB `now()`, never worker wall-clock.
- 🟡 Observability (no orchestrator UI) → `GET /jobs/{id}` exposes per-task status + DLQ visibility so a reviewer can watch progress.

**Stated assumptions**
1. **TTS unit = speaker line** (PDF never specifies — #1 implicit assumption).
2. **At-least-once delivery** → everything idempotent (chosen over impossible exactly-once).
3. **DB = source of truth, broker = transport.**
4. **Failures transient-by-default, permanent-after-3-retries.**

---

## 8. How a reviewer verifies each requirement

| Requirement | How to demonstrate |
|---|---|
| Idempotency | Publish the same `JobCreated` twice → one COMPLETED job, no dup tasks. |
| TTS max 3 | Submit many jobs → log shows semaphore holders never exceed 3. |
| Cache | Submit identical text twice → 2nd skips vendor (log "cache hit", no extra sleep). |
| DLQ + backoff | Submit a `POISON` manuscript → 3 retries w/ growing delay → lands in `tts.dlq`; other jobs keep completing. |
| Crash recovery | `docker kill` a worker mid-TTS → another worker resumes; job still COMPLETES. |
