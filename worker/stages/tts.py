"""
TTS stage: consumes jobs.tts events, synthesises audio (simulated), uploads
chunk to MinIO, checks whether all chunks for the job are done, and if so
emits a jobs.stitch event.

Key resilience mechanisms:
  - Atomic DB task claim (idempotency + crash recovery via lease)
  - Redis ZSET semaphore: max TTS_MAX_CONCURRENCY concurrent globally
  - Redis content cache: same text hash → skip vendor, reuse MinIO key
  - Stampede prevention: SETNX lock per hash guards the miss→synth→cache path
  - Heartbeat thread: refreshes semaphore slot while synthesis sleeps
  - Semaphore released BEFORE backing off on retry (slot not held during wait)
  - Tiered retry queues with exponential backoff; DLQ after MAX_RETRIES
"""

import json
import logging
import threading
import time
import uuid

import pika

import db
import store
import locks
from config import (
    TTS_SIMULATE_SECS, MAX_RETRIES, BACKOFF_SECS,
    POISON_TOKEN, LEASE_SECONDS,
)
from topology import MAIN_EXCHANGE

log = logging.getLogger("worker.tts")


def handle(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method:  pika.spec.Basic.Deliver,
    _props,
    body:    bytes,
):
    msg         = json.loads(body)
    job_id      = msg["job_id"]
    task_id     = msg["task_id"]
    chunk_index = msg["chunk_index"]
    speaker     = msg["speaker"]
    chunk_text  = msg["chunk_text"]
    input_hash  = msg["input_hash"]
    attempt     = msg.get("attempt", 0)

    log.info("job=%s task=%s tts attempt=%d speaker=%s", job_id, task_id, attempt, speaker)

    # ── Poison pill: always fails ────────────────────────────────────────────
    if POISON_TOKEN in chunk_text:
        log.warning("job=%s task=%s poison pill in TTS chunk, routing to DLQ", job_id, task_id)
        _send_to_dlq(channel, method, msg)
        return

    # ── Atomic DB claim ──────────────────────────────────────────────────────
    with db.conn() as cx:
        claimed = cx.execute(
            """
            UPDATE tasks
               SET status = 'IN_PROGRESS',
                   locked_until = now() + %s::interval,
                   attempts = attempts + 1
             WHERE id = %s
               AND (status = 'PENDING'
                    OR (status = 'IN_PROGRESS' AND locked_until < now()))
            """,
            (f"{LEASE_SECONDS} seconds", task_id),
        ).rowcount
        cx.commit()

    if claimed == 0:
        log.info("job=%s task=%s already claimed or done, skipping", job_id, task_id)
        channel.basic_ack(method.delivery_tag)
        return

    # ── Acquire global TTS semaphore (max TTS_MAX_CONCURRENCY) ───────────────
    holder_id = f"{task_id}"
    acquired  = locks.sem_acquire(holder_id)
    if not acquired:
        log.error("job=%s task=%s semaphore timeout, will retry", job_id, task_id)
        # Release claim so reaper doesn't need to
        with db.conn() as cx:
            cx.execute("UPDATE tasks SET status='PENDING', locked_until=NULL WHERE id=%s", (task_id,))
            cx.commit()
        _retry_or_dlq(channel, method, msg, attempt)
        return

    # ── Heartbeat thread: keep semaphore slot alive during synthesis ──────────
    stop_hb = threading.Event()

    def _heartbeat():
        interval = max(1, LEASE_SECONDS // 3)
        while not stop_hb.wait(interval):
            locks.sem_heartbeat(holder_id)
            log.debug("job=%s task=%s semaphore heartbeat", job_id, task_id)

    hb_thread = threading.Thread(target=_heartbeat, daemon=True, name=f"hb-{task_id[:8]}")
    hb_thread.start()

    output_key = None
    try:
        # ── Cache check (with stampede prevention) ────────────────────────────
        output_key = locks.cache_get(input_hash)

        if output_key:
            log.info("job=%s task=%s CACHE HIT hash=%s output_key=%s",
                     job_id, task_id, input_hash[:8], output_key)

        else:
            # Try to become the synthesiser for this hash
            got_lock = locks.cache_lock_acquire(input_hash)

            if got_lock:
                # Double-check cache in case another worker snuck in
                output_key = locks.cache_get(input_hash)
                if not output_key:
                    output_key = _synthesise(job_id, task_id, speaker, chunk_text)
                    locks.cache_set(input_hash, output_key)
                    log.info("job=%s task=%s SYNTHESISED + cached hash=%s", job_id, task_id, input_hash[:8])
                locks.cache_lock_release(input_hash)

            else:
                # Lost the stampede race — wait for the winner to populate cache
                log.info("job=%s task=%s waiting for cache (lost stampede race)", job_id, task_id)
                output_key = locks.cache_wait(input_hash)
                if not output_key:
                    raise RuntimeError(f"Cache wait timed out for hash {input_hash[:8]}")
                log.info("job=%s task=%s cache populated by peer, reusing key=%s",
                         job_id, task_id, output_key)

    except Exception as exc:
        log.exception("job=%s task=%s TTS error: %s", job_id, task_id, exc)
        stop_hb.set()
        locks.sem_release(holder_id)  # release slot BEFORE backoff
        # Reset claim so reaper won't fight us
        with db.conn() as cx:
            cx.execute("UPDATE tasks SET status='PENDING', locked_until=NULL WHERE id=%s", (task_id,))
            cx.commit()
        _retry_or_dlq(channel, method, msg, attempt)
        return

    finally:
        stop_hb.set()

    # Semaphore released as soon as synthesis is done (not held during DB/ack)
    locks.sem_release(holder_id)

    # ── Commit task DONE + check if all chunks complete (same tx) ────────────
    with db.conn() as cx:
        cx.execute(
            "UPDATE tasks SET status='DONE', output_key=%s, locked_until=NULL WHERE id=%s",
            (output_key, task_id),
        )

        remaining = cx.execute(
            "SELECT count(*) FROM tasks WHERE job_id=%s AND stage='TTS' AND status != 'DONE'",
            (job_id,),
        ).fetchone()[0]

        if remaining == 0:
            # Atomically claim the stitch transition — prevents two workers
            # both seeing remaining=0 and both publishing stitch events
            flipped = cx.execute(
                "UPDATE jobs SET status='STITCHING' WHERE id=%s AND status='PROCESSING'",
                (job_id,),
            ).rowcount

            if flipped:
                cx.execute(
                    "INSERT INTO outbox (routing_key, payload) VALUES (%s, %s)",
                    ("jobs.stitch", json.dumps({"job_id": job_id, "attempt": 0})),
                )
                log.info("job=%s all TTS chunks DONE → stitch event queued", job_id)

        cx.commit()

    log.info("job=%s task=%s TTS DONE (remaining=%d)", job_id, task_id, remaining)
    channel.basic_ack(method.delivery_tag)


# ── Simulation ─────────────────────────────────────────────────────────────────

def _synthesise(job_id: str, task_id: str, speaker: str, text: str) -> str:
    """Simulate TTS: sleep, then upload a dummy audio file to MinIO."""
    log.info("job=%s task=%s synthesising speaker=%s len=%d chars",
             job_id, task_id, speaker, len(text))
    time.sleep(TTS_SIMULATE_SECS)

    # Dummy audio: a text file pretending to be an mp3
    content = f"[AUDIO] speaker={speaker} text={text}".encode()
    key     = f"audio/chunks/{job_id}/{task_id}.mp3"
    store.put_object(key, content, "audio/mpeg")
    return key


# ── Retry / DLQ ────────────────────────────────────────────────────────────────

def _retry_or_dlq(channel, method, msg, attempt):
    channel.basic_ack(method.delivery_tag)
    next_attempt = attempt + 1

    if next_attempt > MAX_RETRIES:
        _send_to_dlq(channel, method, msg)
        return

    delay_ms = int(BACKOFF_SECS[min(attempt, len(BACKOFF_SECS) - 1)] * 1000)
    retry_q  = f"tts.retry.{delay_ms}ms"
    msg["attempt"] = next_attempt

    channel.basic_publish(
        exchange="",
        routing_key=retry_q,
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.warning("job=%s task=%s retry attempt=%d delay=%dms",
                msg["job_id"], msg["task_id"], next_attempt, delay_ms)


def _send_to_dlq(channel, method, msg):
    channel.basic_publish(
        exchange="",
        routing_key="tts.dlq",
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.error("job=%s task=%s sent to tts.dlq after exhausting retries", msg["job_id"], msg["task_id"])
    channel.basic_ack(method.delivery_tag)
