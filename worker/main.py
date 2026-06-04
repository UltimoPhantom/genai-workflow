"""
Worker entrypoint. Starts three concurrent threads:
  1. Outbox relay  — polls DB outbox and publishes pending events to RabbitMQ
  2. Reaper        — resets expired task leases back to PENDING
  3. Consumers     — parse / tts / stitch stage handlers
"""

import json
import logging
import threading
import time

import pika

import db
import store
from config import RABBITMQ_URL, WORKER_PREFETCH, LEASE_SECONDS, REAPER_INTERVAL_SECS
from topology import declare_topology, MAIN_EXCHANGE
from stages.parse import handle as handle_parse
from stages.tts import handle as handle_tts
from stages.stitch import handle as handle_stitch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("worker")


# ── RabbitMQ connection helper ─────────────────────────────────────────────────

def _make_connection() -> pika.BlockingConnection:
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 600
    params.blocked_connection_timeout = 300
    for attempt in range(20):
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            log.warning("RabbitMQ not ready (attempt %d): %s", attempt + 1, e)
            time.sleep(3)
    raise RuntimeError("Could not connect to RabbitMQ after 20 attempts")


# ── Outbox relay ───────────────────────────────────────────────────────────────

def outbox_relay():
    """
    Polls the outbox table every second for unpublished events and publishes
    them to RabbitMQ, then stamps published_at. Runs on its own connection so
    a publish failure doesn't affect consumers.
    """
    log.info("Outbox relay started")
    conn   = _make_connection()
    ch     = conn.channel()

    while True:
        try:
            # Every worker runs a relay. Without coordination all N relays would
            # SELECT the same unpublished rows and each publish them → N× broker
            # traffic + N× duplicate deliveries. FOR UPDATE SKIP LOCKED makes the
            # relays partition the work: each row is locked by exactly one relay
            # for the duration of its transaction; the others skip it. Select,
            # publish, and mark-published all happen in ONE transaction so the
            # row stays locked until it's durably marked sent.
            published = 0
            with db.conn() as cx:
                rows = cx.execute(
                    """
                    SELECT id, routing_key, payload
                      FROM outbox
                     WHERE published_at IS NULL
                     ORDER BY created_at
                     LIMIT 50
                     FOR UPDATE SKIP LOCKED
                    """
                ).fetchall()

                for row_id, routing_key, payload in rows:
                    ch.basic_publish(
                        exchange=MAIN_EXCHANGE,
                        routing_key=routing_key,
                        body=json.dumps(payload) if not isinstance(payload, str) else payload,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    cx.execute(
                        "UPDATE outbox SET published_at = now() WHERE id = %s",
                        (row_id,),
                    )
                    published += 1

                cx.commit()

            if published:
                log.info("Outbox relay published %d events", published)

        except Exception as e:
            log.exception("Outbox relay error: %s", e)
            # Reconnect on channel/connection failure
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(2)
            conn = _make_connection()
            ch   = conn.channel()

        time.sleep(1)


# ── Lease reaper ───────────────────────────────────────────────────────────────

def lease_reaper():
    """
    Periodically finds IN_PROGRESS tasks whose locked_until has expired (worker
    crashed mid-processing) and recovers them.

    Crucial subtlety: resetting the row to PENDING is NOT enough. When a worker
    is killed AFTER it claimed the task (lease held) but before ack, RabbitMQ
    redelivers the message to another worker — which finds the task still
    IN_PROGRESS with an unexpired lease, treats it as a duplicate, and ACKs it
    away. So by the time the lease expires there is no message left in the
    broker to drive the task. The reaper must therefore RE-EMIT the jobs.tts
    event (via the outbox) for every task it reclaims, closing the loop:
    reset → re-emit → reprocess. The re-emitted message carries the same
    task_id, so the atomic claim still dedupes against any stray redelivery.
    """
    log.info("Lease reaper started (interval=%ds, lease=%ds)",
             REAPER_INTERVAL_SECS, LEASE_SECONDS)
    while True:
        time.sleep(REAPER_INTERVAL_SECS)
        try:
            with db.conn() as cx:
                reclaimed = cx.execute(
                    """
                    UPDATE tasks
                       SET status = 'PENDING', locked_until = NULL
                     WHERE status = 'IN_PROGRESS'
                       AND locked_until < now()
                 RETURNING id, job_id, chunk_index, speaker, chunk_text, input_hash
                    """,
                ).fetchall()

                for task_id, job_id, chunk_index, speaker, chunk_text, input_hash in reclaimed:
                    cx.execute(
                        "INSERT INTO outbox (routing_key, payload) VALUES (%s, %s)",
                        (
                            "jobs.tts",
                            json.dumps({
                                "job_id":      str(job_id),
                                "task_id":     str(task_id),
                                "chunk_index": chunk_index,
                                "speaker":     speaker,
                                "chunk_text":  chunk_text,
                                "input_hash":  input_hash,
                                "attempt":     0,
                            }),
                        ),
                    )
                cx.commit()
            if reclaimed:
                log.warning("Reaper reclaimed + re-emitted %d expired task leases", len(reclaimed))

            # Stuck-job reconciler (defense-in-depth): a job whose TTS chunks are
            # ALL done but is still PROCESSING means a completion flip was missed
            # (e.g. legacy race, or the flipping worker died between flip and
            # outbox insert). Reconcile it to STITCHING and queue the event.
            # Idempotent: the WHERE clause only matches genuinely-stuck jobs.
            with db.conn() as cx:
                stuck = cx.execute(
                    """
                    SELECT j.id
                      FROM jobs j
                     WHERE j.status = 'PROCESSING'
                       AND NOT EXISTS (
                           SELECT 1 FROM tasks t
                            WHERE t.job_id = j.id AND t.stage = 'TTS'
                              AND t.status <> 'DONE'
                       )
                       AND EXISTS (
                           SELECT 1 FROM tasks t
                            WHERE t.job_id = j.id AND t.stage = 'TTS'
                       )
                     FOR UPDATE SKIP LOCKED
                    """
                ).fetchall()

                for (job_id,) in stuck:
                    cx.execute(
                        "UPDATE jobs SET status='STITCHING' WHERE id=%s AND status='PROCESSING'",
                        (job_id,),
                    )
                    cx.execute(
                        "INSERT INTO outbox (routing_key, payload) VALUES (%s, %s)",
                        ("jobs.stitch", json.dumps({"job_id": str(job_id), "attempt": 0})),
                    )
                    log.warning("Reaper reconciled stuck job=%s → STITCHING", job_id)
                cx.commit()
        except Exception as e:
            log.exception("Reaper error: %s", e)


# ── Consumer thread ────────────────────────────────────────────────────────────

def start_consumers():
    log.info("Consumer thread starting")
    conn = _make_connection()
    ch   = conn.channel()

    declare_topology(ch)
    ch.basic_qos(prefetch_count=WORKER_PREFETCH)

    ch.basic_consume("parse.queue",  handle_parse,  auto_ack=False)
    ch.basic_consume("tts.queue",    handle_tts,    auto_ack=False)
    ch.basic_consume("stitch.queue", handle_stitch, auto_ack=False)

    log.info("Consumers registered: parse.queue, tts.queue, stitch.queue")
    ch.start_consuming()


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    store.ensure_bucket()
    log.info("MinIO bucket ready")

    # Give Postgres a moment on first boot
    time.sleep(2)

    threading.Thread(target=outbox_relay, daemon=True, name="outbox-relay").start()
    threading.Thread(target=lease_reaper, daemon=True, name="lease-reaper").start()

    # Consumers run on the main thread (blocking)
    start_consumers()
