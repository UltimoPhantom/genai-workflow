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
from config import RABBITMQ_URL, WORKER_PREFETCH, LEASE_SECONDS
from topology import declare_topology, MAIN_EXCHANGE
from stages.parse import handle as handle_parse

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
            with db.conn() as cx:
                rows = cx.execute(
                    """
                    SELECT id, routing_key, payload
                      FROM outbox
                     WHERE published_at IS NULL
                     ORDER BY created_at
                     LIMIT 50
                    """
                ).fetchall()

            for row_id, routing_key, payload in rows:
                ch.basic_publish(
                    exchange=MAIN_EXCHANGE,
                    routing_key=routing_key,
                    body=json.dumps(payload) if not isinstance(payload, str) else payload,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                with db.conn() as cx:
                    cx.execute(
                        "UPDATE outbox SET published_at = now() WHERE id = %s",
                        (row_id,),
                    )
                    cx.commit()

            if rows:
                log.info("Outbox relay published %d events", len(rows))

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
    Periodically finds IN_PROGRESS tasks whose locked_until has expired
    (worker crashed after ack but before completing) and resets them to PENDING
    so another worker can re-claim them.
    """
    log.info("Lease reaper started (interval=30s, lease=%ds)", LEASE_SECONDS)
    while True:
        time.sleep(30)
        try:
            with db.conn() as cx:
                reclaimed = cx.execute(
                    """
                    UPDATE tasks
                       SET status = 'PENDING', locked_until = NULL
                     WHERE status = 'IN_PROGRESS'
                       AND locked_until < now()
                    """,
                ).rowcount
                cx.commit()
            if reclaimed:
                log.warning("Reaper reclaimed %d expired task leases", reclaimed)
        except Exception as e:
            log.exception("Reaper error: %s", e)


# ── Consumer thread ────────────────────────────────────────────────────────────

def start_consumers():
    log.info("Consumer thread starting")
    conn = _make_connection()
    ch   = conn.channel()

    declare_topology(ch)
    ch.basic_qos(prefetch_count=WORKER_PREFETCH)

    ch.basic_consume("parse.queue", handle_parse, auto_ack=False)
    # tts and stitch consumers registered in later phases

    log.info("Consumers registered: parse.queue")
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
