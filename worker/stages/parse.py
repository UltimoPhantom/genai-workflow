"""
Parse stage: consumes jobs.parse, splits manuscript by speaker line,
inserts one TTS task per line, emits one jobs.tts event per chunk.

Simulated failure: PARSE_FAIL_RATE (15%) raises a transient 500 error
to exercise the retry/backoff path.
"""

import json
import logging
import random
import re
import uuid
from typing import Optional

import pika

import db
import store
from config import PARSE_FAIL_RATE, MAX_RETRIES, BACKOFF_SECS, POISON_TOKEN
from topology import MAIN_EXCHANGE, DLX

log = logging.getLogger("worker.parse")

# Matches "SPEAKER: text" lines (e.g. "ALICE: It was a dark night.")
SPEAKER_RE = re.compile(r"^([A-Z][A-Z0-9 _\-]{0,29}):\s*(.+)$", re.MULTILINE)


def _parse_segments(text: str) -> list[dict]:
    """Extract speaker lines; fall back to paragraph chunks if none found."""
    matches = SPEAKER_RE.findall(text)
    if matches:
        return [{"speaker": spk.strip(), "text": txt.strip(), "index": i}
                for i, (spk, txt) in enumerate(matches)]
    # Fallback: split on blank lines
    chunks = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return [{"speaker": "NARRATOR", "text": c, "index": i} for i, c in enumerate(chunks)]


def handle(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    _props,
    body: bytes,
):
    msg     = json.loads(body)
    job_id  = msg["job_id"]
    attempt = msg.get("attempt", 0)

    log.info("job=%s parse attempt=%d", job_id, attempt)

    # ── Idempotency guard: skip if job already past PENDING ──────────────────
    with db.conn() as cx:
        row = cx.execute(
            "SELECT status FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()

    if not row:
        log.warning("job=%s not found, acking and skipping", job_id)
        channel.basic_ack(method.delivery_tag)
        return

    if row[0] not in ("PENDING",):
        log.info("job=%s already status=%s, skipping parse", job_id, row[0])
        channel.basic_ack(method.delivery_tag)
        return

    # ── Simulated transient failure (15%) ────────────────────────────────────
    if random.random() < PARSE_FAIL_RATE:
        log.warning("job=%s parse simulated 500 (attempt=%d)", job_id, attempt)
        _retry_or_dlq(channel, method, msg, "parse", attempt)
        return

    try:
        # ── Download manuscript ───────────────────────────────────────────────
        text = store.get_object(msg["manuscript_key"]).decode()

        # ── Poison pill: manuscript containing POISON_TOKEN always fails ──────
        if POISON_TOKEN in text:
            log.warning("job=%s poison pill detected, routing to DLQ", job_id)
            _send_to_dlq(channel, method, msg, "parse")
            return

        segments = _parse_segments(text)
        if not segments:
            log.error("job=%s no segments parsed", job_id)
            _send_to_dlq(channel, method, msg, "parse")
            return

        # ── Insert TTS tasks + outbox events in ONE transaction ───────────────
        with db.conn() as cx:
            import hashlib

            # Atomic claim: mark job as PROCESSING
            rows = cx.execute(
                """
                UPDATE jobs SET status = 'PROCESSING'
                 WHERE id = %s AND status = 'PENDING'
                """,
                (job_id,),
            ).rowcount

            if rows == 0:
                log.info("job=%s already claimed by another worker", job_id)
                cx.rollback()
                channel.basic_ack(method.delivery_tag)
                return

            task_ids = []
            for seg in segments:
                task_id    = str(uuid.uuid4())
                # Cache key includes speaker: different voices for the same text
                # must not collide (e.g. ALICE "Hi" vs BOB "Hi" → different audio).
                input_hash = hashlib.sha256(
                    f"{seg['speaker']}:{seg['text']}".encode()
                ).hexdigest()
                cx.execute(
                    """
                    INSERT INTO tasks
                      (id, job_id, stage, chunk_index, status, input_hash, speaker, chunk_text)
                    VALUES (%s, %s, 'TTS', %s, 'PENDING', %s, %s, %s)
                    """,
                    (task_id, job_id, seg["index"], input_hash, seg["speaker"], seg["text"]),
                )
                cx.execute(
                    "INSERT INTO outbox (routing_key, payload) VALUES (%s, %s)",
                    (
                        "jobs.tts",
                        json.dumps({
                            "job_id":     job_id,
                            "task_id":    task_id,
                            "chunk_index": seg["index"],
                            "speaker":    seg["speaker"],
                            "chunk_text": seg["text"],
                            "input_hash": input_hash,
                            "attempt":    0,
                        }),
                    ),
                )
                task_ids.append(task_id)

            cx.commit()

        log.info("job=%s parse DONE: %d segments → TTS tasks created", job_id, len(segments))
        channel.basic_ack(method.delivery_tag)

    except Exception as exc:
        log.exception("job=%s parse unexpected error: %s", job_id, exc)
        _retry_or_dlq(channel, method, msg, "parse", attempt)


def _retry_or_dlq(channel, method, msg, stage, attempt):
    """Republish to the appropriate tiered retry queue, or send to DLQ."""
    channel.basic_ack(method.delivery_tag)  # ack original, we'll republish
    next_attempt = attempt + 1

    if next_attempt > MAX_RETRIES:
        _send_to_dlq(channel, method, msg, stage)
        return

    delay_ms = int(BACKOFF_SECS[min(attempt, len(BACKOFF_SECS) - 1)] * 1000)
    retry_q  = f"{stage}.retry.{delay_ms}ms"
    msg["attempt"] = next_attempt

    channel.basic_publish(
        exchange="",
        routing_key=retry_q,
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info(
        "job=%s queued retry attempt=%d delay=%dms queue=%s",
        msg.get("job_id"), next_attempt, delay_ms, retry_q,
    )


def _send_to_dlq(channel, method, msg, stage):
    dlq    = f"{stage}.dlq"
    job_id = msg.get("job_id")
    channel.basic_publish(
        exchange="",
        routing_key=dlq,
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    # Terminal state: mark the job FAILED so GET /jobs shows a final status
    # instead of leaving it stuck at PENDING forever.
    with db.conn() as cx:
        cx.execute(
            "UPDATE jobs SET status='FAILED', error=%s WHERE id=%s AND status != 'COMPLETED'",
            (f"parse stage routed to DLQ after exhausting retries", job_id),
        )
        cx.commit()
    log.error("job=%s sent to DLQ=%s, job marked FAILED", job_id, dlq)
    channel.basic_ack(method.delivery_tag)
