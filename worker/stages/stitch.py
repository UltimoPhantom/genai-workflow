"""
Stitch stage: consumes jobs.stitch events, reassembles audio chunks in
chunk_index order, uploads the final asset to MinIO, marks the job COMPLETED,
and fires a webhook notification (logged locally for the MVP).

Race protection: the job was already atomically flipped to STITCHING in the
TTS stage before this event was published, so if two stitch messages somehow
arrive (duplicate delivery), the second worker sees status != STITCHING and
skips without corrupting the DB.
"""

import json
import logging
import time

import pika

import db
import store
from config import MAX_RETRIES, BACKOFF_SECS
from topology import MAIN_EXCHANGE

log = logging.getLogger("worker.stitch")


def handle(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method:  pika.spec.Basic.Deliver,
    _props,
    body:    bytes,
):
    msg     = json.loads(body)
    job_id  = msg["job_id"]
    attempt = msg.get("attempt", 0)

    log.info("job=%s stitch attempt=%d", job_id, attempt)

    # ── Idempotency: only proceed if job is still STITCHING ──────────────────
    with db.conn() as cx:
        row = cx.execute("SELECT status FROM jobs WHERE id=%s", (job_id,)).fetchone()

    if not row or row[0] != "STITCHING":
        log.info("job=%s status=%s not STITCHING, skipping stitch", job_id, row[0] if row else "missing")
        channel.basic_ack(method.delivery_tag)
        return

    try:
        # ── Fetch all TTS chunks in order ────────────────────────────────────
        with db.conn() as cx:
            chunks = cx.execute(
                """
                SELECT chunk_index, speaker, chunk_text, output_key
                  FROM tasks
                 WHERE job_id = %s AND stage = 'TTS' AND status = 'DONE'
                 ORDER BY chunk_index
                """,
                (job_id,),
            ).fetchall()

        if not chunks:
            raise RuntimeError(f"job={job_id} no completed TTS chunks found")

        # ── Simulate stitching: concatenate chunk audio bytes ─────────────────
        log.info("job=%s stitching %d chunks", job_id, len(chunks))
        stitched = _stitch_chunks(job_id, chunks)

        # ── Upload final asset ────────────────────────────────────────────────
        final_key = f"audio/final/{job_id}/output.mp3"
        store.put_object(final_key, stitched, "audio/mpeg")
        log.info("job=%s final audio uploaded key=%s", job_id, final_key)

        # ── Mark job COMPLETED ────────────────────────────────────────────────
        with db.conn() as cx:
            cx.execute(
                "UPDATE jobs SET status='COMPLETED', final_audio_key=%s WHERE id=%s",
                (final_key, job_id),
            )
            cx.commit()

        # ── Notify user (webhook simulated as structured log) ─────────────────
        _notify(job_id, final_key, len(chunks))

        log.info("job=%s COMPLETED final_key=%s", job_id, final_key)
        channel.basic_ack(method.delivery_tag)

    except Exception as exc:
        log.exception("job=%s stitch error: %s", job_id, exc)
        _retry_or_dlq(channel, method, msg, attempt)


# ── Simulation ─────────────────────────────────────────────────────────────────

def _stitch_chunks(job_id: str, chunks: list) -> bytes:
    """
    Download each WAV chunk from MinIO and concatenate into a single valid WAV.
    Each Piper chunk is 16-bit mono 22050 Hz PCM. We strip each chunk's 44-byte
    RIFF header, concatenate the raw PCM frames, then write one new RIFF header
    covering the combined length.
    """
    import struct
    import io

    WAV_HEADER_SIZE = 44  # standard PCM WAV header
    SILENCE_FRAMES  = b"\x00\x00" * 2205  # ~100ms silence at 22050 Hz 16-bit mono

    pcm_parts = []
    sample_rate   = 22050
    num_channels  = 1
    bits_per_sample = 16

    for chunk_index, speaker, chunk_text, output_key in chunks:
        chunk_bytes = store.get_object(output_key)
        log.debug("job=%s stitched chunk=%d speaker=%s bytes=%d",
                  job_id, chunk_index, speaker, len(chunk_bytes))

        if chunk_bytes[:4] == b"RIFF":
            # Strip the WAV header — keep only PCM audio data
            pcm_parts.append(chunk_bytes[WAV_HEADER_SIZE:])
        else:
            # Fallback: simulated dummy audio (not real WAV)
            pcm_parts.append(chunk_bytes)

        # Add a short silence gap between chunks
        if chunk_index < len(chunks) - 1:
            pcm_parts.append(SILENCE_FRAMES)

    raw_pcm    = b"".join(pcm_parts)
    data_size  = len(raw_pcm)
    byte_rate  = sample_rate * num_channels * bits_per_sample // 8
    block_align = num_channels * bits_per_sample // 8

    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + data_size,       # file size - 8
        b"WAVE",
        b"fmt ",
        16,                   # PCM chunk size
        1,                    # audio format: PCM
        num_channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    )
    return header + raw_pcm


def _notify(job_id: str, final_key: str, chunk_count: int):
    """
    Simulate webhook: in production this would POST to a callback URL.
    Assumption: best-effort logged event for MVP; prod needs its own
    outbox + retry for guaranteed delivery.
    """
    log.info(
        "WEBHOOK job=%s status=COMPLETED final_audio_key=%s chunks=%d",
        job_id, final_key, chunk_count,
    )


# ── Retry / DLQ ────────────────────────────────────────────────────────────────

def _retry_or_dlq(channel, method, msg, attempt):
    channel.basic_ack(method.delivery_tag)
    next_attempt = attempt + 1

    if next_attempt > MAX_RETRIES:
        job_id = msg["job_id"]
        channel.basic_publish(
            exchange="",
            routing_key="stitch.dlq",
            body=json.dumps(msg),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        # Terminal: stitch could not assemble the final asset. Mark FAILED so
        # the job doesn't linger in STITCHING forever.
        with db.conn() as cx:
            cx.execute(
                "UPDATE jobs SET status='FAILED', error=%s WHERE id=%s AND status != 'COMPLETED'",
                ("stitch stage routed to DLQ after exhausting retries", job_id),
            )
            cx.commit()
        log.error("job=%s sent to stitch.dlq, job marked FAILED", job_id)
        return

    delay_ms = int(BACKOFF_SECS[min(attempt, len(BACKOFF_SECS) - 1)] * 1000)
    retry_q  = f"stitch.retry.{delay_ms}ms"
    msg["attempt"] = next_attempt

    channel.basic_publish(
        exchange="",
        routing_key=retry_q,
        body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.warning("job=%s stitch retry attempt=%d delay=%dms", msg["job_id"], next_attempt, delay_ms)
