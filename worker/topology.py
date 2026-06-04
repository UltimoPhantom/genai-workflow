"""
RabbitMQ topology declaration.

Exchange layout:
  jobs.exchange  (direct)  — main routing exchange for all pipeline events
  jobs.dlx       (direct)  — dead-letter exchange; receives messages from expired
                             retry queues and routes them back to the main queue,
                             OR routes to a DLQ when max retries exceeded.

Queues per stage (shown for TTS; parse/stitch follow the same pattern):

  tts.queue          — main consumer queue
  tts.retry.1s       — x-message-ttl=1000,  dead-letters → jobs.exchange / tts.queue
  tts.retry.4s       — x-message-ttl=4000
  tts.retry.16s      — x-message-ttl=16000
  tts.dlq            — parking lot after MAX_RETRIES exhausted

Why tiered queues, not a single TTL queue?
  RabbitMQ only expires messages from the head of a queue. A single shared
  TTL queue causes head-of-line blocking: a 16s message parks in front of a
  1s message. Three tiered queues avoid this entirely.
"""

import pika
from config import BACKOFF_SECS

MAIN_EXCHANGE = "jobs.exchange"
DLX           = "jobs.dlx"

STAGES = ("parse", "tts", "stitch")


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel):
    # ── Exchanges ──────────────────────────────────────────────────────────────
    channel.exchange_declare(MAIN_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(DLX,          exchange_type="direct", durable=True)

    for stage in STAGES:
        main_q   = f"{stage}.queue"
        dlq      = f"{stage}.dlq"

        # Main queue — messages come from jobs.exchange, dead-letter to DLX
        channel.queue_declare(
            main_q,
            durable=True,
            arguments={
                "x-dead-letter-exchange":    DLX,
                "x-dead-letter-routing-key": f"{stage}.dlq",
            },
        )
        channel.queue_bind(main_q, MAIN_EXCHANGE, routing_key=f"jobs.{stage}")

        # Dead-letter queue — final parking lot, no TTL
        channel.queue_declare(dlq, durable=True)
        channel.queue_bind(dlq, DLX, routing_key=f"{stage}.dlq")

        # Tiered retry queues — each TTL dead-letters back to the MAIN queue
        for i, delay_s in enumerate(BACKOFF_SECS):
            delay_ms = int(delay_s * 1000)
            retry_q  = f"{stage}.retry.{delay_ms}ms"

            channel.queue_declare(
                retry_q,
                durable=True,
                arguments={
                    "x-message-ttl":             delay_ms,
                    # On expiry, route back to the main stage queue
                    "x-dead-letter-exchange":    MAIN_EXCHANGE,
                    "x-dead-letter-routing-key": f"jobs.{stage}",
                },
            )
            # Retry queues are published to directly; no exchange binding needed
            # (we publish with routing_key = retry_q to the default exchange,
            #  or bind to DLX with a unique key if preferred — direct publish is simpler)

    # Convenience: stitch queue also accepts "jobs.stitch" routing key (set above)
