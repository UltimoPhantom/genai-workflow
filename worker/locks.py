"""
Redis primitives used by the pipeline.

1. Fair semaphore (ZSET)
   ─────────────────────
   A sorted-set where each holder is a member scored by acquisition time.
   On acquire we trim stale entries (score < now - lease_ttl), count remaining,
   and add ourselves only if count < max_slots.

   Why ZSET, not INCR/DECR?
   INCR/DECR leaks on crash: the worker dies without decrementing, and the
   counter is permanently low. ZSET entries age out automatically via the
   ZREMRANGEBYSCORE trim on the next acquire — leak-proof by design.

   Heartbeat: callers refresh their score every ~lease/3 seconds so a
   long-running task does not lose its slot before it finishes.

2. TTS content cache
   ──────────────────
   Key: tts:cache:{sha256(chunk_text)} → MinIO output_key
   Lock: tts:lock:{sha256}  (SETNX with TTL)

   Stampede prevention: when two workers miss the cache simultaneously both
   try SETNX on the lock key.  The winner runs synthesis and writes the cache;
   the loser spins until the cache key appears, then reads it.
   This makes the (miss → synthesise → cache-write) path atomic per content hash.
"""

import time
import logging

import redis as redis_lib

from config import REDIS_URL, TTS_MAX_CONCURRENCY, LEASE_SECONDS, TTS_SIMULATE_SECS

log = logging.getLogger("worker.locks")

_redis: redis_lib.Redis | None = None

SEMAPHORE_KEY   = "tts:semaphore"
CACHE_PREFIX    = "tts:cache:"
# The stampede lock only needs to outlive ONE synthesis. Tying it to the long
# task lease (LEASE+30) was a bug: a worker that dies holding this lock would
# strand every other worker synthesising the same hash for the full TTL. Keep
# it just above synthesis time so an orphaned lock clears fast and a survivor
# can take over. If a legit synthesis ever overran this, the worst case is a
# duplicate (idempotent) synthesis — never corruption.
CACHE_LOCK_TTL    = max(int(TTS_SIMULATE_SECS * 3), 10)
CACHE_WAIT_TIMEOUT = 120.0   # overall ceiling for obtaining a cached result


def get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _redis


# ── Fair ZSET semaphore ────────────────────────────────────────────────────────

def sem_acquire(holder_id: str, timeout: float = 120.0) -> bool:
    """
    Block until a semaphore slot is available or timeout expires.
    Returns True if acquired, False on timeout.
    """
    r         = get_redis()
    deadline  = time.monotonic() + timeout
    lease_ttl = LEASE_SECONDS
    logged_full = False

    while time.monotonic() < deadline:
        now = time.time()

        with r.pipeline() as pipe:
            # Trim slots whose heartbeat has expired → auto-reclaim crashed workers
            pipe.zremrangebyscore(SEMAPHORE_KEY, "-inf", now - lease_ttl)
            pipe.zcard(SEMAPHORE_KEY)
            _, count = pipe.execute()

        if count >= TTS_MAX_CONCURRENCY and not logged_full:
            # Make the cap engaging visible: this holder is being throttled.
            log.info("sem FULL (%d/%d) holder=%s waiting for a slot",
                     count, TTS_MAX_CONCURRENCY, holder_id)
            logged_full = True

        if count < TTS_MAX_CONCURRENCY:
            # Try to claim a slot — use a Lua script so trim+add is atomic
            script = """
                local key     = KEYS[1]
                local holder  = ARGV[1]
                local now     = tonumber(ARGV[2])
                local ttl_ago = tonumber(ARGV[3])
                local max     = tonumber(ARGV[4])
                redis.call('ZREMRANGEBYSCORE', key, '-inf', ttl_ago)
                local count = redis.call('ZCARD', key)
                if count < max then
                    redis.call('ZADD', key, now, holder)
                    return 1
                end
                return 0
            """
            acquired = r.eval(
                script, 1,
                SEMAPHORE_KEY,
                holder_id,
                str(now),
                str(now - lease_ttl),
                str(TTS_MAX_CONCURRENCY),
            )
            if acquired:
                log.info("sem acquired holder=%s (slots_used=%d/%d)",
                         holder_id, count + 1, TTS_MAX_CONCURRENCY)
                return True

        time.sleep(0.5)

    log.warning("sem acquire TIMEOUT holder=%s", holder_id)
    return False


def sem_heartbeat(holder_id: str):
    """Refresh the holder's score so the lease does not expire mid-work."""
    get_redis().zadd(SEMAPHORE_KEY, {holder_id: time.time()})


def sem_release(holder_id: str):
    get_redis().zrem(SEMAPHORE_KEY, holder_id)
    log.info("sem released holder=%s", holder_id)


def sem_current_count() -> int:
    """How many slots are currently held (after trimming stale entries)."""
    r   = get_redis()
    now = time.time()
    r.zremrangebyscore(SEMAPHORE_KEY, "-inf", now - LEASE_SECONDS)
    return r.zcard(SEMAPHORE_KEY)


# ── TTS content cache ──────────────────────────────────────────────────────────

def cache_get(content_hash: str) -> str | None:
    """Return cached MinIO output_key if it exists, else None."""
    return get_redis().get(f"{CACHE_PREFIX}{content_hash}")


def cache_set(content_hash: str, output_key: str):
    """Store output_key in cache indefinitely (content-addressed → never stale)."""
    get_redis().set(f"{CACHE_PREFIX}{content_hash}", output_key)


def cache_lock_acquire(content_hash: str) -> bool:
    """
    SETNX lock for the (miss → synthesise → cache-write) path.
    Prevents cache stampede: only one worker calls the vendor per unique hash.
    Returns True if this worker won the lock, False if another holds it.
    """
    return bool(
        get_redis().set(
            f"tts:lock:{content_hash}",
            "1",
            nx=True,
            ex=CACHE_LOCK_TTL,
        )
    )


def cache_lock_release(content_hash: str):
    get_redis().delete(f"tts:lock:{content_hash}")


def cache_wait(content_hash: str, timeout: float = 120.0) -> str | None:
    """
    Spin-wait for another worker to populate the cache (lost the SETNX race).
    Returns the output_key once available, or None on timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = cache_get(content_hash)
        if val:
            return val
        time.sleep(0.5)
    return None
