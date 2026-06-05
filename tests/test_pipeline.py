"""
Edge case integration tests for the genai-workflow pipeline.
Requires the full Docker Compose stack to be running:
    docker compose up -d

Run all edge cases:
    pytest tests/test_pipeline.py -v

Run one class:
    pytest tests/test_pipeline.py::TestPoisonPill -v

Skip slow crash tests:
    pytest tests/test_pipeline.py -v -k "not Crash"
"""

import subprocess
import time
import uuid

import psycopg2
import pytest
import requests

# ── Config ────────────────────────────────────────────────────────────────────

GATEWAY         = "http://localhost:8000"
DB_DSN          = "host=localhost port=5433 dbname=genai_pipeline user=genai password=genai_secret"
LEASE_SECONDS   = 30
REAPER_INTERVAL = 10
POLL_INTERVAL   = 0.3
FAST_TIMEOUT    = 45
POISON_TIMEOUT  = 40  # 3 retries × backoff (1s+4s+16s) before DLQ
CRASH_TIMEOUT   = 180
TTS_MAX         = 3

ONE_LINE  = "ALICE: It was a dark and stormy night."
TWO_LINES = "ALICE: It was a dark and stormy night.\nBOB: Are you certain about that?"
TEN_LINES = "\n".join([f"ALICE: Line {i}." if i % 2 == 0 else f"BOB: Line {i}." for i in range(1, 11)])


# ── Helpers ───────────────────────────────────────────────────────────────────

def unique_key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def submit(manuscript: str, key: str | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    r = requests.post(f"{GATEWAY}/jobs", json={"manuscript": manuscript}, headers=headers)
    assert r.status_code in (200, 202), f"submit failed: {r.status_code} {r.text}"
    return r.json()


def poll_until_terminal(job_id: str, timeout: int = FAST_TIMEOUT) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{GATEWAY}/jobs/{job_id}")
        assert r.status_code == 200
        data = r.json()
        if data["status"] in ("COMPLETED", "FAILED"):
            return data
        time.sleep(POLL_INTERVAL)
    pytest.fail(f"job {job_id} did not reach terminal state within {timeout}s")


def db_query(sql: str, params: tuple = ()) -> list:
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# ═════════════════════════════════════════════════════════════════════════════
# 1. Idempotency
# ═════════════════════════════════════════════════════════════════════════════

class TestIdempotency:
    def test_same_key_returns_same_job_id(self):
        key = unique_key("idem")
        r1 = submit("ALICE: First submission.", key=key)
        r2 = submit("BOB: Completely different text.", key=key)
        assert r1["job_id"] == r2["job_id"]

    def test_second_submission_returns_http_200(self):
        key = unique_key("idem-200")
        requests.post(f"{GATEWAY}/jobs", json={"manuscript": "ALICE: Original."},
                      headers={"Content-Type": "application/json", "Idempotency-Key": key})
        r2 = requests.post(f"{GATEWAY}/jobs", json={"manuscript": "BOB: Different."},
                           headers={"Content-Type": "application/json", "Idempotency-Key": key})
        assert r2.status_code == 200

    def test_duplicate_does_not_create_new_db_row(self):
        key = unique_key("idem-db")
        submit("ALICE: Original.", key=key)
        submit("BOB: Different text.", key=key)
        rows = db_query("SELECT COUNT(*) FROM jobs WHERE idempotency_key = %s", (key,))
        assert rows[0][0] == 1

    def test_different_keys_create_different_jobs(self):
        r1 = submit("ALICE: Hello.", key=unique_key("diff-a"))
        r2 = submit("ALICE: Hello.", key=unique_key("diff-b"))
        assert r1["job_id"] != r2["job_id"]


# ═════════════════════════════════════════════════════════════════════════════
# 2. Poison pill
# ═════════════════════════════════════════════════════════════════════════════

class TestPoisonPill:
    def test_poison_job_reaches_failed(self):
        resp = submit("ALICE: This line contains POISON and must fail.", key=unique_key("poison"))
        result = poll_until_terminal(resp["job_id"], timeout=POISON_TIMEOUT)
        assert result["status"] == "FAILED"

    def test_poison_job_has_error_message(self):
        resp = submit("ALICE: POISON in the manuscript.", key=unique_key("poison-err"))
        result = poll_until_terminal(resp["job_id"], timeout=POISON_TIMEOUT)
        assert result["error"] is not None and result["error"] != ""

    def test_poison_marked_failed_in_db(self):
        resp = submit("ALICE: This is POISON.", key=unique_key("poison-db"))
        poll_until_terminal(resp["job_id"], timeout=POISON_TIMEOUT)
        rows = db_query("SELECT status, error FROM jobs WHERE id = %s", (resp["job_id"],))
        assert rows[0][0] == "FAILED"
        assert rows[0][1] is not None

    def test_workers_healthy_after_poison(self):
        """Workers must keep processing clean jobs after a poison pill."""
        submit("ALICE: POISON pill here.", key=unique_key("poison-before"))
        resp = submit(ONE_LINE, key=unique_key("clean-after"))
        assert poll_until_terminal(resp["job_id"])["status"] == "COMPLETED"


# ═════════════════════════════════════════════════════════════════════════════
# 3. TTS semaphore — max TTS_MAX concurrent
# ═════════════════════════════════════════════════════════════════════════════

class TestSemaphore:
    def test_never_exceeds_max_concurrent_tts(self):
        """Submit 4 jobs at once; DB must never show more than 3 TTS IN_PROGRESS."""
        job_ids = [submit(TWO_LINES, key=unique_key(f"sem-{i}"))["job_id"] for i in range(4)]

        max_seen = 0
        deadline = time.time() + 8
        placeholders = ",".join(["%s"] * len(job_ids))
        while time.time() < deadline:
            rows = db_query(
                f"SELECT COUNT(*) FROM tasks WHERE job_id IN ({placeholders})"
                f" AND stage='TTS' AND status='IN_PROGRESS'",
                tuple(job_ids),
            )
            max_seen = max(max_seen, rows[0][0])
            time.sleep(0.2)

        assert max_seen <= TTS_MAX, f"Semaphore violated: saw {max_seen} concurrent (cap={TTS_MAX})"

        for job_id in job_ids:
            assert poll_until_terminal(job_id, timeout=30)["status"] == "COMPLETED"

    def test_all_burst_jobs_complete(self):
        job_ids = [submit(ONE_LINE, key=unique_key(f"burst-{i}"))["job_id"] for i in range(3)]
        for job_id in job_ids:
            assert poll_until_terminal(job_id, timeout=45)["status"] == "COMPLETED"


# ═════════════════════════════════════════════════════════════════════════════
# 4. Crash recovery  (slow ~3 min — skip with: pytest -k "not Crash")
# ═════════════════════════════════════════════════════════════════════════════

class TestCrashRecovery:
    def test_job_stays_pending_with_no_workers(self):
        subprocess.run(["docker", "compose", "stop", "worker"], check=True, capture_output=True)
        try:
            resp = submit(ONE_LINE, key=unique_key("no-worker"))
            time.sleep(3)
            assert requests.get(f"{GATEWAY}/jobs/{resp['job_id']}").json()["status"] == "PENDING"
        finally:
            subprocess.run(["docker", "compose", "start", "worker"], check=True, capture_output=True)
        poll_until_terminal(resp["job_id"], CRASH_TIMEOUT)

    def test_job_completes_after_worker_restart(self):
        subprocess.run(["docker", "compose", "stop", "worker"], check=True, capture_output=True)
        try:
            resp = submit(TEN_LINES, key=unique_key("restart"))
            job_id = resp["job_id"]
            time.sleep(3)
            assert requests.get(f"{GATEWAY}/jobs/{job_id}").json()["status"] == "PENDING"
        finally:
            subprocess.run(["docker", "compose", "start", "worker"], check=True, capture_output=True)
        assert poll_until_terminal(job_id, CRASH_TIMEOUT)["status"] == "COMPLETED"

    def test_reaper_reclaims_expired_lease(self):
        resp = submit(TEN_LINES, key=unique_key("reaper"))
        job_id = resp["job_id"]
        time.sleep(2)
        subprocess.run(["docker", "compose", "stop", "worker"], check=True, capture_output=True)

        rows = db_query(
            "SELECT COUNT(*) FROM tasks WHERE job_id=%s AND stage='TTS' AND status='IN_PROGRESS'",
            (job_id,),
        )
        stuck_count = rows[0][0]

        time.sleep(LEASE_SECONDS + REAPER_INTERVAL + 5)
        subprocess.run(["docker", "compose", "start", "worker"], check=True, capture_output=True)

        result = poll_until_terminal(job_id, CRASH_TIMEOUT)
        assert result["status"] == "COMPLETED"

        if stuck_count > 0:
            reclaimed = [t for t in result["tasks"] if t["stage"] == "TTS" and t["attempts"] >= 2]
            assert len(reclaimed) > 0, f"Expected reclaimed tasks but none found (stuck={stuck_count})"
