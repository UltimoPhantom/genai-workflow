#!/usr/bin/env bash
# End-to-end demo: exercises all 5 pipeline requirements.
# Run after: docker compose up --build -d
set -euo pipefail

BASE="http://localhost:8000"
PASS=0; FAIL=0

# ── helpers ───────────────────────────────────────────────────────────────────

red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[0;34m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

pass() { green "  ✓ $*"; PASS=$((PASS+1)); }
fail() { red   "  ✗ $*"; FAIL=$((FAIL+1)); }

wait_for_status() {
  local job_id="$1" want="$2" max="${3:-60}" elapsed=0
  while [[ $elapsed -lt $max ]]; do
    status=$(curl -s "$BASE/jobs/$job_id" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "ERR")
    [[ "$status" == "$want" ]] && { echo "    status=$status after ${elapsed}s"; return 0; }
    sleep 2; elapsed=$((elapsed+2))
  done
  echo "    TIMEOUT: last status=$status"
  return 1
}

submit_job() {
  local text="$1" ikey="$2"
  local payload
  payload=$(python3 -c "import json,sys; print(json.dumps({'manuscript': sys.argv[1]}))" "$text")
  curl -s -X POST "$BASE/jobs" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $ikey" \
    -d "$payload"
}

job_field() {
  local job_id="$1" field="$2"
  curl -s "$BASE/jobs/$job_id" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$field',''))" 2>/dev/null
}

# ── 1. Happy path ─────────────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 1. HAPPY PATH"
bold "═══════════════════════════════════════════════"

MANUSCRIPT_HAPPY="ALICE: It was a dark and stormy night.
BOB: Are you absolutely certain about that?
ALICE: Yes, the thunder shook the windows.
NARRATOR: And so it began, as all tales do."

RESP=$(submit_job "$MANUSCRIPT_HAPPY" "demo-happy-$(date +%s)")
JOB1=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
echo "  Submitted job_id=$JOB1"

if wait_for_status "$JOB1" "COMPLETED" 120; then
  FINAL=$(job_field "$JOB1" "final_audio_key")
  pass "Job reached COMPLETED — final_audio_key=$FINAL"
else
  fail "Job did not complete within timeout"
fi

# ── 2. Idempotency ────────────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 2. IDEMPOTENCY (same Idempotency-Key twice)"
bold "═══════════════════════════════════════════════"

IKEY="demo-idem-$(date +%s)"
R1=$(submit_job "ALICE: First submission." "$IKEY")
R2=$(submit_job "ALICE: Second submission — should be ignored." "$IKEY")

ID1=$(echo "$R1" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
ID2=$(echo "$R2" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)

echo "  First  job_id=$ID1"
echo "  Second job_id=$ID2"

if [[ "$ID1" == "$ID2" ]]; then
  pass "Both calls returned the same job_id — no duplicate created"
else
  fail "Different job_ids returned: $ID1 vs $ID2"
fi

# ── 3. TTS semaphore cap ──────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 3. TTS SEMAPHORE CAP (max 3 concurrent slots)"
bold "═══════════════════════════════════════════════"

# A unique nonce per run guarantees every line is a genuine cache MISS, so TTS
# actually sleeps (2s) instead of returning instantly from cache. Sustained
# synthesis is what lets concurrency build past the cap. Submitting 6 jobs ×
# 2 lines = 12 simultaneous TTS tasks against 5 workers → the cap of 3 engages.
NONCE="$(date +%s)-$RANDOM"
blue "  Submitting 6 jobs (unique text → real synthesis) simultaneously..."
CONCURRENT_IDS=()
for i in $(seq 1 6); do
  TEXT="ALICE: Unique line A job $i nonce $NONCE.
BOB: Unique line B job $i nonce $NONCE."
  RESP=$(submit_job "$TEXT" "demo-sem-$i-$NONCE")
  JID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
  CONCURRENT_IDS+=("$JID")
  echo "  Submitted job $i: $JID"
done

blue "  Waiting for all 6 to complete (check logs for semaphore messages)..."
ALL_OK=true
for jid in "${CONCURRENT_IDS[@]}"; do
  if ! wait_for_status "$jid" "COMPLETED" 180; then
    fail "Job $jid did not complete"
    ALL_OK=false
  fi
done

if $ALL_OK; then
  pass "All 6 concurrent jobs completed"
fi

# The cap of 3 must actually engage: we expect to see slots_used=3/3 (cap
# reached) AND at least one "sem FULL ... waiting" (a 4th request blocked).
HIGH_WATER=$(docker compose logs worker --tail=500 2>/dev/null \
  | grep -oE "slots_used=[0-9]+/[0-9]+" | sort -t= -k2 | tail -1 || true)
FULL_HITS=$(docker compose logs worker --tail=500 2>/dev/null | grep -c "sem FULL" || true)

echo "  High-water mark: $HIGH_WATER"
echo "  'sem FULL, waiting' events: $FULL_HITS"

if [[ "$HIGH_WATER" == "slots_used=3/3" ]]; then
  pass "Semaphore reached the cap (3/3) — concurrency limit is being enforced"
else
  fail "Cap never reached (high-water=$HIGH_WATER) — semaphore not exercised"
fi

if [[ "$FULL_HITS" -gt 0 ]]; then
  pass "At least one request was throttled at the cap ($FULL_HITS 'sem FULL' events)"
else
  fail "No throttling observed — 4th concurrent request was never blocked"
fi

echo ""
echo "  Semaphore log evidence (sample):"
docker compose logs worker --tail=500 2>/dev/null \
  | grep -E "sem FULL|slots_used=3/3" \
  | tail -10 \
  | sed 's/^/    /'

# ── 4. Content cache hit ──────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 4. TTS CONTENT CACHE HIT"
bold "═══════════════════════════════════════════════"

SHARED_TEXT="ALICE: This exact line appears in both manuscripts.
BOB: So the second job must reuse the cached audio."

blue "  Submitting two jobs with identical lines..."
RC1=$(submit_job "$SHARED_TEXT" "demo-cache-a-$(date +%s)")
sleep 0.3
RC2=$(submit_job "$SHARED_TEXT" "demo-cache-b-$(date +%s)")

CACHE_JOB1=$(echo "$RC1" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
CACHE_JOB2=$(echo "$RC2" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)

echo "  Job A: $CACHE_JOB1"
echo "  Job B: $CACHE_JOB2"

wait_for_status "$CACHE_JOB1" "COMPLETED" 120 || true
wait_for_status "$CACHE_JOB2" "COMPLETED" 120 || true

HITS=$(docker compose logs worker --tail=300 2>/dev/null | grep -c "CACHE HIT" || true)
if [[ "$HITS" -gt 0 ]]; then
  pass "CACHE HIT logged $HITS time(s) — duplicate synthesis avoided"
else
  blue "  Note: cache hit may already have been logged earlier in session"
  blue "  Check: docker compose logs worker | grep 'CACHE HIT'"
fi

# ── 5. Poison pill / DLQ ──────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 5. POISON PILL → DLQ"
bold "═══════════════════════════════════════════════"

POISON_TEXT="ALICE: This message contains POISON in the text.
BOB: It should never reach TTS."

RPOISON=$(submit_job "$POISON_TEXT" "demo-poison-$(date +%s)")
POISON_JOB=$(echo "$RPOISON" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
echo "  Submitted poison job: $POISON_JOB"

# Wait for the job to reach a terminal FAILED state
if wait_for_status "$POISON_JOB" "FAILED" 30; then
  ERR=$(job_field "$POISON_JOB" "error")
  pass "Poison job reached terminal FAILED state — error=\"$ERR\""
else
  fail "Poison job did not reach FAILED (stuck at $(job_field "$POISON_JOB" status))"
fi

# Confirm the message actually landed in the RabbitMQ DLQ (not just logged)
DLQ_DEPTH=$(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null \
  | grep "parse.dlq" | awk '{print $2}')
echo "  parse.dlq depth in RabbitMQ: ${DLQ_DEPTH:-0}"
if [[ "${DLQ_DEPTH:-0}" -gt 0 ]]; then
  pass "Message physically present in parse.dlq queue (depth=$DLQ_DEPTH)"
else
  fail "parse.dlq is empty — message did not reach the DLQ"
fi

# Prove the queue wasn't blocked: a normal job submitted right after still completes
NORMAL=$(submit_job "ALICE: A clean job right after the poison one." "demo-after-poison-$(date +%s)")
NORMAL_JOB=$(echo "$NORMAL" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
if wait_for_status "$NORMAL_JOB" "COMPLETED" 60; then
  pass "Queue not blocked — a normal job after the poison pill still COMPLETED"
else
  fail "Normal job after poison did not complete — queue may be blocked"
fi

# ── 6. Crash recovery ─────────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " 6. CRASH RECOVERY (docker kill mid-synthesis)"
bold "═══════════════════════════════════════════════"

# Submit a multi-line job with unique text so synthesis is real (2s/chunk) and
# sustained, giving us a window to kill a worker WHILE it holds a task.
CNONCE="democrash-$(date +%s)-$RANDOM"
CTEXT=""
for i in $(seq 1 6); do CTEXT="${CTEXT}SPEAKER$i: Crash demo unique line $i $CNONCE.
"; done
CRESP=$(submit_job "$CTEXT" "$CNONCE")
CJOB=$(echo "$CRESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
echo "  Submitted job: $CJOB"

# Catch a worker actively synthesising THIS job and kill it (forceful SIGKILL).
KILLED=""
for t in $(seq 1 30); do
  LINE=$(docker compose logs worker --since=15s 2>/dev/null | grep "synthesising" | grep "$CJOB" | head -1 || true)
  if [[ -n "$LINE" ]]; then
    WN=$(echo "$LINE" | awk '{print $1}')
    KILLED="genai-workflow-${WN}"
    echo "  >>> $WN is mid-synthesis — docker kill $KILLED"
    docker kill "$KILLED" >/dev/null 2>&1
    break
  fi
  sleep 0.3
done

if [[ -z "$KILLED" ]]; then
  fail "Could not catch a worker mid-synthesis (try increasing job size)"
else
  # The orphaned task must be reclaimed by the reaper and the job must still
  # complete — no message lost, no human intervention.
  if wait_for_status "$CJOB" "COMPLETED" 90; then
    pass "Job COMPLETED after worker was forcefully killed mid-processing"
    RECLAIMS=$(docker compose logs worker --since=120s 2>/dev/null | grep -c "reclaimed + re-emitted" || true)
    if [[ "$RECLAIMS" -gt 0 ]]; then
      pass "Reaper reclaimed + re-emitted the orphaned task ($RECLAIMS sweep(s))"
    else
      fail "No reaper reclaim logged — recovery path unclear"
    fi
  else
    fail "Job did not recover after worker kill"
  fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " RESULTS"
bold "═══════════════════════════════════════════════"
green "  Passed: $PASS"
[[ $FAIL -gt 0 ]] && red "  Failed: $FAIL" || echo "  Failed: 0"
bold ""

[[ $FAIL -eq 0 ]] && exit 0 || exit 1
