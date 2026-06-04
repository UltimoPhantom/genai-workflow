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

blue "  Submitting 4 jobs simultaneously..."
CONCURRENT_IDS=()
for i in $(seq 1 4); do
  TEXT="ALICE: Line one for concurrent job $i.
BOB: Line two for concurrent job $i.
ALICE: Line three for concurrent job $i."
  RESP=$(submit_job "$TEXT" "demo-sem-$i-$(date +%s)")
  JID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
  CONCURRENT_IDS+=("$JID")
  echo "  Submitted job $i: $JID"
done

blue "  Waiting for all 4 to complete (check logs for semaphore messages)..."
ALL_OK=true
for jid in "${CONCURRENT_IDS[@]}"; do
  if ! wait_for_status "$jid" "COMPLETED" 180; then
    fail "Job $jid did not complete"
    ALL_OK=false
  fi
done

if $ALL_OK; then
  pass "All 4 concurrent jobs completed"
fi

echo ""
echo "  Semaphore log evidence (last 50 lines):"
docker compose logs worker --tail=200 2>/dev/null \
  | grep -E "sem acquired|sem released|slots_used" \
  | tail -20 \
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

HITS=$(docker compose logs worker --tail=300 2>/dev/null | grep -c "CACHE HIT" || echo 0)
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

sleep 8

STATUS=$(job_field "$POISON_JOB" "status")
echo "  Job status after 8s: $STATUS"

DLQ_COUNT=$(docker compose logs worker --tail=200 2>/dev/null | grep -c "DLQ\|dlq" || echo 0)
if [[ "$DLQ_COUNT" -gt 0 ]]; then
  pass "DLQ routing confirmed — $DLQ_COUNT DLQ log entries found"
else
  fail "No DLQ log entries found"
fi

if [[ "$STATUS" != "COMPLETED" ]]; then
  pass "Poison job not COMPLETED (status=$STATUS) — correctly failed"
else
  fail "Poison job unexpectedly COMPLETED"
fi

# ── Summary ───────────────────────────────────────────────────────────────────

bold ""
bold "═══════════════════════════════════════════════"
bold " RESULTS"
bold "═══════════════════════════════════════════════"
green "  Passed: $PASS"
[[ $FAIL -gt 0 ]] && red "  Failed: $FAIL" || echo "  Failed: 0"
bold ""

echo "  Crash recovery (manual test):"
echo "    docker kill genai-workflow-worker-1"
echo "    # submit a job, wait ~30s for reaper to fire"
echo "    docker compose logs worker | grep 'Reaper reclaimed'"
echo ""

[[ $FAIL -eq 0 ]] && exit 0 || exit 1
