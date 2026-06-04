import uuid
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import db
import store
from config import MINIO_BUCKET

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    store.ensure_bucket()
    log.info("MinIO bucket ready: %s", MINIO_BUCKET)
    yield
    db.get_pool().close()


app = FastAPI(title="GenAI Pipeline Gateway", lifespan=lifespan)


class JobRequest(BaseModel):
    manuscript: str


# ── POST /jobs ────────────────────────────────────────────────────────────────

@app.post("/jobs", status_code=202)
def submit_job(
    body: JobRequest,
    idempotency_key: str = Header(default=None, alias="Idempotency-Key"),
):
    if not body.manuscript.strip():
        raise HTTPException(status_code=422, detail="manuscript must not be empty")

    # Use provided key or generate one; allows client retry without double-submit
    idem_key = idempotency_key or str(uuid.uuid4())

    with db.conn() as cx:
        # Idempotency: return existing job if key already seen
        existing = cx.execute(
            "SELECT id, status FROM jobs WHERE idempotency_key = %s",
            (idem_key,),
        ).fetchone()
        if existing:
            log.info("job=%s duplicate submission (idempotency_key=%s)", existing[0], idem_key)
            return JSONResponse({"job_id": str(existing[0]), "status": existing[1]}, status_code=200)

        job_id = uuid.uuid4()
        manuscript_key = f"manuscripts/{job_id}.txt"

        # 1. Save manuscript to MinIO
        store.put_object(manuscript_key, body.manuscript.encode(), "text/plain")
        log.info("job=%s manuscript uploaded to MinIO key=%s", job_id, manuscript_key)

        # 2. INSERT job + outbox event in ONE transaction
        #    Transactional outbox: if broker publish later fails, the event
        #    row remains unpublished and the relay will retry.
        cx.execute(
            """
            INSERT INTO jobs (id, idempotency_key, status, manuscript_key)
            VALUES (%s, %s, 'PENDING', %s)
            """,
            (str(job_id), idem_key, manuscript_key),
        )
        cx.execute(
            """
            INSERT INTO outbox (routing_key, payload)
            VALUES (%s, %s)
            """,
            (
                "jobs.parse",
                json.dumps({"job_id": str(job_id), "manuscript_key": manuscript_key}),
            ),
        )
        cx.commit()

    log.info("job=%s created status=PENDING", job_id)
    return {"job_id": str(job_id), "status": "PENDING"}


# ── GET /jobs/{job_id} ────────────────────────────────────────────────────────

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with db.conn() as cx:
        job = cx.execute(
            "SELECT id, status, manuscript_key, final_audio_key, error, created_at, updated_at "
            "FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="job not found")

        tasks = cx.execute(
            "SELECT id, stage, chunk_index, status, speaker, attempts, output_key, locked_until "
            "FROM tasks WHERE job_id = %s ORDER BY stage, chunk_index",
            (job_id,),
        ).fetchall()

    return {
        "job_id":           str(job[0]),
        "status":           job[1],
        "manuscript_key":   job[2],
        "final_audio_key":  job[3],
        "error":            job[4],
        "created_at":       job[5].isoformat(),
        "updated_at":       job[6].isoformat(),
        "tasks": [
            {
                "task_id":     str(t[0]),
                "stage":       t[1],
                "chunk_index": t[2],
                "status":      t[3],
                "speaker":     t[4],
                "attempts":    t[5],
                "output_key":  t[6],
                "locked_until": t[7].isoformat() if t[7] else None,
            }
            for t in tasks
        ],
    }


# ── GET /health ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
