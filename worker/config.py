import os

DATABASE_URL      = os.environ["DATABASE_URL"]
REDIS_URL         = os.environ["REDIS_URL"]
RABBITMQ_URL      = os.environ["RABBITMQ_URL"]
MINIO_ENDPOINT    = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS      = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET      = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET      = os.environ.get("MINIO_BUCKET", "genai-pipeline")
MINIO_USE_SSL     = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"

TTS_SERVICE_URL   = os.environ.get("TTS_SERVICE_URL", "http://tts-service:5050")
PARSE_FAIL_RATE   = float(os.environ.get("PARSE_FAIL_RATE", "0.15"))
TTS_MAX_CONCURRENCY = int(os.environ.get("TTS_MAX_CONCURRENCY", "3"))
TTS_SIMULATE_SECS = float(os.environ.get("TTS_SIMULATE_SECONDS", "2"))
LEASE_SECONDS     = int(os.environ.get("LEASE_SECONDS", "60"))
REAPER_INTERVAL_SECS = int(os.environ.get("REAPER_INTERVAL_SECONDS", "10"))
MAX_RETRIES       = int(os.environ.get("MAX_RETRIES", "3"))
# "1000,4000,16000" → [1.0, 4.0, 16.0]
BACKOFF_SECS      = [int(x) / 1000 for x in os.environ.get("BACKOFF_MS", "1000,4000,16000").split(",")]
WORKER_PREFETCH   = int(os.environ.get("WORKER_PREFETCH", "4"))
POISON_TOKEN      = os.environ.get("POISON_TOKEN", "POISON")
