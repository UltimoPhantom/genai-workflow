import os

DATABASE_URL   = os.environ["DATABASE_URL"]
REDIS_URL      = os.environ["REDIS_URL"]
RABBITMQ_URL   = os.environ["RABBITMQ_URL"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS   = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET   = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET   = os.environ.get("MINIO_BUCKET", "genai-pipeline")
MINIO_USE_SSL  = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
