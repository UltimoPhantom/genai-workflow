import boto3
from botocore.client import Config
from config import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, MINIO_BUCKET, MINIO_USE_SSL

def _client():
    return boto3.client(
        "s3",
        endpoint_url=f"{'https' if MINIO_USE_SSL else 'http'}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ensure_bucket():
    s3 = _client()
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)

def put_object(key: str, body: bytes, content_type: str = "application/octet-stream") -> str:
    _client().put_object(Bucket=MINIO_BUCKET, Key=key, Body=body, ContentType=content_type)
    return key

def get_object(key: str) -> bytes:
    resp = _client().get_object(Bucket=MINIO_BUCKET, Key=key)
    return resp["Body"].read()

def object_exists(key: str) -> bool:
    try:
        _client().head_object(Bucket=MINIO_BUCKET, Key=key)
        return True
    except Exception:
        return False
