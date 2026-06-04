import psycopg
from psycopg_pool import ConnectionPool
from config import DATABASE_URL

_pool: ConnectionPool | None = None

def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, open=True)
    return _pool

def conn():
    return get_pool().connection()
