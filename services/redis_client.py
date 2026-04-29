import os
from functools import lru_cache

import redis

from logger_config import get_logger

logger = get_logger("Redis")


def _redis_url() -> str:
    url = (os.getenv("REDIS_URL") or "").strip()
    if url:
        return url
    host = (os.getenv("REDIS_HOST") or "").strip()
    if not host:
        raise RuntimeError("Missing Redis configuration: set REDIS_URL or REDIS_HOST.")
    port = (os.getenv("REDIS_PORT") or "6379").strip()
    db = (os.getenv("REDIS_DB") or "0").strip()
    return f"redis://{host}:{port}/{db}"


def _safe_int(env_key: str, default: int) -> int:
    raw = (os.getenv(env_key) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _safe_float(env_key: str, default: float) -> float:
    raw = (os.getenv(env_key) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except Exception:
        return default
    return value if value >= 0 else default


@lru_cache(maxsize=1)
def get_redis_pool() -> redis.ConnectionPool:
    url = _redis_url()
    max_connections = _safe_int("REDIS_MAX_CONNECTIONS", 200)
    socket_timeout = _safe_float("REDIS_SOCKET_TIMEOUT_SEC", 2.0)
    connect_timeout = _safe_float("REDIS_CONNECT_TIMEOUT_SEC", 2.0)
    health_interval = _safe_int("REDIS_HEALTH_CHECK_INTERVAL_SEC", 30)
    try:
        return redis.ConnectionPool.from_url(
            url,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=connect_timeout,
            health_check_interval=health_interval,
            retry_on_timeout=True,
            decode_responses=False,
        )
    except Exception as exc:
        logger.error("Failed to build Redis pool from {}: {}", url, exc)
        raise


@lru_cache(maxsize=1)
def get_redis_client() -> redis.Redis:
    return redis.Redis(connection_pool=get_redis_pool())
