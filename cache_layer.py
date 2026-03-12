"""
Cache layer — SQLite-backed key/value cache with per-entry TTL.
Each cached value is stored as JSON with a timestamp.
"""
import sqlite3
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

log = logging.getLogger(__name__)


class CacheLayer:
    """
    Simple SQLite cache. Thread-safe for single-process use.
    Schema: (key TEXT PK, value TEXT, stored_at TEXT)
    """

    def __init__(self, cache_dir: str):
        os.makedirs(cache_dir, exist_ok=True)
        self._db_path = os.path.join(cache_dir, "stock_fitness_cache.db")
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    key       TEXT PRIMARY KEY,
                    value     TEXT NOT NULL,
                    stored_at TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=10)

    def get(self, key: str, max_age_hours: float = 24) -> Optional[Any]:
        """
        Return cached value if it exists and is within max_age_hours.
        Returns None on miss or stale.
        """
        cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value, stored_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        value_json, stored_at = row
        if stored_at < cutoff:
            log.debug("Cache stale for key=%s (stored_at=%s)", key, stored_at)
            return None
        try:
            return json.loads(value_json)
        except json.JSONDecodeError:
            log.warning("Cache corrupt for key=%s", key)
            return None

    def set(self, key: str, value: Any) -> None:
        """Persist value to cache with current timestamp."""
        try:
            value_json = json.dumps(value, default=str)
        except (TypeError, ValueError) as exc:
            log.warning("Cannot cache key=%s: %s", key, exc)
            return
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cache (key, value, stored_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, stored_at=excluded.stored_at
                """,
                (key, value_json, now),
            )

    def invalidate(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))

    def invalidate_prefix(self, prefix: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM cache WHERE key LIKE ?", (prefix + "%",)
            )
            return cur.rowcount

    def clear_all(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cache")

    def stats(self) -> dict:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
        return {"total_entries": total, "db_path": self._db_path}


# Module-level singleton
_cache: Optional[CacheLayer] = None


def get_cache(cache_dir: Optional[str] = None) -> CacheLayer:
    global _cache
    if _cache is None:
        from config import get_config
        cfg = get_config()
        _cache = CacheLayer(cache_dir or cfg.cache_dir)
    return _cache
