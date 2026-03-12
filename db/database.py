"""
Database layer for Stock Fitness Agent.

Handles job queue and persisted scan results.
Uses SQLite locally and PostgreSQL in production.

Environment variables:
  DATABASE_URL  — full connection string (overrides default SQLite)
                  SQLite:    sqlite:///./data/stock_fitness.db
                  Postgres:  postgresql://user:pass@host:5432/dbname
  DATA_DIR      — directory for SQLite file and cache (default: ./data)

Schema:
  scan_jobs      — one row per scan job (pending → running → completed/failed/cancelled)
  ticker_results — one row per ticker per job, stores full JSON result
"""

import os
import uuid
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

_engine: Optional[Engine] = None


# ------------------------------------------------------------------ #
#  Engine setup
# ------------------------------------------------------------------ #

def _data_dir() -> str:
    d = os.environ.get("DATA_DIR", os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    ))
    os.makedirs(d, exist_ok=True)
    return d


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        sqlite_path = os.path.join(_data_dir(), "stock_fitness.db")
        url = f"sqlite:///{sqlite_path}"
    # Render.com returns postgres:// — SQLAlchemy requires postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = _db_url()
        safe = url.split("@")[-1] if "@" in url else url
        log.info("Database: %s", safe)
        kwargs: Dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}
        _engine = create_engine(url, **kwargs)
        _init_schema(_engine)
    return _engine


def _init_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        # Users — stores login credentials
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id               INTEGER PRIMARY KEY,
                username         TEXT NOT NULL UNIQUE,
                email            TEXT UNIQUE,
                hashed_password  TEXT NOT NULL,
                created_at       TEXT NOT NULL
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scan_jobs (
                id                 TEXT PRIMARY KEY,
                universe           TEXT NOT NULL,
                status             TEXT NOT NULL DEFAULT 'pending',
                created_at         TEXT NOT NULL,
                started_at         TEXT,
                completed_at       TEXT,
                total_tickers      INTEGER DEFAULT 0,
                processed_tickers  INTEGER DEFAULT 0,
                current_ticker     TEXT DEFAULT '',
                error_message      TEXT,
                user_id            INTEGER
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ticker_results (
                job_id           TEXT NOT NULL,
                ticker           TEXT NOT NULL,
                company_name     TEXT,
                sector           TEXT,
                classification   TEXT,
                final_score      REAL,
                hard_reject_flag INTEGER DEFAULT 0,
                result_json      TEXT NOT NULL,
                evaluated_at     TEXT NOT NULL,
                PRIMARY KEY (job_id, ticker)
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_tr_job ON ticker_results(job_id)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_tr_score ON ticker_results(job_id, final_score)"
        ))
        conn.commit()
    log.info("Database schema ready")


# ------------------------------------------------------------------ #
#  Job CRUD
# ------------------------------------------------------------------ #

def create_job(universe: str) -> str:
    """Create a new pending scan job. Returns the job_id."""
    job_id = str(uuid.uuid4())
    with get_engine().connect() as conn:
        conn.execute(text("""
            INSERT INTO scan_jobs (id, universe, status, created_at)
            VALUES (:id, :universe, 'pending', :ts)
        """), {"id": job_id, "universe": universe, "ts": _now()})
        conn.commit()
    log.info("Created job %s universe=%s", job_id, universe)
    return job_id


def get_job(job_id: str) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM scan_jobs WHERE id = :id"),
            {"id": job_id}
        ).mappings().fetchone()
    return dict(row) if row else None


def list_recent_jobs(limit: int = 20) -> List[Dict]:
    with get_engine().connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM scan_jobs ORDER BY created_at DESC LIMIT :limit"),
            {"limit": limit}
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def get_pending_jobs(limit: int = 1) -> List[Dict]:
    with get_engine().connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM scan_jobs WHERE status='pending' ORDER BY created_at ASC LIMIT :limit"),
            {"limit": limit}
        ).mappings().fetchall()
    return [dict(r) for r in rows]


def claim_job(job_id: str) -> bool:
    """Atomically claim a pending job. Returns True if claimed."""
    with get_engine().connect() as conn:
        result = conn.execute(text("""
            UPDATE scan_jobs SET status='running', started_at=:ts
            WHERE id=:id AND status='pending'
        """), {"id": job_id, "ts": _now()})
        conn.commit()
        return result.rowcount > 0


def update_job_progress(job_id: str, processed: int, current_ticker: str) -> None:
    with get_engine().connect() as conn:
        conn.execute(text("""
            UPDATE scan_jobs
            SET processed_tickers=:proc, current_ticker=:ticker
            WHERE id=:id
        """), {"id": job_id, "proc": processed, "ticker": current_ticker})
        conn.commit()


def update_job_started(job_id: str, total: int) -> None:
    with get_engine().connect() as conn:
        conn.execute(text("""
            UPDATE scan_jobs SET started_at=:ts, total_tickers=:total, status='running'
            WHERE id=:id
        """), {"id": job_id, "ts": _now(), "total": total})
        conn.commit()


def update_job_completed(job_id: str) -> None:
    with get_engine().connect() as conn:
        conn.execute(text("""
            UPDATE scan_jobs
            SET status='completed', completed_at=:ts, current_ticker=''
            WHERE id=:id
        """), {"id": job_id, "ts": _now()})
        conn.commit()


def update_job_failed(job_id: str, error: str) -> None:
    with get_engine().connect() as conn:
        conn.execute(text("""
            UPDATE scan_jobs
            SET status='failed', completed_at=:ts, error_message=:err
            WHERE id=:id
        """), {"id": job_id, "ts": _now(), "err": str(error)[:500]})
        conn.commit()


def cancel_job(job_id: str) -> None:
    with get_engine().connect() as conn:
        conn.execute(text("""
            UPDATE scan_jobs SET status='cancelled', completed_at=:ts
            WHERE id=:id AND status IN ('pending','running')
        """), {"id": job_id, "ts": _now()})
        conn.commit()


def reset_interrupted_jobs() -> int:
    """
    On startup, reset any 'running' jobs back to 'pending'.
    These are jobs interrupted by a process restart.
    Returns number of jobs reset.
    """
    with get_engine().connect() as conn:
        result = conn.execute(text("""
            UPDATE scan_jobs
            SET status='pending', started_at=NULL, processed_tickers=0, current_ticker=''
            WHERE status='running'
        """))
        conn.commit()
        n = result.rowcount
    if n:
        log.info("Reset %d interrupted job(s) back to pending", n)
    return n


# ------------------------------------------------------------------ #
#  Result CRUD
# ------------------------------------------------------------------ #

def save_ticker_result(job_id: str, result_dict: Dict) -> None:
    ticker = result_dict.get("ticker", "UNKNOWN")
    with get_engine().connect() as conn:
        # DELETE + INSERT works across SQLite and Postgres without dialect-specific UPSERT
        conn.execute(
            text("DELETE FROM ticker_results WHERE job_id=:j AND ticker=:t"),
            {"j": job_id, "t": ticker}
        )
        conn.execute(text("""
            INSERT INTO ticker_results
                (job_id, ticker, company_name, sector, classification,
                 final_score, hard_reject_flag, result_json, evaluated_at)
            VALUES
                (:job_id, :ticker, :company, :sector, :cls,
                 :score, :reject, :json, :ts)
        """), {
            "job_id":  job_id,
            "ticker":  ticker,
            "company": result_dict.get("company_name"),
            "sector":  result_dict.get("sector"),
            "cls":     result_dict.get("classification"),
            "score":   result_dict.get("final_score"),
            "reject":  1 if result_dict.get("hard_reject_flag") else 0,
            "json":    json.dumps(result_dict, default=str),
            "ts":      _now(),
        })
        conn.commit()


def get_job_results(job_id: str) -> List[Dict]:
    """Return all ticker results for a job, sorted by score descending."""
    with get_engine().connect() as conn:
        rows = conn.execute(
            text("""
                SELECT result_json FROM ticker_results
                WHERE job_id=:id
                ORDER BY
                  hard_reject_flag ASC,
                  final_score DESC
            """),
            {"id": job_id}
        ).fetchall()
    out = []
    for row in rows:
        try:
            out.append(json.loads(row[0]))
        except Exception:
            pass
    return out


def get_job_result_count(job_id: str) -> int:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT COUNT(*) FROM ticker_results WHERE job_id=:id"),
            {"id": job_id}
        ).fetchone()
    return row[0] if row else 0


# ------------------------------------------------------------------ #
#  User CRUD
# ------------------------------------------------------------------ #

def create_user(username: str, hashed_password: str, email: Optional[str] = None) -> int:
    """Create a new user. Returns the new user's id."""
    with get_engine().connect() as conn:
        result = conn.execute(text("""
            INSERT INTO users (username, email, hashed_password, created_at)
            VALUES (:username, :email, :pw, :ts)
        """), {"username": username, "email": email, "pw": hashed_password, "ts": _now()})
        conn.commit()
        # Fetch the inserted id
        row = conn.execute(
            text("SELECT id FROM users WHERE username = :u"), {"u": username}
        ).fetchone()
        return row[0] if row else 0


def get_user_by_username(username: str) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM users WHERE username = :u"), {"u": username}
        ).mappings().fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM users WHERE id = :id"), {"id": user_id}
        ).mappings().fetchone()
    return dict(row) if row else None


def count_users() -> int:
    with get_engine().connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
    return row[0] if row else 0


# ------------------------------------------------------------------ #
#  Dashboard queries
# ------------------------------------------------------------------ #

def get_dashboard_stats() -> Dict:
    """
    Returns aggregate stats for the dashboard page:
      total_scans, latest_job, classification_counts, top_results
    """
    with get_engine().connect() as conn:
        # Total completed scans
        row = conn.execute(
            text("SELECT COUNT(*) FROM scan_jobs WHERE status='completed'")
        ).fetchone()
        total_scans = row[0] if row else 0

        # Latest completed job
        row = conn.execute(text("""
            SELECT id, universe, completed_at, total_tickers, processed_tickers
            FROM scan_jobs
            WHERE status='completed'
            ORDER BY completed_at DESC
            LIMIT 1
        """)).mappings().fetchone()
        latest_job = dict(row) if row else None

        classification_counts = {"IDEAL_FIT": 0, "TRADABLE": 0, "WATCHLIST_ONLY": 0, "AVOID": 0}
        top_results = []

        if latest_job:
            # Classification counts for latest job
            rows = conn.execute(text("""
                SELECT classification, COUNT(*) as cnt
                FROM ticker_results
                WHERE job_id = :jid AND hard_reject_flag = 0
                GROUP BY classification
            """), {"jid": latest_job["id"]}).fetchall()
            for r in rows:
                if r[0] in classification_counts:
                    classification_counts[r[0]] = r[1]

            # Top results (Ideal + Tradable) from latest job
            rows = conn.execute(text("""
                SELECT ticker, company_name, sector, classification, final_score
                FROM ticker_results
                WHERE job_id = :jid
                  AND classification IN ('IDEAL_FIT', 'TRADABLE')
                  AND hard_reject_flag = 0
                ORDER BY final_score DESC
                LIMIT 8
            """), {"jid": latest_job["id"]}).mappings().fetchall()
            top_results = [dict(r) for r in rows]

    return {
        "total_scans": total_scans,
        "latest_job": latest_job,
        "classification_counts": classification_counts,
        "top_results": top_results,
    }


# ------------------------------------------------------------------ #
#  Cleanup
# ------------------------------------------------------------------ #

def purge_old_jobs(keep_days: int = 30) -> int:
    """
    Delete completed/failed/cancelled scan jobs and their results older than
    keep_days days. Keeps the most recent scans for history.
    Returns the number of jobs deleted.

    Run this periodically (e.g. once per day from the worker) to prevent the
    database from growing unboundedly. With daily S&P 500 scans at ~1.2 MB each,
    keeping 30 days uses ~36 MB — well within the basic-256mb Postgres limit.
    """
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()

    with get_engine().connect() as conn:
        # Find old job IDs first
        rows = conn.execute(
            text("""
                SELECT id FROM scan_jobs
                WHERE status IN ('completed', 'failed', 'cancelled')
                  AND created_at < :cutoff
            """),
            {"cutoff": cutoff}
        ).fetchall()

        if not rows:
            return 0

        old_ids = [r[0] for r in rows]
        placeholders = ",".join(f":id{i}" for i in range(len(old_ids)))
        params = {f"id{i}": oid for i, oid in enumerate(old_ids)}

        # Delete results first (FK constraint)
        conn.execute(
            text(f"DELETE FROM ticker_results WHERE job_id IN ({placeholders})"),
            params
        )
        conn.execute(
            text(f"DELETE FROM scan_jobs WHERE id IN ({placeholders})"),
            params
        )
        conn.commit()

    log.info("Purged %d old scan job(s) older than %d days", len(old_ids), keep_days)
    return len(old_ids)


# ------------------------------------------------------------------ #
#  Helpers
# ------------------------------------------------------------------ #

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
