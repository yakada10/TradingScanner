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
                error_message      TEXT
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
#  Helpers
# ------------------------------------------------------------------ #

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
