"""
Database layer for tradetuu.

Handles job queue and persisted scan results.
Uses SQLite locally and PostgreSQL in production.

Environment variables:
  DATABASE_URL  — full connection string (overrides default SQLite)
                  SQLite:    sqlite:///./data/tradetuu.db
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
        sqlite_path = os.path.join(_data_dir(), "tradetuu.db")
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
    # SQLite uses INTEGER PRIMARY KEY (auto-increments implicitly).
    # PostgreSQL requires SERIAL or BIGSERIAL for auto-increment.
    is_pg = engine.dialect.name == "postgresql"
    id_col = "id BIGSERIAL PRIMARY KEY" if is_pg else "id INTEGER PRIMARY KEY"

    with engine.connect() as conn:
        # Users — stores login credentials
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS users (
                {id_col},
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
    # Accounting tables (safe to call on every startup)
    _init_accounting_schema(engine)


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


# ================================================================== #
#  ACCOUNTING — Schema Extension
# ================================================================== #

def _init_accounting_schema(engine: Engine) -> None:
    """
    Add trading_accounts, trades, and withdrawals tables if they don't exist.
    Called from _init_schema() on each startup (safe to re-run).
    """
    is_pg = engine.dialect.name == "postgresql"
    id_col = "id BIGSERIAL PRIMARY KEY" if is_pg else "id INTEGER PRIMARY KEY"

    with engine.connect() as conn:
        # ── Trading Accounts ────────────────────────────────────────────
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS trading_accounts (
                {id_col},
                user_id                  INTEGER NOT NULL,
                name                     TEXT NOT NULL,
                broker                   TEXT,
                account_type             TEXT NOT NULL DEFAULT 'taxable',
                is_active                INTEGER NOT NULL DEFAULT 1,
                starting_balance         REAL DEFAULT 0,
                default_tax_reserve_pct  REAL DEFAULT 30.0,
                notes                    TEXT,
                created_at               TEXT NOT NULL,
                updated_at               TEXT NOT NULL
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ta_user ON trading_accounts(user_id)"
        ))

        # ── Trades ──────────────────────────────────────────────────────
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS trades (
                {id_col},
                user_id        INTEGER NOT NULL,
                account_id     INTEGER NOT NULL,
                trade_date     TEXT NOT NULL,
                ticker         TEXT NOT NULL,
                side           TEXT,
                gross_pnl      REAL NOT NULL DEFAULT 0,
                fees           REAL DEFAULT 0,
                net_pnl        REAL NOT NULL DEFAULT 0,
                quantity       REAL,
                entry_price    REAL,
                exit_price     REAL,
                strategy_tag   TEXT,
                confidence_tag TEXT,
                notes          TEXT,
                screenshot_url TEXT,
                created_at     TEXT NOT NULL,
                updated_at     TEXT NOT NULL,
                is_deleted     INTEGER NOT NULL DEFAULT 0
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_tr_user ON trades(user_id, trade_date)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_tr_acct ON trades(account_id, trade_date)"
        ))

        # ── Withdrawals / Distributions ─────────────────────────────────
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS withdrawals (
                {id_col},
                user_id                INTEGER NOT NULL,
                account_id             INTEGER NOT NULL,
                withdrawal_date        TEXT NOT NULL,
                gross_amount           REAL NOT NULL DEFAULT 0,
                tax_reserve_pct        REAL NOT NULL DEFAULT 0,
                estimated_tax          REAL NOT NULL DEFAULT 0,
                estimated_penalty      REAL NOT NULL DEFAULT 0,
                net_to_owner           REAL NOT NULL DEFAULT 0,
                retained_tax_reserve   REAL NOT NULL DEFAULT 0,
                distribution_type      TEXT,
                penalty_exception      INTEGER DEFAULT 0,
                under_59_5             INTEGER DEFAULT 0,
                qualified_distribution INTEGER DEFAULT 1,
                notes                  TEXT,
                created_at             TEXT NOT NULL,
                updated_at             TEXT NOT NULL
            )
        """))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_wd_user ON withdrawals(user_id, withdrawal_date)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_wd_acct ON withdrawals(account_id)"
        ))

        conn.commit()


# ------------------------------------------------------------------ #
#  Accounting — Trading Accounts CRUD
# ------------------------------------------------------------------ #

def create_trading_account(
    user_id: int, name: str, broker: Optional[str],
    account_type: str, is_active: bool, starting_balance: float,
    default_tax_reserve_pct: float, notes: Optional[str]
) -> int:
    now = _now()
    with get_engine().connect() as conn:
        conn.execute(text("""
            INSERT INTO trading_accounts
                (user_id, name, broker, account_type, is_active,
                 starting_balance, default_tax_reserve_pct, notes,
                 created_at, updated_at)
            VALUES
                (:uid, :name, :broker, :atype, :active,
                 :bal, :reserve, :notes, :ts, :ts)
        """), {
            "uid": user_id, "name": name, "broker": broker or None,
            "atype": account_type, "active": 1 if is_active else 0,
            "bal": float(starting_balance or 0),
            "reserve": float(default_tax_reserve_pct or 30.0),
            "notes": notes or None, "ts": now,
        })
        conn.commit()
        row = conn.execute(text(
            "SELECT id FROM trading_accounts WHERE user_id=:uid ORDER BY id DESC LIMIT 1"
        ), {"uid": user_id}).fetchone()
        return row[0] if row else 0


def get_trading_accounts(user_id: int, include_archived: bool = False) -> List[Dict]:
    with get_engine().connect() as conn:
        q = """
            SELECT ta.*,
                   COALESCE((SELECT COUNT(*) FROM trades t
                              WHERE t.account_id=ta.id AND t.is_deleted=0), 0) AS trade_count,
                   COALESCE((SELECT SUM(t.net_pnl) FROM trades t
                              WHERE t.account_id=ta.id AND t.is_deleted=0), 0) AS total_pnl
            FROM trading_accounts ta
            WHERE ta.user_id=:uid
        """
        if not include_archived:
            q += " AND ta.is_active >= 0"
        q += " ORDER BY ta.created_at ASC"
        rows = conn.execute(text(q), {"uid": user_id}).mappings().fetchall()
    return [dict(r) for r in rows]


def get_trading_account(account_id: int, user_id: int) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM trading_accounts WHERE id=:id AND user_id=:uid"
        ), {"id": account_id, "uid": user_id}).mappings().fetchone()
    return dict(row) if row else None


def update_trading_account(account_id: int, user_id: int, **kwargs) -> None:
    allowed = {"name", "broker", "account_type", "is_active",
               "starting_balance", "default_tax_reserve_pct", "notes"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = _now()
    sets = ", ".join(f"{k}=:{k}" for k in updates)
    updates["id"] = account_id
    updates["uid"] = user_id
    with get_engine().connect() as conn:
        conn.execute(text(
            f"UPDATE trading_accounts SET {sets} WHERE id=:id AND user_id=:uid"
        ), updates)
        conn.commit()


def archive_trading_account(account_id: int, user_id: int) -> None:
    with get_engine().connect() as conn:
        conn.execute(text(
            "UPDATE trading_accounts SET is_active=-1, updated_at=:ts WHERE id=:id AND user_id=:uid"
        ), {"ts": _now(), "id": account_id, "uid": user_id})
        conn.commit()


# ------------------------------------------------------------------ #
#  Accounting — Trades CRUD
# ------------------------------------------------------------------ #

def create_trade(
    user_id: int, account_id: int, trade_date: str, ticker: str,
    side: Optional[str], gross_pnl: float, fees: float, net_pnl: float,
    quantity: Optional[float], entry_price: Optional[float],
    exit_price: Optional[float], strategy_tag: Optional[str],
    confidence_tag: Optional[str], notes: Optional[str],
    screenshot_url: Optional[str]
) -> int:
    now = _now()
    with get_engine().connect() as conn:
        conn.execute(text("""
            INSERT INTO trades
                (user_id, account_id, trade_date, ticker, side,
                 gross_pnl, fees, net_pnl, quantity, entry_price,
                 exit_price, strategy_tag, confidence_tag, notes,
                 screenshot_url, created_at, updated_at, is_deleted)
            VALUES
                (:uid, :acct, :dt, :ticker, :side,
                 :gross, :fees, :net, :qty, :entry,
                 :exit_, :strat, :conf, :notes,
                 :ss, :ts, :ts, 0)
        """), {
            "uid": user_id, "acct": account_id, "dt": trade_date,
            "ticker": ticker.upper().strip(), "side": side or None,
            "gross": float(gross_pnl or 0), "fees": float(fees or 0),
            "net": float(net_pnl), "qty": quantity,
            "entry": entry_price, "exit_": exit_price,
            "strat": strategy_tag or None, "conf": confidence_tag or None,
            "notes": notes or None, "ss": screenshot_url or None, "ts": now,
        })
        conn.commit()
        row = conn.execute(text(
            "SELECT id FROM trades WHERE user_id=:uid ORDER BY id DESC LIMIT 1"
        ), {"uid": user_id}).fetchone()
        return row[0] if row else 0


def get_trades(
    user_id: int, account_id: Optional[int] = None,
    date_from: Optional[str] = None, date_to: Optional[str] = None,
    ticker: Optional[str] = None, limit: int = 200, offset: int = 0
) -> List[Dict]:
    params: Dict[str, Any] = {"uid": user_id, "limit": limit, "offset": offset}
    q = """
        SELECT t.*, ta.name AS account_name, ta.account_type
        FROM trades t
        JOIN trading_accounts ta ON ta.id = t.account_id
        WHERE t.user_id=:uid AND t.is_deleted=0
    """
    if account_id:
        q += " AND t.account_id=:acct"
        params["acct"] = account_id
    if date_from:
        q += " AND t.trade_date >= :df"
        params["df"] = date_from
    if date_to:
        q += " AND t.trade_date <= :dt"
        params["dt"] = date_to
    if ticker:
        q += " AND UPPER(t.ticker) LIKE :tk"
        params["tk"] = f"%{ticker.upper()}%"
    q += " ORDER BY t.trade_date DESC, t.id DESC LIMIT :limit OFFSET :offset"
    with get_engine().connect() as conn:
        rows = conn.execute(text(q), params).mappings().fetchall()
    return [dict(r) for r in rows]


def get_trade(trade_id: int, user_id: int) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM trades WHERE id=:id AND user_id=:uid AND is_deleted=0"
        ), {"id": trade_id, "uid": user_id}).mappings().fetchone()
    return dict(row) if row else None


def update_trade(trade_id: int, user_id: int, **kwargs) -> None:
    allowed = {"account_id", "trade_date", "ticker", "side", "gross_pnl",
               "fees", "net_pnl", "quantity", "entry_price", "exit_price",
               "strategy_tag", "confidence_tag", "notes", "screenshot_url"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    if "ticker" in updates and updates["ticker"]:
        updates["ticker"] = updates["ticker"].upper().strip()
    updates["updated_at"] = _now()
    sets = ", ".join(f"{k}=:{k}" for k in updates)
    updates["id"] = trade_id
    updates["uid"] = user_id
    with get_engine().connect() as conn:
        conn.execute(text(
            f"UPDATE trades SET {sets} WHERE id=:id AND user_id=:uid AND is_deleted=0"
        ), updates)
        conn.commit()


def delete_trade(trade_id: int, user_id: int) -> None:
    with get_engine().connect() as conn:
        conn.execute(text(
            "UPDATE trades SET is_deleted=1, updated_at=:ts WHERE id=:id AND user_id=:uid"
        ), {"ts": _now(), "id": trade_id, "uid": user_id})
        conn.commit()


# ------------------------------------------------------------------ #
#  Accounting — Withdrawals CRUD
# ------------------------------------------------------------------ #

def create_withdrawal(
    user_id: int, account_id: int, withdrawal_date: str,
    gross_amount: float, tax_reserve_pct: float, estimated_tax: float,
    estimated_penalty: float, net_to_owner: float, retained_tax_reserve: float,
    distribution_type: Optional[str], penalty_exception: bool,
    under_59_5: bool, qualified_distribution: bool, notes: Optional[str]
) -> int:
    now = _now()
    with get_engine().connect() as conn:
        conn.execute(text("""
            INSERT INTO withdrawals
                (user_id, account_id, withdrawal_date, gross_amount,
                 tax_reserve_pct, estimated_tax, estimated_penalty,
                 net_to_owner, retained_tax_reserve, distribution_type,
                 penalty_exception, under_59_5, qualified_distribution,
                 notes, created_at, updated_at)
            VALUES
                (:uid, :acct, :dt, :gross,
                 :res_pct, :est_tax, :est_pen,
                 :net, :retained, :dtype,
                 :pex, :u59, :qual,
                 :notes, :ts, :ts)
        """), {
            "uid": user_id, "acct": account_id, "dt": withdrawal_date,
            "gross": float(gross_amount), "res_pct": float(tax_reserve_pct),
            "est_tax": float(estimated_tax), "est_pen": float(estimated_penalty),
            "net": float(net_to_owner), "retained": float(retained_tax_reserve),
            "dtype": distribution_type or None,
            "pex": 1 if penalty_exception else 0,
            "u59": 1 if under_59_5 else 0,
            "qual": 1 if qualified_distribution else 0,
            "notes": notes or None, "ts": now,
        })
        conn.commit()
        row = conn.execute(text(
            "SELECT id FROM withdrawals WHERE user_id=:uid ORDER BY id DESC LIMIT 1"
        ), {"uid": user_id}).fetchone()
        return row[0] if row else 0


def get_withdrawals(
    user_id: int, account_id: Optional[int] = None, limit: int = 100
) -> List[Dict]:
    params: Dict[str, Any] = {"uid": user_id, "limit": limit}
    q = """
        SELECT w.*, ta.name AS account_name, ta.account_type
        FROM withdrawals w
        JOIN trading_accounts ta ON ta.id = w.account_id
        WHERE w.user_id=:uid
    """
    if account_id:
        q += " AND w.account_id=:acct"
        params["acct"] = account_id
    q += " ORDER BY w.withdrawal_date DESC, w.id DESC LIMIT :limit"
    with get_engine().connect() as conn:
        rows = conn.execute(text(q), params).mappings().fetchall()
    return [dict(r) for r in rows]


def get_withdrawal(withdrawal_id: int, user_id: int) -> Optional[Dict]:
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM withdrawals WHERE id=:id AND user_id=:uid"
        ), {"id": withdrawal_id, "uid": user_id}).mappings().fetchone()
    return dict(row) if row else None


def update_withdrawal(withdrawal_id: int, user_id: int, **kwargs) -> None:
    allowed = {"account_id", "withdrawal_date", "gross_amount", "tax_reserve_pct",
               "estimated_tax", "estimated_penalty", "net_to_owner",
               "retained_tax_reserve", "distribution_type", "penalty_exception",
               "under_59_5", "qualified_distribution", "notes"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = _now()
    sets = ", ".join(f"{k}=:{k}" for k in updates)
    updates["id"] = withdrawal_id
    updates["uid"] = user_id
    with get_engine().connect() as conn:
        conn.execute(text(
            f"UPDATE withdrawals SET {sets} WHERE id=:id AND user_id=:uid"
        ), updates)
        conn.commit()


def delete_withdrawal(withdrawal_id: int, user_id: int) -> None:
    with get_engine().connect() as conn:
        conn.execute(text(
            "DELETE FROM withdrawals WHERE id=:id AND user_id=:uid"
        ), {"id": withdrawal_id, "uid": user_id})
        conn.commit()


# ------------------------------------------------------------------ #
#  Accounting — Analytics / Summary
# ------------------------------------------------------------------ #

def get_trade_stats(user_id: int, account_id: Optional[int] = None) -> Dict:
    """
    Returns comprehensive trade statistics for a user (optionally per account).
    """
    from datetime import date, timedelta
    today = date.today().isoformat()
    week_start = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    month_start = date.today().replace(day=1).isoformat()

    params: Dict[str, Any] = {"uid": user_id}
    acct_filter = " AND account_id=:acct" if account_id else ""
    if account_id:
        params["acct"] = account_id

    with get_engine().connect() as conn:
        base = f"FROM trades WHERE user_id=:uid AND is_deleted=0{acct_filter}"

        def scalar(sql, p=None):
            r = conn.execute(text(sql), {**params, **(p or {})}).fetchone()
            return (r[0] or 0) if r else 0

        total_trades  = scalar(f"SELECT COUNT(*) {base}")
        total_pnl     = scalar(f"SELECT COALESCE(SUM(net_pnl),0) {base}")
        today_pnl     = scalar(f"SELECT COALESCE(SUM(net_pnl),0) {base} AND trade_date=:d", {"d": today})
        week_pnl      = scalar(f"SELECT COALESCE(SUM(net_pnl),0) {base} AND trade_date>=:d", {"d": week_start})
        month_pnl     = scalar(f"SELECT COALESCE(SUM(net_pnl),0) {base} AND trade_date>=:d", {"d": month_start})

        # Win/loss counts (trade-level)
        win_count  = scalar(f"SELECT COUNT(*) {base} AND net_pnl > 0")
        loss_count = scalar(f"SELECT COUNT(*) {base} AND net_pnl < 0")

        # Best and worst single trade
        best_trade  = scalar(f"SELECT COALESCE(MAX(net_pnl),0) {base}")
        worst_trade = scalar(f"SELECT COALESCE(MIN(net_pnl),0) {base}")

        # Best and worst day (sum by date)
        day_q = f"""
            SELECT trade_date, SUM(net_pnl) AS dpnl
            FROM trades WHERE user_id=:uid AND is_deleted=0{acct_filter}
            GROUP BY trade_date
        """
        day_rows = conn.execute(text(day_q), params).fetchall()
        day_pnls = [r[1] for r in day_rows if r[1] is not None]
        best_day  = max(day_pnls) if day_pnls else 0
        worst_day = min(day_pnls) if day_pnls else 0
        trading_days = len(day_pnls)
        avg_daily_pnl = round(total_pnl / trading_days, 2) if trading_days else 0
        avg_trade_pnl = round(total_pnl / total_trades, 2) if total_trades else 0

        # Recent 5 trades
        recent_q = f"""
            SELECT t.id, t.trade_date, t.ticker, t.net_pnl, t.side,
                   ta.name AS account_name
            FROM trades t JOIN trading_accounts ta ON ta.id=t.account_id
            WHERE t.user_id=:uid AND t.is_deleted=0{acct_filter}
            ORDER BY t.trade_date DESC, t.id DESC LIMIT 5
        """
        recent = [dict(r) for r in conn.execute(text(recent_q), params).mappings().fetchall()]

    return {
        "total_trades":   int(total_trades),
        "total_pnl":      round(float(total_pnl), 2),
        "today_pnl":      round(float(today_pnl), 2),
        "week_pnl":       round(float(week_pnl), 2),
        "month_pnl":      round(float(month_pnl), 2),
        "win_count":      int(win_count),
        "loss_count":     int(loss_count),
        "best_trade":     round(float(best_trade), 2),
        "worst_trade":    round(float(worst_trade), 2),
        "best_day":       round(float(best_day), 2),
        "worst_day":      round(float(worst_day), 2),
        "trading_days":   int(trading_days),
        "avg_daily_pnl":  avg_daily_pnl,
        "avg_trade_pnl":  avg_trade_pnl,
        "recent_trades":  recent,
    }


def get_withdrawal_totals(user_id: int, account_id: Optional[int] = None) -> Dict:
    params: Dict[str, Any] = {"uid": user_id}
    acct_filter = " AND account_id=:acct" if account_id else ""
    if account_id:
        params["acct"] = account_id

    with get_engine().connect() as conn:
        q = f"""
            SELECT
              COALESCE(SUM(gross_amount),0)         AS total_gross,
              COALESCE(SUM(estimated_tax),0)         AS total_tax,
              COALESCE(SUM(estimated_penalty),0)     AS total_penalty,
              COALESCE(SUM(net_to_owner),0)          AS total_net,
              COALESCE(SUM(retained_tax_reserve),0)  AS total_reserved,
              COUNT(*)                               AS count
            FROM withdrawals
            WHERE user_id=:uid{acct_filter}
        """
        row = conn.execute(text(q), params).fetchone()

    return {
        "total_gross":    round(float(row[0] or 0), 2),
        "total_tax":      round(float(row[1] or 0), 2),
        "total_penalty":  round(float(row[2] or 0), 2),
        "total_net":      round(float(row[3] or 0), 2),
        "total_reserved": round(float(row[4] or 0), 2),
        "count":          int(row[5] or 0),
    }


def get_calendar_data(
    user_id: int, year: int, month: int,
    account_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns a dict keyed by YYYY-MM-DD date string with
    {pnl, trade_count, win, loss} for each day that has trades.
    """
    import calendar
    month_start = f"{year:04d}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    month_end = f"{year:04d}-{month:02d}-{last_day:02d}"

    params: Dict[str, Any] = {"uid": user_id, "ms": month_start, "me": month_end}
    acct_filter = " AND account_id=:acct" if account_id else ""
    if account_id:
        params["acct"] = account_id

    with get_engine().connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
              trade_date,
              SUM(net_pnl)                      AS pnl,
              COUNT(*)                           AS trade_count,
              SUM(CASE WHEN net_pnl>0 THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN net_pnl<0 THEN 1 ELSE 0 END) AS losses
            FROM trades
            WHERE user_id=:uid AND is_deleted=0
              AND trade_date >= :ms AND trade_date <= :me
              {acct_filter}
            GROUP BY trade_date
        """), params).fetchall()

    result: Dict[str, Any] = {}
    for row in rows:
        result[row[0]] = {
            "pnl":         round(float(row[1] or 0), 2),
            "trade_count": int(row[2] or 0),
            "wins":        int(row[3] or 0),
            "losses":      int(row[4] or 0),
        }
    return result


def get_cumulative_pnl_series(
    user_id: int, account_id: Optional[int] = None, limit_days: int = 90
) -> List[Dict]:
    """
    Returns [{date, daily_pnl, cumulative_pnl}] ordered by date ASC,
    for the last `limit_days` calendar days that have trades.
    """
    params: Dict[str, Any] = {"uid": user_id, "limit": limit_days}
    acct_filter = " AND account_id=:acct" if account_id else ""
    if account_id:
        params["acct"] = account_id

    with get_engine().connect() as conn:
        rows = conn.execute(text(f"""
            SELECT trade_date, SUM(net_pnl) AS daily_pnl
            FROM trades
            WHERE user_id=:uid AND is_deleted=0{acct_filter}
            GROUP BY trade_date
            ORDER BY trade_date ASC
            LIMIT :limit
        """), params).fetchall()

    cumulative = 0.0
    result = []
    for row in rows:
        cumulative += float(row[1] or 0)
        result.append({
            "date":           row[0],
            "daily_pnl":      round(float(row[1] or 0), 2),
            "cumulative_pnl": round(cumulative, 2),
        })
    return result


def get_accounting_dashboard_data(user_id: int) -> Dict:
    """Combined data for the dashboard accounting section."""
    stats     = get_trade_stats(user_id)
    wt        = get_withdrawal_totals(user_id)
    accounts  = get_trading_accounts(user_id)
    active_accounts = sum(1 for a in accounts if a.get("is_active", 1) == 1)
    return {
        "trade_stats":      stats,
        "withdrawal_totals": wt,
        "active_accounts":  active_accounts,
        "total_accounts":   len(accounts),
    }


# ------------------------------------------------------------------ #
#  Helpers
# ------------------------------------------------------------------ #

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
