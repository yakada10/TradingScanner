"""
tradetuu — Web API + UI Server

Routes:
  GET  /                  → redirect to /dashboard (or /login)
  GET  /login             → login page
  POST /login             → authenticate, set cookie, redirect /dashboard
  GET  /signup            → signup page
  POST /signup            → create account, redirect /login
  GET  /logout            → clear cookie, redirect /login
  GET  /dashboard         → protected dashboard page
  GET  /scanner           → protected scanner page

  GET  /health            → liveness probe (public)
  POST /api/scan          → create scan job (auth required)
  POST /api/scan/{id}/cancel
  GET  /api/scan/{id}/progress
  GET  /api/scan/{id}/results
  GET  /api/scans         → list recent jobs
  POST /api/evaluate      → evaluate single ticker

Worker mode (RUN_WORKER_INLINE env var):
  true  → inline background thread (local dev / free Render tier)
  false → external worker process (separate Render worker service)
"""
import sys
import os
import logging
import re
from contextlib import asynccontextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import io
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional

import db.database as db
from web.auth import (
    hash_password, verify_password, create_access_token,
    get_current_user, set_auth_cookie, clear_auth_cookie,
    is_production, _RedirectToLogin,
)
from engine.pipeline import ScoringPipeline
from utils.serialize import result_to_dict

# ── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
for noisy in ["yfinance", "urllib3", "requests", "peewee"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ── Templates + Static ────────────────────────────────────────────
_web_dir = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(_web_dir, "templates"))

# ── Lazy pipeline ─────────────────────────────────────────────────
_pipeline: ScoringPipeline | None = None


def _get_pipeline() -> ScoringPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = ScoringPipeline()
    return _pipeline


# ── Auth helpers ──────────────────────────────────────────────────

def _require_user(request: Request) -> dict:
    user = get_current_user(request)
    if not user:
        raise _RedirectToLogin()
    return user


def _require_user_api(request: Request) -> dict:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


_USERNAME_RE = re.compile(r'^[a-zA-Z0-9_]{3,32}$')


# ─────────────────────────────────────────────────────────────────
#  Lifespan
# ─────────────────────────────────────────────────────────────────

def _ensure_admin_user() -> None:
    """
    On startup, create the admin account if no users exist yet.
    Credentials come from env vars ADMIN_USERNAME / ADMIN_PASSWORD.
    Defaults: admin / changeme  (change immediately via env vars in production).
    """
    if db.count_users() > 0:
        return  # users already exist, skip
    username = os.environ.get("ADMIN_USERNAME", "admin")
    password = os.environ.get("ADMIN_PASSWORD", "tradeovo10$")
    db.create_user(username, hash_password(password), email=None)
    log.info("Default admin account created — username: '%s' (set ADMIN_PASSWORD env var to secure it)", username)


@asynccontextmanager
async def lifespan(app_instance):
    db.get_engine()
    n = db.reset_interrupted_jobs()
    if n:
        log.info("Reset %d interrupted job(s) to pending on startup", n)

    # Auto-create admin if DB is fresh
    _ensure_admin_user()

    run_inline = os.environ.get("RUN_WORKER_INLINE", "true").lower() != "false"
    if run_inline:
        from worker.scanner_worker import start_worker_thread
        start_worker_thread()
        log.info("Inline worker thread started")
    else:
        log.info("Inline worker disabled — expecting separate worker process")

    yield


# ─────────────────────────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="tradetuu",
    version="3.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=os.path.join(_web_dir, "static")), name="static")


@app.exception_handler(_RedirectToLogin)
async def redirect_to_login(request: Request, exc: _RedirectToLogin):
    return RedirectResponse(url="/login", status_code=303)


# ─────────────────────────────────────────────────────────────────
#  Public routes
# ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "tradetuu"}


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user = get_current_user(request)
    target = "/dashboard" if user else "/login"
    return RedirectResponse(url=target, status_code=303)


# ── Login ─────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if get_current_user(request):
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login", response_class=HTMLResponse)
async def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    user_row = db.get_user_by_username(username.strip())
    if not user_row or not verify_password(password, user_row["hashed_password"]):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password",
            "username": username,
        }, status_code=401)

    token = create_access_token(user_row["id"], user_row["username"])
    response = RedirectResponse(url="/dashboard", status_code=303)
    set_auth_cookie(response, token, is_prod=is_production())
    return response


# ── Signup ────────────────────────────────────────────────────────
# Controlled by ALLOW_SIGNUP env var (default: false).
# Set ALLOW_SIGNUP=true to re-enable public registration.

def _signup_enabled() -> bool:
    return os.environ.get("ALLOW_SIGNUP", "false").lower() == "true"


def _signup_closed_response(request: Request):
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": "Account registration is currently closed.",
    }, status_code=403)


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    if not _signup_enabled():
        return _signup_closed_response(request)
    if get_current_user(request):
        return RedirectResponse(url="/dashboard", status_code=303)
    return templates.TemplateResponse("signup.html", {"request": request})


@app.post("/signup", response_class=HTMLResponse)
async def signup_post(
    request: Request,
    username: str = Form(...),
    email: str = Form(""),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    if not _signup_enabled():
        return _signup_closed_response(request)

    username = username.strip()
    email = email.strip() or None

    def _err(msg):
        return templates.TemplateResponse("signup.html", {
            "request": request,
            "error": msg,
            "username": username,
            "email": email or "",
        }, status_code=422)

    if not _USERNAME_RE.match(username):
        return _err("Username must be 3–32 characters (letters, numbers, underscores only)")
    if len(password) < 8:
        return _err("Password must be at least 8 characters")
    if password != confirm_password:
        return _err("Passwords do not match")
    if db.get_user_by_username(username):
        return _err("Username already taken")

    db.create_user(username, hash_password(password), email)
    log.info("New user registered: %s", username)
    return templates.TemplateResponse("login.html", {
        "request": request,
        "success": "Account created! Sign in below.",
    })


# ── Logout ────────────────────────────────────────────────────────

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    clear_auth_cookie(response)
    return response


# ─────────────────────────────────────────────────────────────────
#  Protected page routes
# ─────────────────────────────────────────────────────────────────

_EMPTY_ACCT = {
    "trade_stats": {
        "total_trades": 0, "total_pnl": 0.0, "today_pnl": 0.0,
        "week_pnl": 0.0, "month_pnl": 0.0, "win_count": 0,
        "loss_count": 0, "best_trade": 0.0, "worst_trade": 0.0,
        "best_day": 0.0, "worst_day": 0.0, "trading_days": 0,
        "avg_daily_pnl": 0.0, "avg_trade_pnl": 0.0, "recent_trades": [],
    },
    "withdrawal_totals": {
        "total_gross": 0.0, "total_tax": 0.0, "total_penalty": 0.0,
        "total_net": 0.0, "total_reserved": 0.0, "count": 0,
    },
    "active_accounts": 0,
    "total_accounts": 0,
}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    current_user = _require_user(request)
    stats = db.get_dashboard_stats()
    recent_jobs = db.list_recent_jobs(limit=8)
    try:
        acct_data = db.get_accounting_dashboard_data(current_user["id"])
    except Exception as exc:
        log.error("Accounting dashboard data error: %s", exc, exc_info=True)
        acct_data = _EMPTY_ACCT
    return templates.TemplateResponse("dashboard.html", {
        "request":      request,
        "current_user": current_user,
        "active_page":  "dashboard",
        "stats":        stats,
        "recent_jobs":  recent_jobs,
        "acct":         acct_data,
        "account_types": ACCOUNT_TYPES,
    })


@app.get("/scanner", response_class=HTMLResponse)
async def scanner_page(request: Request):
    current_user = _require_user(request)
    return templates.TemplateResponse("scanner.html", {
        "request": request,
        "current_user": current_user,
        "active_page": "scanner",
    })


# ─────────────────────────────────────────────────────────────────
#  API routes (auth required)
# ─────────────────────────────────────────────────────────────────

_VALID_UNIVERSES = {"sp500", "sp500_extended", "nasdaq_full", "nyse_nasdaq_full"}


class ScanRequest(BaseModel):
    universe: str = "sp500"


class EvaluateRequest(BaseModel):
    ticker: str


@app.post("/api/scan")
async def start_scan(request: Request, body: ScanRequest):
    current_user = _require_user_api(request)
    if body.universe not in _VALID_UNIVERSES:
        raise HTTPException(status_code=400, detail=f"Unknown universe: {body.universe}")
    job_id = db.create_job(body.universe)
    log.info("Scan job %s created by user %s", job_id, current_user["username"])
    return {"job_id": job_id, "universe": body.universe, "status": "pending"}


@app.post("/api/scan/{job_id}/cancel")
async def cancel_scan(request: Request, job_id: str):
    _require_user_api(request)
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.cancel_job(job_id)
    return {"job_id": job_id, "status": "cancelled"}


@app.get("/api/scan/{job_id}/progress")
async def scan_progress(request: Request, job_id: str):
    _require_user_api(request)
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    from datetime import datetime, timezone as tz
    total     = job.get("total_tickers") or 0
    processed = job.get("processed_tickers") or 0
    pct       = round(processed / total * 100, 1) if total > 0 else 0.0

    # Estimated time remaining
    eta_seconds = None
    if job.get("started_at") and processed > 0 and total > processed:
        try:
            started = datetime.fromisoformat(job["started_at"])
            if started.tzinfo is None:
                started = started.replace(tzinfo=tz.utc)
            elapsed = (datetime.now(tz.utc) - started).total_seconds()
            rate = elapsed / processed          # seconds per ticker
            eta_seconds = int(rate * (total - processed))
        except Exception:
            pass

    return {
        "job_id":         job_id,
        "universe":       job.get("universe"),
        "status":         job.get("status"),
        "percent":        pct,
        "progress":       processed,
        "total":          total,
        "current_ticker": job.get("current_ticker", ""),
        "results_so_far": db.get_job_result_count(job_id),
        "started_at":     job.get("started_at"),
        "completed_at":   job.get("completed_at"),
        "error_message":  job.get("error_message"),
        "eta_seconds":    eta_seconds,
    }


@app.get("/api/scan/{job_id}/results")
async def scan_results(request: Request, job_id: str):
    _require_user_api(request)
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(db.get_job_results(job_id))


@app.get("/api/scans")
async def list_scans(request: Request, limit: int = 20):
    _require_user_api(request)
    return db.list_recent_jobs(limit=limit)


@app.post("/api/evaluate")
async def evaluate_ticker(request: Request, body: EvaluateRequest):
    _require_user_api(request)
    ticker = body.ticker.upper().strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker required")
    try:
        result = _get_pipeline().evaluate(ticker)
        return JSONResponse(result_to_dict(result))
    except Exception as exc:
        log.error("Evaluate error for %s: %s", ticker, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ═════════════════════════════════════════════════════════════════
#  ACCOUNTANT — helpers and constants
# ═════════════════════════════════════════════════════════════════

ACCOUNT_TYPES = {
    "taxable":         "Taxable Brokerage",
    "roth_ira":        "Roth IRA",
    "traditional_ira": "Traditional IRA",
    "rollover_ira":    "Rollover IRA",
    "futures":         "Futures Account",
    "prop":            "Prop / Evaluation",
    "paper":           "Paper / Demo",
    "other":           "Other / Custom",
}

DISTRIBUTION_TYPES = [
    ("profit_distribution",  "Profit Distribution"),
    ("capital_return",       "Return of Capital"),
    ("owner_draw",           "Owner Draw / Living Expenses"),
    ("quarterly_tax",        "Quarterly Tax Estimate Payment"),
    ("tax_reserve_transfer", "Tax Reserve Transfer"),
    ("ira_qualified",        "Qualified IRA Distribution"),
    ("ira_non_qualified",    "Non-Qualified IRA Distribution"),
    ("roth_qualified",       "Roth Qualified Distribution"),
    ("roth_non_qualified",   "Roth Non-Qualified Distribution"),
    ("other",                "Other"),
]

STRATEGY_TAGS = [
    "Momentum", "Reversal", "Breakout", "Scalp", "Gap & Go",
    "VWAP", "Earnings Play", "Support / Resistance", "News Catalyst",
    "Options", "Futures", "Swing", "Other",
]

CONFIDENCE_TAGS = [
    "High Conviction", "Medium Conviction", "Low Conviction",
    "Impulsive / FOMO", "Revenge Trade", "Well-Planned",
]


def _estimate_withdrawal(
    account_type: str,
    gross_amount: float,
    tax_reserve_pct: float,
    under_59_5: bool = False,
    penalty_exception: bool = False,
    qualified_distribution: bool = True,
) -> dict:
    """
    Estimate tax/penalty for a withdrawal based on account type.
    All values are ESTIMATES for planning purposes only — not tax advice.
    """
    gross = max(float(gross_amount or 0), 0)
    res   = max(float(tax_reserve_pct or 0), 0)

    if account_type in ("taxable", "futures"):
        est_tax     = round(gross * res / 100, 2)
        est_penalty = 0.0
        retained    = est_tax
        net         = round(gross - est_tax, 2)

    elif account_type == "roth_ira":
        if qualified_distribution:
            est_tax = est_penalty = retained = 0.0
            net = gross
        else:
            est_tax     = round(gross * res / 100, 2)
            est_penalty = round(gross * 0.10, 2) if (under_59_5 and not penalty_exception) else 0.0
            retained    = est_tax
            net         = round(gross - est_tax - est_penalty, 2)

    elif account_type in ("traditional_ira", "rollover_ira"):
        est_tax     = round(gross * res / 100, 2)
        est_penalty = round(gross * 0.10, 2) if (under_59_5 and not penalty_exception) else 0.0
        retained    = est_tax
        net         = round(gross - est_tax - est_penalty, 2)

    else:  # prop, paper, other, unknown
        est_tax     = round(gross * res / 100, 2)
        est_penalty = 0.0
        retained    = est_tax
        net         = round(gross - est_tax, 2)

    return {
        "estimated_tax":        est_tax,
        "estimated_penalty":    est_penalty,
        "net_to_owner":         max(net, 0.0),
        "retained_tax_reserve": retained,
    }


# ─────────────────────────────────────────────────────────────────
#  Accountant page routes
# ─────────────────────────────────────────────────────────────────

@app.get("/accountant", response_class=HTMLResponse)
async def accountant_root(request: Request):
    return RedirectResponse(url="/accountant/trades", status_code=303)


@app.get("/accountant/trades", response_class=HTMLResponse)
async def accountant_trades_page(request: Request):
    current_user = _require_user(request)
    accounts = db.get_trading_accounts(current_user["id"])
    stats    = db.get_trade_stats(current_user["id"])
    return templates.TemplateResponse("accountant_trades.html", {
        "request":      request,
        "current_user": current_user,
        "active_page":  "accountant",
        "active_sub":   "trades",
        "accounts":     accounts,
        "stats":        stats,
        "account_types": ACCOUNT_TYPES,
        "strategy_tags": STRATEGY_TAGS,
        "confidence_tags": CONFIDENCE_TAGS,
    })


@app.get("/accountant/calendar", response_class=HTMLResponse)
async def accountant_calendar_page(request: Request):
    current_user = _require_user(request)
    accounts = db.get_trading_accounts(current_user["id"])
    return templates.TemplateResponse("accountant_calendar.html", {
        "request":      request,
        "current_user": current_user,
        "active_page":  "accountant",
        "active_sub":   "calendar",
        "accounts":     accounts,
    })


@app.get("/accountant/accounts", response_class=HTMLResponse)
async def accountant_accounts_page(request: Request):
    current_user = _require_user(request)
    accounts = db.get_trading_accounts(current_user["id"], include_archived=True)
    return templates.TemplateResponse("accountant_accounts.html", {
        "request":       request,
        "current_user":  current_user,
        "active_page":   "accountant",
        "active_sub":    "accounts",
        "accounts":      accounts,
        "account_types": ACCOUNT_TYPES,
    })


@app.get("/accountant/accounts/{account_id}", response_class=HTMLResponse)
async def accountant_account_detail_page(request: Request, account_id: int):
    current_user = _require_user(request)
    account = db.get_trading_account(account_id, current_user["id"])
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    stats   = db.get_trade_stats(current_user["id"], account_id=account_id)
    wt      = db.get_withdrawal_totals(current_user["id"], account_id=account_id)
    series  = db.get_cumulative_pnl_series(current_user["id"], account_id=account_id)
    return templates.TemplateResponse("accountant_account_detail.html", {
        "request":       request,
        "current_user":  current_user,
        "active_page":   "accountant",
        "active_sub":    "accounts",
        "account":       account,
        "stats":         stats,
        "wt":            wt,
        "series_json":   __import__("json").dumps(series),
        "account_types": ACCOUNT_TYPES,
        "account_type_label": ACCOUNT_TYPES.get(account.get("account_type", ""), "Unknown"),
    })


@app.get("/accountant/withdrawals", response_class=HTMLResponse)
async def accountant_withdrawals_page(request: Request):
    current_user = _require_user(request)
    accounts = db.get_trading_accounts(current_user["id"])
    wt       = db.get_withdrawal_totals(current_user["id"])
    return templates.TemplateResponse("accountant_withdrawals.html", {
        "request":            request,
        "current_user":       current_user,
        "active_page":        "accountant",
        "active_sub":         "withdrawals",
        "accounts":           accounts,
        "wt":                 wt,
        "account_types":      ACCOUNT_TYPES,
        "distribution_types": DISTRIBUTION_TYPES,
    })


# ─────────────────────────────────────────────────────────────────
#  Accountant API — Trading Accounts
# ─────────────────────────────────────────────────────────────────

class AccountCreate(BaseModel):
    name: str
    broker: str = ""
    account_type: str = "taxable"
    is_active: bool = True
    starting_balance: float = 0.0
    default_tax_reserve_pct: float = 30.0
    notes: str = ""


@app.get("/api/accountant/accounts")
async def api_get_accounts(request: Request):
    user = _require_user_api(request)
    return db.get_trading_accounts(user["id"])


@app.post("/api/accountant/accounts")
async def api_create_account(request: Request, body: AccountCreate):
    user = _require_user_api(request)
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="Account name required")
    if body.account_type not in ACCOUNT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid account type")
    acct_id = db.create_trading_account(
        user["id"], body.name.strip(), body.broker.strip() or None,
        body.account_type, body.is_active, body.starting_balance,
        body.default_tax_reserve_pct, body.notes.strip() or None,
    )
    return {"id": acct_id, "status": "created"}


@app.put("/api/accountant/accounts/{account_id}")
async def api_update_account(request: Request, account_id: int, body: AccountCreate):
    user = _require_user_api(request)
    existing = db.get_trading_account(account_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Account not found")
    db.update_trading_account(
        account_id, user["id"],
        name=body.name.strip(),
        broker=body.broker.strip() or None,
        account_type=body.account_type,
        is_active=1 if body.is_active else 0,
        starting_balance=body.starting_balance,
        default_tax_reserve_pct=body.default_tax_reserve_pct,
        notes=body.notes.strip() or None,
    )
    return {"id": account_id, "status": "updated"}


@app.delete("/api/accountant/accounts/{account_id}")
async def api_archive_account(request: Request, account_id: int):
    user = _require_user_api(request)
    existing = db.get_trading_account(account_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Account not found")
    db.archive_trading_account(account_id, user["id"])
    return {"status": "archived"}


# ─────────────────────────────────────────────────────────────────
#  Accountant API — Trades
# ─────────────────────────────────────────────────────────────────

class TradeCreate(BaseModel):
    account_id: int
    trade_date: str
    ticker: str
    side: str = ""
    gross_pnl: float = 0.0
    fees: float = 0.0
    net_pnl: float = 0.0
    quantity: Optional[float] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    strategy_tag: str = ""
    confidence_tag: str = ""
    notes: str = ""
    screenshot_url: str = ""


@app.get("/api/accountant/trades")
async def api_get_trades(
    request: Request,
    account_id: int = None,
    date_from: str = None,
    date_to: str = None,
    ticker: str = None,
    limit: int = 200,
    offset: int = 0,
):
    user = _require_user_api(request)
    trades = db.get_trades(
        user["id"], account_id=account_id,
        date_from=date_from, date_to=date_to,
        ticker=ticker, limit=min(limit, 500), offset=offset,
    )
    return trades


@app.post("/api/accountant/trades")
async def api_create_trade(request: Request, body: TradeCreate):
    user = _require_user_api(request)
    # Verify account belongs to user
    acct = db.get_trading_account(body.account_id, user["id"])
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    if not body.ticker.strip():
        raise HTTPException(status_code=400, detail="Ticker required")
    # Auto-calculate net_pnl if not explicitly provided
    net_pnl = round(float(body.gross_pnl) - float(body.fees or 0), 2)
    # Allow manual override if net_pnl differs meaningfully
    if abs(body.net_pnl - net_pnl) > 0.01:
        net_pnl = round(float(body.net_pnl), 2)
    trade_id = db.create_trade(
        user["id"], body.account_id, body.trade_date, body.ticker,
        body.side.strip() or None, body.gross_pnl, body.fees or 0, net_pnl,
        body.quantity, body.entry_price, body.exit_price,
        body.strategy_tag.strip() or None,
        body.confidence_tag.strip() or None,
        body.notes.strip() or None,
        body.screenshot_url.strip() or None,
    )
    return {"id": trade_id, "net_pnl": net_pnl, "status": "created"}


@app.put("/api/accountant/trades/{trade_id}")
async def api_update_trade(request: Request, trade_id: int, body: TradeCreate):
    user = _require_user_api(request)
    existing = db.get_trade(trade_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Trade not found")
    acct = db.get_trading_account(body.account_id, user["id"])
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    net_pnl = round(float(body.gross_pnl) - float(body.fees or 0), 2)
    if abs(body.net_pnl - net_pnl) > 0.01:
        net_pnl = round(float(body.net_pnl), 2)
    db.update_trade(
        trade_id, user["id"],
        account_id=body.account_id, trade_date=body.trade_date,
        ticker=body.ticker, side=body.side.strip() or None,
        gross_pnl=body.gross_pnl, fees=body.fees or 0, net_pnl=net_pnl,
        quantity=body.quantity, entry_price=body.entry_price,
        exit_price=body.exit_price,
        strategy_tag=body.strategy_tag.strip() or None,
        confidence_tag=body.confidence_tag.strip() or None,
        notes=body.notes.strip() or None,
        screenshot_url=body.screenshot_url.strip() or None,
    )
    return {"id": trade_id, "net_pnl": net_pnl, "status": "updated"}


@app.delete("/api/accountant/trades/{trade_id}")
async def api_delete_trade(request: Request, trade_id: int):
    user = _require_user_api(request)
    existing = db.get_trade(trade_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Trade not found")
    db.delete_trade(trade_id, user["id"])
    return {"status": "deleted"}


# ─────────────────────────────────────────────────────────────────
#  Accountant API — Withdrawals
# ─────────────────────────────────────────────────────────────────

class WithdrawalCreate(BaseModel):
    account_id: int
    withdrawal_date: str
    gross_amount: float
    tax_reserve_pct: float = 30.0
    distribution_type: str = ""
    penalty_exception: bool = False
    under_59_5: bool = False
    qualified_distribution: bool = True
    notes: str = ""


@app.get("/api/accountant/withdrawals")
async def api_get_withdrawals(
    request: Request, account_id: int = None, limit: int = 100
):
    user = _require_user_api(request)
    return db.get_withdrawals(user["id"], account_id=account_id, limit=limit)


@app.post("/api/accountant/withdrawals")
async def api_create_withdrawal(request: Request, body: WithdrawalCreate):
    user = _require_user_api(request)
    acct = db.get_trading_account(body.account_id, user["id"])
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    if body.gross_amount <= 0:
        raise HTTPException(status_code=400, detail="Gross amount must be positive")
    estimates = _estimate_withdrawal(
        acct["account_type"], body.gross_amount, body.tax_reserve_pct,
        body.under_59_5, body.penalty_exception, body.qualified_distribution,
    )
    wd_id = db.create_withdrawal(
        user["id"], body.account_id, body.withdrawal_date, body.gross_amount,
        body.tax_reserve_pct, estimates["estimated_tax"],
        estimates["estimated_penalty"], estimates["net_to_owner"],
        estimates["retained_tax_reserve"],
        body.distribution_type.strip() or None,
        body.penalty_exception, body.under_59_5, body.qualified_distribution,
        body.notes.strip() or None,
    )
    return {"id": wd_id, "status": "created", **estimates}


@app.put("/api/accountant/withdrawals/{withdrawal_id}")
async def api_update_withdrawal(request: Request, withdrawal_id: int, body: WithdrawalCreate):
    user = _require_user_api(request)
    existing = db.get_withdrawal(withdrawal_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    acct = db.get_trading_account(body.account_id, user["id"])
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    estimates = _estimate_withdrawal(
        acct["account_type"], body.gross_amount, body.tax_reserve_pct,
        body.under_59_5, body.penalty_exception, body.qualified_distribution,
    )
    db.update_withdrawal(
        withdrawal_id, user["id"],
        account_id=body.account_id, withdrawal_date=body.withdrawal_date,
        gross_amount=body.gross_amount, tax_reserve_pct=body.tax_reserve_pct,
        estimated_tax=estimates["estimated_tax"],
        estimated_penalty=estimates["estimated_penalty"],
        net_to_owner=estimates["net_to_owner"],
        retained_tax_reserve=estimates["retained_tax_reserve"],
        distribution_type=body.distribution_type.strip() or None,
        penalty_exception=1 if body.penalty_exception else 0,
        under_59_5=1 if body.under_59_5 else 0,
        qualified_distribution=1 if body.qualified_distribution else 0,
        notes=body.notes.strip() or None,
    )
    return {"id": withdrawal_id, "status": "updated", **estimates}


@app.delete("/api/accountant/withdrawals/{withdrawal_id}")
async def api_delete_withdrawal(request: Request, withdrawal_id: int):
    user = _require_user_api(request)
    existing = db.get_withdrawal(withdrawal_id, user["id"])
    if not existing:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    db.delete_withdrawal(withdrawal_id, user["id"])
    return {"status": "deleted"}


# ─────────────────────────────────────────────────────────────────
#  Accountant API — Analytics
# ─────────────────────────────────────────────────────────────────

@app.get("/api/accountant/stats")
async def api_accountant_stats(request: Request, account_id: int = None):
    user = _require_user_api(request)
    stats = db.get_trade_stats(user["id"], account_id=account_id)
    wt    = db.get_withdrawal_totals(user["id"], account_id=account_id)
    return {"trade_stats": stats, "withdrawal_totals": wt}


@app.get("/api/accountant/calendar")
async def api_accountant_calendar(
    request: Request, year: int = None, month: int = None,
    account_id: int = None
):
    from datetime import date
    user = _require_user_api(request)
    today = date.today()
    y = year  or today.year
    m = month or today.month
    data = db.get_calendar_data(user["id"], y, m, account_id=account_id)
    return {"year": y, "month": m, "days": data}


@app.get("/api/accountant/series")
async def api_pnl_series(request: Request, account_id: int = None, days: int = 90):
    user = _require_user_api(request)
    return db.get_cumulative_pnl_series(user["id"], account_id=account_id, limit_days=days)


@app.post("/api/accountant/estimate-withdrawal")
async def api_estimate_withdrawal(request: Request, body: WithdrawalCreate):
    user = _require_user_api(request)
    acct = db.get_trading_account(body.account_id, user["id"])
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    return _estimate_withdrawal(
        acct["account_type"], body.gross_amount, body.tax_reserve_pct,
        body.under_59_5, body.penalty_exception, body.qualified_distribution,
    )


# ─────────────────────────────────────────────────────────────────
#  Local dev entry point
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    print(f"tradetuu → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
