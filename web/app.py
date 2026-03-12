"""
Stock Fitness Agent — Web API + UI Server

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

@asynccontextmanager
async def lifespan(app_instance):
    db.get_engine()
    n = db.reset_interrupted_jobs()
    if n:
        log.info("Reset %d interrupted job(s) to pending on startup", n)

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
    title="Stock Fitness Agent",
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
    return {"status": "ok", "service": "stock-fitness-agent"}


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

@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
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

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    current_user = _require_user(request)
    stats = db.get_dashboard_stats()
    recent_jobs = db.list_recent_jobs(limit=8)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "current_user": current_user,
        "active_page": "dashboard",
        "stats": stats,
        "recent_jobs": recent_jobs,
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
    total = job.get("total_tickers") or 0
    processed = job.get("processed_tickers") or 0
    pct = round(processed / total * 100, 1) if total > 0 else 0.0
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


# ─────────────────────────────────────────────────────────────────
#  Local dev entry point
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    print(f"Stock Fitness Agent → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
