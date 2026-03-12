"""
Stock Fitness Agent — Web API (FastAPI)

Exposes:
  GET  /health                        — liveness probe
  GET  /                              — dashboard HTML
  POST /api/scan                      — create a scan job, returns job_id
  POST /api/scan/{job_id}/cancel      — cancel a running scan
  GET  /api/scan/{job_id}/progress    — job status + progress
  GET  /api/scan/{job_id}/results     — all ticker results for a completed scan
  GET  /api/scans                     — list recent scan jobs
  POST /api/evaluate                  — evaluate a single ticker (synchronous)

Worker mode (controlled by RUN_WORKER_INLINE env var):
  true  — launches the scanner as a background daemon thread (Render free tier / local)
  false — web service only; deploy worker/scanner_worker.py as a separate process
"""
import sys
import os
import logging
from contextlib import asynccontextmanager

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env if present (local development only)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Ensure UTF-8 output on Windows terminals
import io
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import db.database as db
from engine.pipeline import ScoringPipeline
from utils.serialize import result_to_dict

# ------------------------------------------------------------------ #
#  Logging
# ------------------------------------------------------------------ #

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
for noisy in ["yfinance", "urllib3", "requests", "peewee"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  App
# ------------------------------------------------------------------ #

@asynccontextmanager
async def lifespan(app_instance):
    # ── Startup ──────────────────────────────────────────────────────
    db.get_engine()
    n = db.reset_interrupted_jobs()
    if n:
        log.info("Reset %d interrupted job(s) back to pending on startup", n)

    run_inline = os.environ.get("RUN_WORKER_INLINE", "true").lower() != "false"
    if run_inline:
        from worker.scanner_worker import start_worker_thread
        start_worker_thread()
        log.info("Running with inline worker thread")
    else:
        log.info("Inline worker disabled — expecting external worker process")

    yield
    # ── Shutdown (nothing to clean up) ───────────────────────────────


app = FastAPI(
    title="Stock Fitness Agent",
    description="Evaluates stocks for active trading style fitness. Not a trade signal system.",
    version="2.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy pipeline — initialized on first evaluate request
_pipeline: ScoringPipeline | None = None


def _get_pipeline() -> ScoringPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = ScoringPipeline()
    return _pipeline


# ------------------------------------------------------------------ #
#  Routes
# ------------------------------------------------------------------ #

@app.get("/health")
async def health():
    return {"status": "ok", "service": "stock-fitness-agent"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ── Scan jobs ────────────────────────────────────────────────────── #

_VALID_UNIVERSES = {
    "sp500",
    "sp500_extended",
    "nasdaq_full",
    "nyse_nasdaq_full",
}


class ScanRequest(BaseModel):
    universe: str = "sp500"


@app.post("/api/scan")
async def start_scan(body: ScanRequest):
    if body.universe not in _VALID_UNIVERSES:
        raise HTTPException(status_code=400, detail=f"Unknown universe: {body.universe}")

    job_id = db.create_job(body.universe)
    log.info("Scan job created: %s universe=%s", job_id, body.universe)
    return {"job_id": job_id, "universe": body.universe, "status": "pending"}


@app.post("/api/scan/{job_id}/cancel")
async def cancel_scan(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.cancel_job(job_id)
    return {"job_id": job_id, "status": "cancelled"}


@app.get("/api/scan/{job_id}/progress")
async def scan_progress(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    total = job.get("total_tickers") or 0
    processed = job.get("processed_tickers") or 0
    pct = round(processed / total * 100, 1) if total > 0 else 0.0

    return {
        "job_id":          job_id,
        "universe":        job.get("universe"),
        "status":          job.get("status"),
        "percent":         pct,
        "progress":        processed,
        "total":           total,
        "current_ticker":  job.get("current_ticker", ""),
        "results_so_far":  db.get_job_result_count(job_id),
        "created_at":      job.get("created_at"),
        "started_at":      job.get("started_at"),
        "completed_at":    job.get("completed_at"),
        "error_message":   job.get("error_message"),
    }


@app.get("/api/scan/{job_id}/results")
async def scan_results(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    results = db.get_job_results(job_id)
    return JSONResponse(results)


@app.get("/api/scans")
async def list_scans(limit: int = 20):
    jobs = db.list_recent_jobs(limit=limit)
    return jobs


# ── Single ticker ────────────────────────────────────────────────── #

class EvaluateRequest(BaseModel):
    ticker: str


@app.post("/api/evaluate")
async def evaluate_ticker(body: EvaluateRequest):
    ticker = body.ticker.upper().strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker required")
    try:
        pipeline = _get_pipeline()
        result = pipeline.evaluate(ticker)
        return JSONResponse(result_to_dict(result))
    except Exception as exc:
        log.error("Evaluate error for %s: %s", ticker, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ------------------------------------------------------------------ #
#  Local development entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Stock Fitness Agent at http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
