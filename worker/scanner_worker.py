"""
tradetuu — Background Scanner Worker

Two operating modes:
  1. Inline thread (default for Render free tier single dyno):
       Called from web/app.py on startup via start_worker_thread().
       Runs as a daemon thread inside the web process.

  2. Standalone process (for multi-dyno / paid Render setup):
       python worker/scanner_worker.py
       Polls the database for pending jobs and processes them independently.

The worker polls the job queue every POLL_INTERVAL seconds. When it finds a
pending job, it claims it atomically, evaluates the full universe, writes each
result to the database as it completes, and marks the job done.

Because results are persisted to the database as each ticker completes, the scan
survives process restarts — any interrupted job is reset to 'pending' on startup
and picked up again automatically.
"""
import sys
import os
import time
import gc
import logging
import threading
from typing import Optional

# Add project root to sys.path regardless of where this is launched from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env if present (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import db.database as db
from engine.pipeline import ScoringPipeline
from engine.universe_loader import UniverseLoader
from utils.serialize import result_to_dict

log = logging.getLogger(__name__)

POLL_INTERVAL = int(os.environ.get("WORKER_POLL_INTERVAL", "3"))  # seconds

_pipeline: Optional[ScoringPipeline] = None
_loader: Optional[UniverseLoader] = None
_pipeline_lock = threading.Lock()


def _get_pipeline() -> ScoringPipeline:
    global _pipeline
    with _pipeline_lock:
        if _pipeline is None:
            _pipeline = ScoringPipeline()
    return _pipeline


def _get_loader() -> UniverseLoader:
    global _loader
    if _loader is None:
        _loader = UniverseLoader()
    return _loader


_UNIVERSE_MAP = {
    "sp500":            lambda l: l.sp500(),
    "sp500_extended":   lambda l: l.sp500_extended(),
    "nasdaq_full":      lambda l: l.nasdaq_full(),
    "nyse_nasdaq_full": lambda l: l.nyse_nasdaq_full(),
}


# ------------------------------------------------------------------ #
#  Job processor
# ------------------------------------------------------------------ #

def process_job(job: dict) -> None:
    job_id  = job["id"]
    universe = job["universe"]
    log.info("[%s] Starting job universe=%s", job_id, universe)

    try:
        loader = _get_loader()
        loader_fn = _UNIVERSE_MAP.get(universe)
        if loader_fn is None:
            db.update_job_failed(job_id, f"Unknown universe: {universe}")
            return

        tickers = loader_fn(loader)
        if not tickers:
            db.update_job_failed(job_id, "Universe loader returned empty ticker list")
            return

        db.update_job_started(job_id, len(tickers))
        log.info("[%s] Processing %d tickers", job_id, len(tickers))

        pipeline = _get_pipeline()

        for i, ticker in enumerate(tickers):
            # Check for cancellation every tick
            current = db.get_job(job_id)
            if current and current["status"] == "cancelled":
                log.info("[%s] Cancelled at ticker %d/%d", job_id, i, len(tickers))
                return

            db.update_job_progress(job_id, i, ticker)

            try:
                result = pipeline.evaluate(ticker)
                db.save_ticker_result(job_id, result_to_dict(result))
                del result  # release pandas DataFrames held in result object
            except Exception as exc:
                log.warning("[%s] Error evaluating %s: %s", job_id, ticker, exc)
            finally:
                # Force Python GC every ticker to prevent cumulative memory buildup.
                # pandas DataFrames (OHLCV history) are the main culprit — without
                # explicit collection they accumulate and cause OOM on long scans.
                gc.collect()

        db.update_job_progress(job_id, len(tickers), "")
        db.update_job_completed(job_id)
        log.info("[%s] Completed %d tickers", job_id, len(tickers))

    except Exception as exc:
        log.error("[%s] Job failed: %s", job_id, exc, exc_info=True)
        db.update_job_failed(job_id, str(exc))


# ------------------------------------------------------------------ #
#  Worker loop
# ------------------------------------------------------------------ #

def run_worker_loop() -> None:
    """
    Main polling loop. Runs forever.
    In inline mode this is called in a daemon thread.
    In standalone mode this is called as __main__.
    """
    KEEP_DAYS = int(os.environ.get("SCAN_HISTORY_DAYS", "30"))
    CLEANUP_INTERVAL = 86400  # run cleanup once per day (seconds)
    last_cleanup = 0.0

    log.info("Scanner worker started (poll interval %ds, history kept %d days)",
             POLL_INTERVAL, KEEP_DAYS)

    while True:
        try:
            # Daily cleanup — purge scans older than KEEP_DAYS
            now = time.time()
            if now - last_cleanup >= CLEANUP_INTERVAL:
                try:
                    db.purge_old_jobs(keep_days=KEEP_DAYS)
                except Exception as exc:
                    log.warning("Cleanup error: %s", exc)
                last_cleanup = now

            jobs = db.get_pending_jobs(limit=1)
            if jobs:
                job = jobs[0]
                claimed = db.claim_job(job["id"])
                if claimed:
                    process_job(job)
                    continue  # immediately check for more pending jobs
            time.sleep(POLL_INTERVAL)
        except Exception as exc:
            log.error("Worker loop error: %s", exc, exc_info=True)
            time.sleep(POLL_INTERVAL)


def start_worker_thread() -> threading.Thread:
    """
    Launch the worker as a background daemon thread inside the web process.
    Used when RUN_WORKER_INLINE=true (default for single-dyno Render free tier).
    """
    t = threading.Thread(target=run_worker_loop, daemon=True, name="scanner-worker")
    t.start()
    log.info("Scanner worker thread started")
    return t


# ------------------------------------------------------------------ #
#  Standalone entry point
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    for noisy in ["yfinance", "urllib3", "requests", "peewee"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Initialize DB (creates schema if not present)
    db.get_engine()

    # Reset any jobs that were interrupted before this process started
    db.reset_interrupted_jobs()

    run_worker_loop()
