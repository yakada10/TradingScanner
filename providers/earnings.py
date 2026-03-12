"""
Earnings provider.
Primary: yfinance earnings calendar + history.
Optional: Finnhub for richer calendar data (if API key is configured).
"""
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional
import numpy as np

import yfinance as yf
import pandas as pd

from models.stock_data import EarningsData, EarningsRecord
from cache_layer import get_cache
from config import get_config

log = logging.getLogger(__name__)


def _trading_days_between(start: date, end: date) -> int:
    """Approximate trading days (Mon-Fri) between two dates."""
    if end <= start:
        return 0
    count = 0
    cur = start + timedelta(days=1)
    while cur <= end:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count


class EarningsProvider:

    def __init__(self):
        self._cfg = get_config()
        self._cache = get_cache()

    def get_earnings(self, ticker: str) -> EarningsData:
        cache_key = f"earnings:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.earnings_cache_hours)
        if cached:
            return self._deserialize(cached, ticker)

        ed = EarningsData(ticker=ticker, fetch_timestamp=datetime.utcnow())
        try:
            yf_ticker = yf.Ticker(ticker)
            ed = self._fetch_yf(ticker, yf_ticker)

            # Optionally supplement with Finnhub
            if self._cfg.finnhub_api_key:
                ed = self._supplement_finnhub(ticker, ed)

            self._cache.set(cache_key, self._serialize(ed))
        except Exception as exc:
            log.error("EarningsProvider error for %s: %s", ticker, exc)
        return ed

    # ------------------------------------------------------------------ #
    #  yfinance
    # ------------------------------------------------------------------ #

    def _fetch_yf(self, ticker: str, yf_ticker: yf.Ticker) -> EarningsData:
        ed = EarningsData(ticker=ticker, fetch_timestamp=datetime.utcnow())

        today = date.today()

        # --- Calendar (next earnings date) ---
        try:
            cal = yf_ticker.calendar
            if cal is not None and not cal.empty:
                # calendar is a DataFrame with index = field name
                if "Earnings Date" in cal.index:
                    raw_date = cal.loc["Earnings Date"].values[0]
                    if raw_date is not None:
                        candidate = pd.Timestamp(raw_date).date()
                        # Only use if it is a future date (or today)
                        if candidate >= today:
                            ed.next_earnings_date = candidate
                            ed.trading_days_to_earnings = _trading_days_between(
                                today, ed.next_earnings_date
                            )
        except Exception as exc:
            log.debug("Calendar parse error for %s: %s", ticker, exc)

        # Also try info dict
        if ed.next_earnings_date is None:
            try:
                info = yf_ticker.info or {}
                raw = info.get("earningsTimestamp") or info.get("earningsDate")
                if raw:
                    ts = pd.Timestamp(raw, unit="s") if isinstance(raw, (int, float)) else pd.Timestamp(raw)
                    candidate = ts.date()
                    # Only use if it is a future date (or today)
                    if candidate >= today:
                        ed.next_earnings_date = candidate
                        ed.trading_days_to_earnings = _trading_days_between(
                            today, ed.next_earnings_date
                        )
            except Exception:
                pass

        # --- Earnings history ---
        try:
            hist = yf_ticker.earnings_history
            if hist is not None and not hist.empty:
                for _, row in hist.iterrows():
                    rec = EarningsRecord()
                    try:
                        rec.report_date = pd.Timestamp(row.name).date()
                    except Exception:
                        pass
                    rec.reported_eps = self._float(row.get("epsActual"))
                    rec.estimated_eps = self._float(row.get("epsEstimate"))
                    surprise = row.get("surprisePercent")
                    rec.surprise_pct = self._float(surprise)
                    ed.recent_earnings.append(rec)
        except Exception as exc:
            log.debug("Earnings history error for %s: %s", ticker, exc)

        ed.data_complete = ed.next_earnings_date is not None or bool(ed.recent_earnings)
        return ed

    # ------------------------------------------------------------------ #
    #  Finnhub supplement
    # ------------------------------------------------------------------ #

    def _supplement_finnhub(self, ticker: str, ed: EarningsData) -> EarningsData:
        try:
            import finnhub
            client = finnhub.Client(api_key=self._cfg.finnhub_api_key)
            today = date.today()
            end = today + timedelta(days=90)
            cal = client.earnings_calendar(
                _from=today.isoformat(),
                to=end.isoformat(),
                symbol=ticker,
            )
            earnings_list = cal.get("earningsCalendar", [])
            if earnings_list:
                # Take earliest upcoming
                upcoming = sorted(
                    [e for e in earnings_list if e.get("date")],
                    key=lambda x: x["date"]
                )
                if upcoming:
                    d = date.fromisoformat(upcoming[0]["date"])
                    if ed.next_earnings_date is None or d < ed.next_earnings_date:
                        ed.next_earnings_date = d
                        ed.trading_days_to_earnings = _trading_days_between(
                            date.today(), d
                        )
        except ImportError:
            pass
        except Exception as exc:
            log.debug("Finnhub earnings supplement error for %s: %s", ticker, exc)
        return ed

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _float(v) -> Optional[float]:
        if v is None:
            return None
        try:
            f = float(v)
            return None if np.isnan(f) else f
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------ #
    #  Serialization
    # ------------------------------------------------------------------ #

    @staticmethod
    def _serialize(ed: EarningsData) -> dict:
        records = []
        for r in ed.recent_earnings:
            records.append({
                "period": r.period,
                "reported_eps": r.reported_eps,
                "estimated_eps": r.estimated_eps,
                "surprise_pct": r.surprise_pct,
                "revenue_actual": r.revenue_actual,
                "revenue_estimated": r.revenue_estimated,
                "report_date": str(r.report_date) if r.report_date else None,
            })
        return {
            "ticker": ed.ticker,
            "next_earnings_date": str(ed.next_earnings_date) if ed.next_earnings_date else None,
            "trading_days_to_earnings": ed.trading_days_to_earnings,
            "recent_earnings": records,
            "data_complete": ed.data_complete,
        }

    @staticmethod
    def _deserialize(d: dict, ticker: str) -> EarningsData:
        ed = EarningsData(ticker=ticker)
        ned = d.get("next_earnings_date")
        if ned:
            try:
                candidate = date.fromisoformat(ned)
                today = date.today()
                # Only restore if still a future date; recalculate trading days
                if candidate >= today:
                    ed.next_earnings_date = candidate
                    ed.trading_days_to_earnings = _trading_days_between(today, candidate)
            except Exception:
                pass
        ed.data_complete = d.get("data_complete", False)
        for raw in d.get("recent_earnings", []):
            rec = EarningsRecord()
            rec.period = raw.get("period")
            rec.reported_eps = raw.get("reported_eps")
            rec.estimated_eps = raw.get("estimated_eps")
            rec.surprise_pct = raw.get("surprise_pct")
            rec.revenue_actual = raw.get("revenue_actual")
            rec.revenue_estimated = raw.get("revenue_estimated")
            rd = raw.get("report_date")
            if rd:
                try:
                    rec.report_date = date.fromisoformat(rd)
                except Exception:
                    pass
            ed.recent_earnings.append(rec)
        return ed
