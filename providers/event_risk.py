"""
Event risk provider.
Detects structural red flags: reverse splits, offerings, shelf registrations,
going-concern, share dilution trends, etc.
Sources: yfinance info, SEC EDGAR EDGAR API, news classification.
"""
import logging
import re
from datetime import datetime, date, timedelta
from typing import List, Optional

import requests
import yfinance as yf

from models.stock_data import EventRiskData, NewsItem
from cache_layer import get_cache
from config import get_config

log = logging.getLogger(__name__)

EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&dateRange=custom&startdt={start}&enddt={end}&entity={ticker}"
EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index?q={query}&entity={ticker}&dateRange=custom&startdt={start}&enddt={end}"

_HEADERS = {"User-Agent": "StockFitnessAgent/1.0 (research tool; contact@example.com)"}


class EventRiskProvider:

    def __init__(self):
        self._cfg = get_config()
        self._cache = get_cache()

    def get_event_risk(self, ticker: str, news_items: Optional[List[NewsItem]] = None) -> EventRiskData:
        cache_key = f"event_risk:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.fundamentals_cache_hours)
        if cached:
            return self._deserialize(cached, ticker)

        erd = EventRiskData(ticker=ticker, fetch_timestamp=datetime.utcnow())
        try:
            # 1. Parse from news items (fast, no extra API call)
            if news_items:
                self._scan_news(erd, news_items)

            # 2. yfinance info for share count trend
            self._scan_yf_info(erd, ticker)

            # 3. SEC EDGAR search (best-effort, may be rate limited)
            self._scan_edgar(erd, ticker)

            erd.data_complete = True
            self._cache.set(cache_key, self._serialize(erd))
        except Exception as exc:
            log.error("EventRiskProvider error for %s: %s", ticker, exc)
        return erd

    # ------------------------------------------------------------------ #
    #  News scan
    # ------------------------------------------------------------------ #

    def _scan_news(self, erd: EventRiskData, news_items: List[NewsItem]) -> None:
        today = date.today()
        for item in news_items:
            text = ((item.headline or "") + " " + (item.summary or "")).lower()
            pub_date = item.published_at.date() if item.published_at else None

            if not pub_date:
                continue

            age_days = (today - pub_date).days

            # Reverse split
            if re.search(r"reverse\s+(stock\s+)?split", text):
                if age_days <= self._cfg.reverse_split_lookback_days:
                    erd.has_reverse_split_12m = True
                    if pub_date not in erd.reverse_split_dates:
                        erd.reverse_split_dates.append(pub_date)
                    erd.risk_notes.append(f"Reverse split mentioned in news ({pub_date})")

            # Delisting
            if any(kw in text for kw in ["delist", "nasdaq notice", "nyse notice",
                                          "listing standards", "non-compliance"]):
                erd.has_active_delisting_warning = True
                erd.risk_notes.append(f"Delisting/listing issue in news ({pub_date})")

            # Going concern
            if "going concern" in text:
                erd.has_going_concern = True
                erd.risk_notes.append(f"Going concern mention ({pub_date})")

            # Bankruptcy
            if any(kw in text for kw in ["bankruptcy", "chapter 11", "chapter 7",
                                          "restructuring", "creditor"]):
                erd.has_bankruptcy_restructuring = True
                erd.risk_notes.append(f"Bankruptcy/restructuring signal ({pub_date})")

            # Offering (< 30 days)
            if item.category == "dilution_or_capital_raise" and age_days <= self._cfg.offering_penalty_window_days:
                erd.has_recent_offering_30d = True
                erd.offering_date = pub_date

            # Shelf registration (< 180 days)
            if "shelf registration" in text and age_days <= self._cfg.shelf_penalty_window_days:
                erd.has_shelf_registration_180d = True
                erd.shelf_date = pub_date

    # ------------------------------------------------------------------ #
    #  yfinance info scan
    # ------------------------------------------------------------------ #

    def _scan_yf_info(self, erd: EventRiskData, ticker: str) -> None:
        try:
            info = yf.Ticker(ticker).info or {}
            # Note: company description is a general business summary, not a risk disclosure.
            # We do not set hard-reject flags from the description to avoid false positives.
            # Hard-reject events (going concern, reverse split) must come from news items.
        except Exception as exc:
            log.debug("yf_info event risk error for %s: %s", ticker, exc)

    # ------------------------------------------------------------------ #
    #  SEC EDGAR search (best-effort)
    # ------------------------------------------------------------------ #

    def _scan_edgar(self, erd: EventRiskData, ticker: str) -> None:
        today = date.today()
        start_date = (today - timedelta(days=self._cfg.filings_lookback_days)).isoformat()
        end_date = today.isoformat()

        checks = [
            ("going concern", "going+concern"),
            ("reverse split", "reverse+split"),
            ("shelf registration", "shelf+registration"),
        ]

        for label, query in checks:
            try:
                url = (
                    f"https://efts.sec.gov/LATEST/search-index?q=%22{query.replace('+', '%20')}%22"
                    f"&dateRange=custom&startdt={start_date}&enddt={end_date}&entity={ticker}"
                )
                resp = requests.get(url, headers=_HEADERS, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    hits = data.get("hits", {}).get("hits", [])
                    if hits:
                        erd.risk_notes.append(f"SEC EDGAR hit: {label} ({len(hits)} filing(s))")
                        # Only set shelf registration from EDGAR (soft penalty).
                        # Going concern and reverse split require news corroboration to avoid
                        # false positives from risk-factor disclosures in 10-K/10-Q filings.
                        if "shelf registration" in label:
                            erd.has_shelf_registration_180d = True
                        # Going concern / reverse split from EDGAR only if 3+ distinct hits
                        # AND not already flagged by news (reduces false positives)
                        elif "going concern" in label and len(hits) >= 3 and not erd.has_going_concern:
                            erd.risk_notes.append("EDGAR going-concern: multiple hits — verify manually")
                        elif "reverse split" in label and len(hits) >= 2 and not erd.has_reverse_split_12m:
                            erd.risk_notes.append("EDGAR reverse-split: multiple hits — verify manually")
            except Exception as exc:
                log.debug("EDGAR search error for %s/%s: %s", ticker, label, exc)

    # ------------------------------------------------------------------ #
    #  Share count YoY (called from pipeline after fundamentals load)
    # ------------------------------------------------------------------ #

    @staticmethod
    def compute_share_count_yoy(erd: EventRiskData, balance_sheets: list) -> None:
        """
        Called by pipeline after balance sheets are loaded.
        balance_sheets sorted ascending by period_end.
        """
        if len(balance_sheets) < 2:
            return
        try:
            sorted_bs = sorted(
                [b for b in balance_sheets if b.shares_outstanding],
                key=lambda x: x.period_end or date.min
            )
            if len(sorted_bs) < 2:
                return
            latest = sorted_bs[-1].shares_outstanding
            prior = sorted_bs[-2].shares_outstanding
            if prior and prior > 0:
                erd.share_count_yoy_pct_change = (latest - prior) / prior * 100
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Serialization
    # ------------------------------------------------------------------ #

    @staticmethod
    def _serialize(erd: EventRiskData) -> dict:
        return {
            "ticker": erd.ticker,
            "has_reverse_split_12m": erd.has_reverse_split_12m,
            "has_active_delisting_warning": erd.has_active_delisting_warning,
            "has_going_concern": erd.has_going_concern,
            "has_bankruptcy_restructuring": erd.has_bankruptcy_restructuring,
            "has_recent_offering_30d": erd.has_recent_offering_30d,
            "has_shelf_registration_180d": erd.has_shelf_registration_180d,
            "offering_date": str(erd.offering_date) if erd.offering_date else None,
            "shelf_date": str(erd.shelf_date) if erd.shelf_date else None,
            "share_count_yoy_pct_change": erd.share_count_yoy_pct_change,
            "reverse_split_dates": [str(d) for d in erd.reverse_split_dates],
            "risk_notes": erd.risk_notes,
            "data_complete": erd.data_complete,
        }

    @staticmethod
    def _deserialize(d: dict, ticker: str) -> EventRiskData:
        erd = EventRiskData(ticker=ticker)
        erd.has_reverse_split_12m = d.get("has_reverse_split_12m", False)
        erd.has_active_delisting_warning = d.get("has_active_delisting_warning", False)
        erd.has_going_concern = d.get("has_going_concern", False)
        erd.has_bankruptcy_restructuring = d.get("has_bankruptcy_restructuring", False)
        erd.has_recent_offering_30d = d.get("has_recent_offering_30d", False)
        erd.has_shelf_registration_180d = d.get("has_shelf_registration_180d", False)
        erd.share_count_yoy_pct_change = d.get("share_count_yoy_pct_change")
        erd.risk_notes = d.get("risk_notes", [])
        erd.data_complete = d.get("data_complete", False)
        for ds in d.get("reverse_split_dates", []):
            try:
                erd.reverse_split_dates.append(date.fromisoformat(ds))
            except Exception:
                pass
        od = d.get("offering_date")
        if od:
            try:
                erd.offering_date = date.fromisoformat(od)
            except Exception:
                pass
        sd = d.get("shelf_date")
        if sd:
            try:
                erd.shelf_date = date.fromisoformat(sd)
            except Exception:
                pass
        return erd
