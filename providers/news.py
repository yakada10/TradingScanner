"""
News provider.
Primary: yfinance news (always available, no key needed).
Supplement: Finnhub news (if API key configured).
Each item is classified into the standard taxonomy from spec section 20.
"""
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import yfinance as yf

from models.stock_data import NewsItem
from cache_layer import get_cache
from config import get_config

log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  Keyword classification maps
# ------------------------------------------------------------------ #

_CRITICAL_NEGATIVE_KW = [
    "bankruptcy", "chapter 11", "chapter 7", "delisting", "delist",
    "going concern", "reverse split", "reverse stock split",
    "fraud", "sec investigation", "accounting irregularities",
    "default", "liquidity crisis", "regulatory block",
]

_HIGH_NEGATIVE_KW = [
    "secondary offering", "public offering", "shelf registration",
    "at-the-market offering", "atm offering", "equity offering",
    "registered direct", "dilution", "share issuance",
    "miss", "missed", "earnings miss", "guidance cut",
    "downgrade", "warning", "investigation", "lawsuit", "litigation",
    "resignation", "ceo departs", "cfo departs", "going concern",
]

_POSITIVE_KW = [
    "beat", "beats", "earnings beat", "record revenue", "record earnings",
    "guidance raised", "raises guidance", "upgrade", "buy rating",
    "new contract", "partnership", "acquisition", "share buyback",
    "dividend", "new product", "fda approval", "regulatory approval",
    "expanding", "growth", "profit", "record quarter",
]

_DILUTION_KW = [
    "offering", "shelf registration", "atm", "at-the-market",
    "shares sold", "share issuance", "equity raise",
    "registered direct", "public offering", "secondary",
]

_LEGAL_KW = [
    "lawsuit", "litigation", "sec", "doj", "investigation",
    "fraud", "settlement", "class action", "legal action", "fine", "penalty",
]

_DELISTING_KW = [
    "delist", "delisting", "nasdaq notice", "nyse notice",
    "non-compliance", "minimum bid price", "listing standards",
]

_PROMOTIONAL_KW = [
    "speculative", "moonshot", "squeeze", "short squeeze",
    "reddit", "meme", "wsb", "wallstreetbets",
]


class NewsProvider:

    def __init__(self):
        self._cfg = get_config()
        self._cache = get_cache()

    def get_news(self, ticker: str) -> List[NewsItem]:
        cache_key = f"news:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.news_cache_hours)
        if cached:
            return [self._dict_to_item(d) for d in cached]

        items: List[NewsItem] = []
        try:
            items = self._fetch_yf(ticker)
        except Exception as exc:
            log.error("NewsProvider yfinance error for %s: %s", ticker, exc)

        if self._cfg.finnhub_api_key:
            try:
                items += self._fetch_finnhub(ticker)
            except Exception as exc:
                log.debug("NewsProvider finnhub error for %s: %s", ticker, exc)

        # Deduplicate by headline
        seen = set()
        unique = []
        for item in items:
            key = item.headline[:80].lower()
            if key not in seen:
                seen.add(key)
                unique.append(item)

        # Classify all items
        for item in unique:
            self._classify(item)

        self._cache.set(cache_key, [self._item_to_dict(i) for i in unique])
        return unique

    # ------------------------------------------------------------------ #
    #  yfinance
    # ------------------------------------------------------------------ #

    def _fetch_yf(self, ticker: str) -> List[NewsItem]:
        yf_ticker = yf.Ticker(ticker)
        raw_news = yf_ticker.news or []
        items = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._cfg.news_lookback_days)
        for article in raw_news:
            try:
                ts = article.get("providerPublishTime")
                if ts:
                    pub = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    pub = None
                if pub and pub < cutoff:
                    continue
                item = NewsItem(
                    headline=article.get("title", ""),
                    source=article.get("publisher"),
                    published_at=pub,
                    url=article.get("link"),
                    summary=article.get("summary", ""),
                )
                items.append(item)
            except Exception:
                continue
        return items

    # ------------------------------------------------------------------ #
    #  Finnhub
    # ------------------------------------------------------------------ #

    def _fetch_finnhub(self, ticker: str) -> List[NewsItem]:
        import finnhub
        client = finnhub.Client(api_key=self._cfg.finnhub_api_key)
        today = datetime.now(timezone.utc).date()
        from_date = (today - timedelta(days=self._cfg.news_lookback_days)).isoformat()
        raw = client.company_news(ticker, _from=from_date, to=today.isoformat())
        items = []
        for article in (raw or []):
            try:
                ts = article.get("datetime")
                pub = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
                item = NewsItem(
                    headline=article.get("headline", ""),
                    source=article.get("source"),
                    published_at=pub,
                    url=article.get("url"),
                    summary=article.get("summary", ""),
                )
                items.append(item)
            except Exception:
                continue
        return items

    # ------------------------------------------------------------------ #
    #  Classification
    # ------------------------------------------------------------------ #

    def _classify(self, item: NewsItem) -> None:
        text = ((item.headline or "") + " " + (item.summary or "")).lower()
        now = datetime.now(timezone.utc)

        # Freshness
        if item.published_at:
            age_days = (now - item.published_at).days
            if age_days <= 7:
                item.freshness_bucket = "0-7d"
            elif age_days <= 30:
                item.freshness_bucket = "8-30d"
            elif age_days <= 90:
                item.freshness_bucket = "31-90d"
            else:
                item.freshness_bucket = "90d+"
        else:
            item.freshness_bucket = "unknown"

        # Severity / category / direction logic
        if any(kw in text for kw in _CRITICAL_NEGATIVE_KW):
            item.severity = "critical"
            item.direction = "negative"
            if any(kw in text for kw in _DELISTING_KW):
                item.category = "delisting_or_listing_issue"
            elif any(kw in text for kw in _DILUTION_KW):
                item.category = "dilution_or_capital_raise"
            elif "going concern" in text or "bankruptcy" in text:
                item.category = "delisting_or_listing_issue"
            else:
                item.category = "negative_financial"

        elif any(kw in text for kw in _HIGH_NEGATIVE_KW):
            item.severity = "high"
            item.direction = "negative"
            if any(kw in text for kw in _DILUTION_KW):
                item.category = "dilution_or_capital_raise"
            elif any(kw in text for kw in _LEGAL_KW):
                item.category = "legal_or_regulatory"
            else:
                item.category = "negative_financial"

        elif any(kw in text for kw in _POSITIVE_KW):
            item.severity = "medium"
            item.direction = "positive"
            if "revenue" in text or "earnings" in text or "profit" in text:
                item.category = "positive_financial"
            elif "product" in text or "approval" in text or "contract" in text:
                item.category = "product_or_catalyst"
            else:
                item.category = "positive_business"

        elif any(kw in text for kw in _PROMOTIONAL_KW):
            item.severity = "low"
            item.direction = "neutral"
            item.category = "promotional_noise"

        elif any(kw in text for kw in _LEGAL_KW):
            item.severity = "medium"
            item.direction = "negative"
            item.category = "legal_or_regulatory"

        else:
            item.severity = "low"
            item.direction = "neutral"
            item.category = "neutral"

    # ------------------------------------------------------------------ #
    #  Serialization
    # ------------------------------------------------------------------ #

    @staticmethod
    def _item_to_dict(item: NewsItem) -> dict:
        return {
            "headline": item.headline,
            "source": item.source,
            "published_at": item.published_at.isoformat() if item.published_at else None,
            "url": item.url,
            "summary": item.summary,
            "category": item.category,
            "direction": item.direction,
            "severity": item.severity,
            "freshness_bucket": item.freshness_bucket,
        }

    @staticmethod
    def _dict_to_item(d: dict) -> NewsItem:
        pub = None
        if d.get("published_at"):
            try:
                pub = datetime.fromisoformat(d["published_at"])
            except Exception:
                pass
        return NewsItem(
            headline=d.get("headline", ""),
            source=d.get("source"),
            published_at=pub,
            url=d.get("url"),
            summary=d.get("summary", ""),
            category=d.get("category"),
            direction=d.get("direction"),
            severity=d.get("severity"),
            freshness_bucket=d.get("freshness_bucket"),
        )
