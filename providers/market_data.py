"""
Market data provider — yfinance primary source.
Computes all derived price metrics needed by the scoring engine.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from models.stock_data import PriceData, ReferenceData
from cache_layer import get_cache
from config import get_config

log = logging.getLogger(__name__)


class MarketDataProvider:
    """
    Fetches OHLCV data and reference data (company info) from yfinance.
    Computes MAs, ATR, ADR, volume metrics inline.
    """

    def __init__(self):
        self._cfg = get_config()
        self._cache = get_cache()

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def get_price_data(self, ticker: str) -> PriceData:
        cache_key = f"price:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.price_cache_hours)
        if cached:
            log.debug("Cache hit price:%s", ticker)
            return self._deserialize_price(cached)

        pd_obj = PriceData(ticker=ticker)
        try:
            yf_ticker = yf.Ticker(ticker)
            pd_obj = self._fetch_and_compute(ticker, yf_ticker)
            self._cache.set(cache_key, self._serialize_price(pd_obj))
        except Exception as exc:
            log.error("MarketDataProvider error for %s: %s", ticker, exc)
            pd_obj.data_complete = False
        return pd_obj

    def get_reference_data(self, ticker: str) -> ReferenceData:
        cache_key = f"ref:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.reference_cache_hours)
        if cached:
            log.debug("Cache hit ref:%s", ticker)
            return ReferenceData(**cached)

        ref = ReferenceData(ticker=ticker)
        try:
            info = yf.Ticker(ticker).info or {}
            ref = self._build_reference(ticker, info)
            self._cache.set(cache_key, ref.__dict__)
        except Exception as exc:
            log.error("ReferenceData error for %s: %s", ticker, exc)
        return ref

    # ------------------------------------------------------------------ #
    #  Internal: fetch + compute
    # ------------------------------------------------------------------ #

    def _fetch_and_compute(self, ticker: str, yf_ticker: yf.Ticker) -> PriceData:
        pd_obj = PriceData(ticker=ticker, fetch_timestamp=datetime.utcnow())

        # --- Daily (2 years for MAs + ATR) ---
        daily = yf_ticker.history(period="2y", interval="1d", auto_adjust=True)
        if daily.empty:
            log.warning("%s: empty daily history", ticker)
            return pd_obj

        daily = daily[["Open", "High", "Low", "Close", "Volume"]].copy()
        daily.index = pd.to_datetime(daily.index).tz_localize(None)
        pd_obj.daily = daily

        # --- Weekly (3 years) ---
        weekly = yf_ticker.history(period="3y", interval="1wk", auto_adjust=True)
        if not weekly.empty:
            weekly = weekly[["Open", "High", "Low", "Close", "Volume"]].copy()
            weekly.index = pd.to_datetime(weekly.index).tz_localize(None)
        pd_obj.weekly = weekly if not weekly.empty else None

        # --- Monthly (5 years) ---
        monthly = yf_ticker.history(period="5y", interval="1mo", auto_adjust=True)
        if not monthly.empty:
            monthly = monthly[["Open", "High", "Low", "Close", "Volume"]].copy()
            monthly.index = pd.to_datetime(monthly.index).tz_localize(None)
        pd_obj.monthly = monthly if not monthly.empty else None

        # --- Current price & 52-week range ---
        closes = daily["Close"]
        pd_obj.current_price = float(closes.iloc[-1])
        cutoff_1y = closes.index[-1] - pd.Timedelta(days=365)
        last_year = closes[closes.index >= cutoff_1y]
        pd_obj.price_52w_high = float(last_year.max())
        pd_obj.price_52w_low = float(last_year.min())
        pd_obj.all_time_high = float(closes.max())

        # --- Volume metrics (30-day) ---
        cutoff_30d = daily.index[-1] - pd.Timedelta(days=30)
        last_30d = daily[daily.index >= cutoff_30d]
        pd_obj.avg_daily_volume_30d = float(last_30d["Volume"].mean())
        dollar_vol = last_30d["Close"] * last_30d["Volume"]
        pd_obj.avg_daily_dollar_volume_30d = float(dollar_vol.mean())

        # --- ATR(14) ---
        pd_obj.atr_14 = self._compute_atr(daily, 14)

        # --- ADR(20) as % ---
        cutoff_20d = daily.index[-1] - pd.Timedelta(days=20)
        last_20d = daily[daily.index >= cutoff_20d]
        daily_ranges = last_20d["High"] - last_20d["Low"]
        adr_abs = float(daily_ranges.mean())
        pd_obj.adr_20_pct = (adr_abs / pd_obj.current_price * 100) if pd_obj.current_price else None

        # --- Daily MAs ---
        pd_obj.ma_20d = self._ma(closes, 20)
        pd_obj.ma_50d = self._ma(closes, 50)

        # --- Weekly MAs (from weekly close) ---
        if pd_obj.weekly is not None:
            wc = pd_obj.weekly["Close"]
            pd_obj.ma_20w = self._ma(wc, 20)
            pd_obj.ma_40w = self._ma(wc, 40)

        # --- Monthly MAs ---
        if pd_obj.monthly is not None:
            mc = pd_obj.monthly["Close"]
            pd_obj.ma_10m = self._ma(mc, 10)
            pd_obj.ma_20m = self._ma(mc, 20)

        pd_obj.data_complete = True
        return pd_obj

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _compute_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
        try:
            h, l, c = df["High"], df["Low"], df["Close"]
            prev_c = c.shift(1)
            tr = pd.concat([
                h - l,
                (h - prev_c).abs(),
                (l - prev_c).abs()
            ], axis=1).max(axis=1)
            atr = tr.rolling(period).mean().iloc[-1]
            return float(atr) if not np.isnan(atr) else None
        except Exception:
            return None

    @staticmethod
    def _ma(series: pd.Series, period: int) -> Optional[float]:
        if len(series) < period:
            return None
        val = series.rolling(period).mean().iloc[-1]
        return float(val) if not np.isnan(val) else None

    @staticmethod
    def _build_reference(ticker: str, info: dict) -> ReferenceData:
        ref = ReferenceData(ticker=ticker, fetch_timestamp=datetime.utcnow())
        ref.company_name = info.get("longName") or info.get("shortName")
        ref.exchange = info.get("exchange")
        ref.sector = info.get("sector")
        ref.industry = info.get("industry")
        ref.market_cap = info.get("marketCap")
        ref.shares_outstanding = info.get("sharesOutstanding")
        ref.float_shares = info.get("floatShares")
        ref.security_type = info.get("quoteType")
        ref.description = info.get("longBusinessSummary", "")[:500]
        ref.country = info.get("country")
        ref.currency = info.get("currency")
        ref.data_complete = bool(ref.company_name and ref.sector)
        return ref

    # ------------------------------------------------------------------ #
    #  Cache serialization
    # ------------------------------------------------------------------ #

    @staticmethod
    def _serialize_price(pd_obj: PriceData) -> dict:
        d = {}
        for attr in [
            "ticker", "current_price", "price_52w_high", "price_52w_low",
            "all_time_high", "avg_daily_volume_30d", "avg_daily_dollar_volume_30d",
            "atr_14", "adr_20_pct", "ma_20d", "ma_50d", "ma_20w", "ma_40w",
            "ma_10m", "ma_20m", "data_complete", "fetch_timestamp",
        ]:
            d[attr] = getattr(pd_obj, attr, None)

        for frame_name in ["daily", "weekly", "monthly"]:
            df = getattr(pd_obj, frame_name, None)
            if df is not None and not df.empty:
                d[frame_name] = df.reset_index().to_dict(orient="list")
            else:
                d[frame_name] = None
        return d

    @staticmethod
    def _deserialize_price(d: dict) -> PriceData:
        pd_obj = PriceData(ticker=d["ticker"])
        for attr in [
            "current_price", "price_52w_high", "price_52w_low", "all_time_high",
            "avg_daily_volume_30d", "avg_daily_dollar_volume_30d",
            "atr_14", "adr_20_pct", "ma_20d", "ma_50d", "ma_20w", "ma_40w",
            "ma_10m", "ma_20m", "data_complete",
        ]:
            setattr(pd_obj, attr, d.get(attr))

        for frame_name in ["daily", "weekly", "monthly"]:
            raw = d.get(frame_name)
            if raw:
                try:
                    df = pd.DataFrame(raw)
                    date_col = "Date" if "Date" in df.columns else "Datetime"
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.set_index(date_col)
                    setattr(pd_obj, frame_name, df)
                except Exception:
                    setattr(pd_obj, frame_name, None)
        return pd_obj
