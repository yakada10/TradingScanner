"""
Technical Trend Fitness scorer — 30 points total.
  Monthly Structure:  10 pts
  Weekly Structure:   12 pts
  Daily Structure:     8 pts
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from models.stock_data import PriceData
from models.result import TechnicalScore

log = logging.getLogger(__name__)


class TechnicalScorer:

    def score(self, price: Optional[PriceData]) -> TechnicalScore:
        ts = TechnicalScore()
        if price is None or not price.data_complete:
            ts.notes.append("Incomplete price data — technical score zeroed")
            return ts

        ts.monthly_structure = self._monthly(price)
        ts.weekly_structure = self._weekly(price)
        ts.daily_structure = self._daily(price)
        ts.total = ts.monthly_structure + ts.weekly_structure + ts.daily_structure
        return ts

    # ------------------------------------------------------------------ #
    #  Monthly Structure (0–10)
    # ------------------------------------------------------------------ #

    def _monthly(self, price: PriceData) -> float:
        score = 0.0
        notes = []

        if price.monthly is None or len(price.monthly) < 3:
            self._note(notes, "No monthly data")
            return 0.0

        closes = price.monthly["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # 1. Price vs monthly MAs (4 pts)
        ma10 = price.ma_10m
        ma20 = price.ma_20m
        if ma10 and ma20:
            if current > ma10:
                score += 2.0
                notes.append("Price above 10-month MA")
            if current > ma20:
                score += 1.0
                notes.append("Price above 20-month MA")
            if ma10 > ma20:
                score += 1.0
                notes.append("10-month MA above 20-month MA (bullish alignment)")
        elif ma10 and current > ma10:
            score += 2.0

        # 2. Higher-high / higher-low structure over last 12 months (3 pts)
        cutoff_12m = closes.index[-1] - pd.DateOffset(months=12)
        recent_monthly = closes[closes.index >= cutoff_12m]
        if len(recent_monthly) >= 6:
            hh_hl = self._check_hh_hl(recent_monthly)
            if hh_hl >= 0.6:
                score += 3.0
                notes.append("Strong monthly HH/HL structure")
            elif hh_hl >= 0.4:
                score += 1.5
                notes.append("Partial monthly HH/HL structure")

        # 3. Reclaim / continuation quality vs 52-week high (3 pts)
        #
        # We do NOT simply reward proximity to the 52-week high.
        # A broken chart that is "cheap" and near its high is NOT the same
        # as a stock building a healthy base or continuing an uptrend.
        #
        # Logic:
        #   - Reward proximity ONLY when the MA structure is also bullish.
        #   - Reward constructive basing/recovery below highs when HH/HL is present.
        #   - If MAs are broken/bearish, proximity to high gets zero reward (dead-cat proximity).
        #   - Far from highs with broken structure = penalty applied via zero contribution.
        if price.price_52w_high and current:
            pct_from_high = (price.price_52w_high - current) / price.price_52w_high
            ma_bullish = (
                ma10 and ma20 and current > ma10 and ma10 > ma20
            ) if (ma10 and ma20) else (ma10 and current > ma10)
            ma_mixed = (
                ma10 and current > ma10 and (not ma20 or ma10 <= ma20)
            ) if ma10 else False
            hh_hl_val = self._check_hh_hl(recent_monthly) if len(recent_monthly) >= 6 else 0.0

            if pct_from_high <= 0.10:
                # Near highs — only reward if MAs support it
                if ma_bullish:
                    score += 3.0
                    notes.append("Continuation near highs with bullish MA structure")
                elif ma_mixed:
                    score += 1.5
                    notes.append("Near highs but MA structure mixed — caution")
                else:
                    # Near high but MAs broken = likely broken chart
                    score += 0.5
                    notes.append("Near 52w high but MA structure does not support it")
            elif pct_from_high <= 0.20:
                # Pulling back or basing — reward if structure constructive
                if ma_bullish and hh_hl_val >= 0.4:
                    score += 2.5
                    notes.append("Constructive base/pullback within 20% of high")
                elif ma_bullish or (ma_mixed and hh_hl_val >= 0.3):
                    score += 1.5
                    notes.append("Within 20% of high with some structural support")
                else:
                    score += 0.5
            elif pct_from_high <= 0.40:
                # Recovering toward highs — reward if trend is repairing
                if hh_hl_val >= 0.5 and (ma_bullish or ma_mixed):
                    score += 1.5
                    notes.append("Recovering toward highs with improving structure")
                elif hh_hl_val >= 0.3:
                    score += 0.5
                else:
                    # Far from high, structure not yet repaired
                    notes.append(f"Far from 52w high ({pct_from_high:.0%}) — structure not yet repaired")
            else:
                # Very far from highs — no reward unless exceptional recovery
                if hh_hl_val >= 0.65 and ma_bullish:
                    score += 1.0
                    notes.append("Strong recovery structure despite being far from highs")
                else:
                    notes.append(f"Chart too far from highs ({pct_from_high:.0%}) without recovery structure")

        self._extend_notes(self._ms_notes if hasattr(self, "_ms_notes") else [], notes)
        return min(10.0, score)

    # ------------------------------------------------------------------ #
    #  Weekly Structure (0–12)
    # ------------------------------------------------------------------ #

    def _weekly(self, price: PriceData) -> float:
        score = 0.0

        if price.weekly is None or len(price.weekly) < 10:
            return 0.0

        closes = price.weekly["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # Price vs weekly MAs (4 pts)
        ma20w = price.ma_20w
        ma40w = price.ma_40w
        if ma20w and current > ma20w:
            score += 2.0
        if ma40w and current > ma40w:
            score += 1.0
        if ma20w and ma40w and ma20w > ma40w:
            score += 1.0

        # Weekly HH/HL (last 16 weeks) (4 pts)
        cutoff_16w = closes.index[-1] - pd.Timedelta(weeks=16)
        recent = closes[closes.index >= cutoff_16w]
        if len(recent) >= 8:
            hh_hl = self._check_hh_hl(recent)
            if hh_hl >= 0.65:
                score += 4.0
            elif hh_hl >= 0.45:
                score += 2.0
            elif hh_hl >= 0.3:
                score += 1.0

        # Constructive base or continuation (no major breakdown) (4 pts)
        cutoff_8w = closes.index[-1] - pd.Timedelta(weeks=8)
        last_8w = closes[closes.index >= cutoff_8w]
        if len(last_8w) >= 4:
            # Measure whether weekly closes are stable/controlled (not cascading down)
            max_wk = float(last_8w.max())
            min_wk = float(last_8w.min())
            last_close = float(last_8w.iloc[-1])
            if max_wk > 0:
                range_pct = (max_wk - min_wk) / max_wk
                # Tight range = basing behavior
                if range_pct < 0.10 and last_close > min_wk + (max_wk - min_wk) * 0.4:
                    score += 4.0
                elif last_close >= min_wk + (max_wk - min_wk) * 0.5:
                    score += 2.0
                elif last_close >= min_wk + (max_wk - min_wk) * 0.25:
                    score += 1.0

        return min(12.0, score)

    # ------------------------------------------------------------------ #
    #  Daily Structure (0–8)
    # ------------------------------------------------------------------ #

    def _daily(self, price: PriceData) -> float:
        score = 0.0

        if price.daily is None or len(price.daily) < 20:
            return 0.0

        closes = price.daily["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # Price vs 50-day MA (3 pts)
        ma50 = price.ma_50d
        if ma50 and current > ma50:
            score += 3.0

        # 20-day MA vs 50-day MA (2 pts)
        ma20 = price.ma_20d
        if ma20 and ma50:
            if ma20 > ma50:
                score += 2.0
            elif ma20 > ma50 * 0.98:
                score += 1.0

        # Recent pullbacks holding (last 10 days vs 20-day MA) (2 pts)
        if ma20:
            cutoff_10d = closes.index[-1] - pd.Timedelta(days=10)
            last_10 = closes[closes.index >= cutoff_10d]
            if not last_10.empty:
                min_last_10 = float(last_10.min())
                if min_last_10 >= ma20 * 0.97:
                    score += 2.0
                elif min_last_10 >= ma20 * 0.93:
                    score += 1.0

        # Not in heavy distribution (1 pt)
        # Proxy: last 10 days net close-to-close direction
        cutoff_10d_dist = closes.index[-1] - pd.Timedelta(days=10)
        last_10d = closes[closes.index >= cutoff_10d_dist]
        if len(last_10d) >= 5:
            ups = (last_10d.diff().dropna() > 0).sum()
            if ups >= 6:
                score += 1.0
            elif ups >= 4:
                score += 0.5

        return min(8.0, score)

    # ------------------------------------------------------------------ #
    #  Utility
    # ------------------------------------------------------------------ #

    @staticmethod
    def _check_hh_hl(series: pd.Series) -> float:
        """
        Returns a 0–1 ratio of how consistently the series forms HH/HL.
        Uses a simple comparison of consecutive local extremes.
        """
        if len(series) < 4:
            return 0.0
        vals = list(series)
        n = len(vals)
        hh_count = 0
        hl_count = 0
        checks = 0
        # Compare pairs with 2-period stride to capture swing structure
        for i in range(2, n, 2):
            prev_h = max(vals[i-2:i])
            curr_h = max(vals[i:min(i+2, n)])
            prev_l = min(vals[i-2:i])
            curr_l = min(vals[i:min(i+2, n)])
            if curr_h > prev_h:
                hh_count += 1
            if curr_l > prev_l:
                hl_count += 1
            checks += 1
        if checks == 0:
            return 0.0
        return (hh_count + hl_count) / (checks * 2)

    @staticmethod
    def _note(lst, msg):
        lst.append(msg)

    @staticmethod
    def _extend_notes(target, source):
        target.extend(source)
