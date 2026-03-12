"""
Expansion / Movement Fitness scorer — 20 points total.
  ATR/ADR relative to price:     8 pts
  Recent daily expansion:        5 pts
  Weekly expansion capacity:     4 pts
  Volatility quality:            3 pts
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from models.stock_data import PriceData
from models.result import MovementScore

log = logging.getLogger(__name__)


class MovementScorer:

    def score(self, price: Optional[PriceData]) -> MovementScore:
        ms = MovementScore()
        if price is None or not price.data_complete:
            ms.notes.append("Incomplete price data — movement score zeroed")
            return ms

        ms.atr_adr_relative = self._atr_adr(price)
        ms.daily_expansion = self._daily_expansion(price)
        ms.weekly_expansion = self._weekly_expansion(price)
        ms.volatility_quality = self._volatility_quality(price)
        ms.total = (
            ms.atr_adr_relative
            + ms.daily_expansion
            + ms.weekly_expansion
            + ms.volatility_quality
        )
        return ms

    # ------------------------------------------------------------------ #
    #  ATR / ADR as % of price (0–8)
    # ------------------------------------------------------------------ #

    def _atr_adr(self, price: PriceData) -> float:
        pct = price.adr_20_pct
        if pct is None and price.atr_14 and price.current_price:
            pct = (price.atr_14 / price.current_price) * 100

        if pct is None:
            return 0.0

        if pct >= 4.0:
            self._n(price, f"ADR/ATR {pct:.1f}% — excellent movement")
            return 8.0
        elif pct >= 3.0:
            self._n(price, f"ADR/ATR {pct:.1f}% — good movement")
            return 6.0
        elif pct >= 2.0:
            self._n(price, f"ADR/ATR {pct:.1f}% — moderate movement")
            return 4.0
        elif pct >= 1.5:
            self._n(price, f"ADR/ATR {pct:.1f}% — below-average movement")
            return 2.0
        else:
            self._n(price, f"ADR/ATR {pct:.1f}% — too slow for this style")
            return 0.0

    # ------------------------------------------------------------------ #
    #  Recent daily expansion behavior (0–5)
    # ------------------------------------------------------------------ #

    def _daily_expansion(self, price: PriceData) -> float:
        if price.daily is None or len(price.daily) < 20:
            return 0.0

        df = price.daily.copy()
        cutoff_30d = df.index[-1] - pd.Timedelta(days=30)
        last_30 = df[df.index >= cutoff_30d]
        if len(last_30) < 10:
            return 0.0

        ranges = last_30["High"] - last_30["Low"]
        median_range = float(ranges.median())
        if median_range == 0:
            return 0.0

        closes = last_30["Close"]
        opens = last_30["Open"]

        # Count expansion days: range > 1.5x median AND close in upper half
        expansion_days = 0
        for i in range(len(last_30)):
            day_range = float(ranges.iloc[i])
            day_low = float(last_30["Low"].iloc[i])
            day_close = float(closes.iloc[i])
            day_high = float(last_30["High"].iloc[i])
            mid = day_low + day_range / 2
            if day_range >= 1.5 * median_range and day_close >= mid:
                expansion_days += 1

        if expansion_days >= 5:
            return 5.0
        elif expansion_days >= 3:
            return 4.0
        elif expansion_days >= 2:
            return 3.0
        elif expansion_days == 1:
            return 1.5
        else:
            return 0.5

    # ------------------------------------------------------------------ #
    #  Weekly expansion capacity (0–4)
    # ------------------------------------------------------------------ #

    def _weekly_expansion(self, price: PriceData) -> float:
        if price.weekly is None or len(price.weekly) < 8:
            return 0.0

        df = price.weekly.copy()
        cutoff_12w = df.index[-1] - pd.Timedelta(weeks=12)
        last_12w = df[df.index >= cutoff_12w]
        if len(last_12w) < 6:
            return 0.0

        ranges = last_12w["High"] - last_12w["Low"]
        closes = last_12w["Close"]
        opens = last_12w["Open"]
        weekly_moves = (closes - opens).abs() / opens.replace(0, np.nan) * 100

        avg_weekly_move_pct = float(weekly_moves.dropna().mean()) if not weekly_moves.dropna().empty else 0
        strong_weeks = int((weekly_moves.dropna() >= 3.0).sum())

        if avg_weekly_move_pct >= 4.0 or strong_weeks >= 4:
            return 4.0
        elif avg_weekly_move_pct >= 2.5 or strong_weeks >= 2:
            return 3.0
        elif avg_weekly_move_pct >= 1.5:
            return 2.0
        else:
            return 1.0

    # ------------------------------------------------------------------ #
    #  Volatility quality (0–3)
    #
    #  Expanded beyond body/range ratio alone. Three sub-signals:
    #    1. Body/range ratio          (1 pt) — directional vs choppy candles
    #    2. Expansion follow-through  (1 pt) — do expansion days continue or reverse?
    #    3. Impulse leg consistency   (1 pt) — clusters of expansion vs isolated spikes
    #
    #  The goal: reward stocks that make real, tradable directional moves.
    #  A high body/ratio alone doesn't distinguish "two big inside days" from
    #  "clean 4-day impulse leg." Follow-through and consistency do.
    # ------------------------------------------------------------------ #

    def _volatility_quality(self, price: PriceData) -> float:
        if price.daily is None or len(price.daily) < 20:
            return 0.0

        cutoff_20d = price.daily.index[-1] - pd.Timedelta(days=20)
        last_20 = price.daily[price.daily.index >= cutoff_20d]

        if len(last_20) < 10:
            return 0.0

        bodies = (last_20["Close"] - last_20["Open"]).abs()
        ranges = last_20["High"] - last_20["Low"]
        closes = last_20["Close"]
        opens = last_20["Open"]

        valid_mask = ranges > 0
        valid_ranges = ranges[valid_mask]
        valid_bodies = bodies[valid_mask]

        if valid_ranges.empty:
            return 0.0

        score = 0.0

        # --- Sub-signal 1: Body/range ratio (0–1 pt) ---
        avg_body_ratio = float((valid_bodies / valid_ranges).mean())
        if avg_body_ratio >= 0.55:
            score += 1.0
        elif avg_body_ratio >= 0.40:
            score += 0.6
        elif avg_body_ratio >= 0.25:
            score += 0.3

        # --- Sub-signal 2: Expansion follow-through (0–1 pt) ---
        # An expansion day is any day with range > 1.5x the 20-day median range.
        # Follow-through = the NEXT day does not fully reverse the move
        # (i.e., the close after an up-expansion day stays above the expansion open,
        #  or the close after a down-expansion day stays below the expansion open).
        median_range = float(valid_ranges.median())
        if median_range > 0 and len(last_20) >= 3:
            followthrough_ok = 0
            expansion_count = 0
            df_arr = last_20.reset_index(drop=True)
            for i in range(len(df_arr) - 1):
                day_range = float(df_arr["High"].iloc[i] - df_arr["Low"].iloc[i])
                if day_range < 1.5 * median_range:
                    continue
                expansion_count += 1
                day_open = float(df_arr["Open"].iloc[i])
                day_close = float(df_arr["Close"].iloc[i])
                next_close = float(df_arr["Close"].iloc[i + 1])
                # Up expansion: next close stays above expansion open
                if day_close > day_open and next_close >= day_open:
                    followthrough_ok += 1
                # Down expansion: next close stays below expansion open
                elif day_close < day_open and next_close <= day_open:
                    followthrough_ok += 1
            if expansion_count >= 2:
                ft_ratio = followthrough_ok / expansion_count
                if ft_ratio >= 0.60:
                    score += 1.0
                elif ft_ratio >= 0.40:
                    score += 0.5

        # --- Sub-signal 3: Impulse leg consistency (0–1 pt) ---
        # Are expansion days clustered (real impulse legs) vs isolated random spikes?
        # Proxy: count consecutive 2+ day runs where both days are above-median range.
        if median_range > 0 and len(last_20) >= 4:
            day_ranges = [float(last_20["High"].iloc[i] - last_20["Low"].iloc[i])
                          for i in range(len(last_20))]
            above_median = [r > median_range for r in day_ranges]
            impulse_runs = 0
            i = 0
            while i < len(above_median) - 1:
                if above_median[i] and above_median[i + 1]:
                    impulse_runs += 1
                    i += 2
                else:
                    i += 1
            if impulse_runs >= 3:
                score += 1.0
            elif impulse_runs >= 1:
                score += 0.5

        return min(3.0, score)

    @staticmethod
    def _n(price, msg):
        pass  # Notes handled by caller summary
