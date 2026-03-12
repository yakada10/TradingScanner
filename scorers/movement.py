"""
Expansion / Movement Fitness scorer — 22 points total.
  ATR/ADR relative to price:     8 pts  (was 10)
  Recent daily expansion:        5 pts  (was  7)
  Weekly expansion capacity:     4 pts  (was  5)
  Volatility quality:            5 pts  (was  6)

Weight adjustment rationale:
  6 pts freed up here fund the new Setup Quality scorer (15 pts), which
  captures the "is the movement actually usable?" dimension more precisely
  via move-stage classification, structure quality, and room-to-move.

  ADR/ATR remains the primary gating signal (8 pts) since raw movement
  capacity is the floor requirement for a scalp trade.  The Setup scorer
  then evaluates whether that movement is organized and timely.

  Volatility quality (5 pts) still rewards stocks with follow-through,
  impulse leg consistency, and directional candles — complementing the
  Setup scorer's structure quality signal.
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

        ms.atr_adr_relative   = self._atr_adr(price)
        ms.daily_expansion    = self._daily_expansion(price)
        ms.weekly_expansion   = self._weekly_expansion(price)
        ms.volatility_quality = self._volatility_quality(price)
        ms.total = (
            ms.atr_adr_relative
            + ms.daily_expansion
            + ms.weekly_expansion
            + ms.volatility_quality
        )
        return ms

    # ------------------------------------------------------------------ #
    #  ATR / ADR as % of price (0–10)
    # ------------------------------------------------------------------ #

    def _atr_adr(self, price: PriceData) -> float:
        """
        Primary movement gating signal for a scalp trader.

        A stock that cannot move ≥2% daily on average simply cannot
        deliver the 2–5% short-term profit targets reliably.

        Score bands (8 pts max, was 10):
          ≥ 6.0%   →  8.0 pts  (exceptional — top tier opportunity)
          ≥ 4.0%   →  6.5 pts  (excellent movement for short-term style)
          ≥ 3.0%   →  5.0 pts  (good — can deliver 2–5% trades)
          ≥ 2.0%   →  3.0 pts  (moderate — workable but tighter targets)
          ≥ 1.5%   →  1.5 pts  (below-average — limited opportunity)
          < 1.5%   →  0.0 pts  (too slow for scalp style)
        """
        pct = price.adr_20_pct
        if pct is None and price.atr_14 and price.current_price:
            pct = (price.atr_14 / price.current_price) * 100

        if pct is None:
            return 0.0

        if pct >= 6.0:
            return 8.0
        elif pct >= 4.0:
            return 6.5
        elif pct >= 3.0:
            return 5.0
        elif pct >= 2.0:
            return 3.0
        elif pct >= 1.5:
            return 1.5
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Recent daily expansion behavior (0–7)
    # ------------------------------------------------------------------ #

    def _daily_expansion(self, price: PriceData) -> float:
        """
        Counts directional expansion days in the last 30 days.

        An expansion day = daily range > 1.5× the 30-day median range
        AND the candle closed in the upper half of its range (bullish bias).

        More expansion days = the stock is actively making large directional
        moves = more opportunities for short-term scalpers.

        Score bands (5 pts max, was 7 pts):
          ≥ 5 expansion days → 5.0 pts
          4 days             → 4.0 pts
          3 days             → 3.0 pts
          2 days             → 2.0 pts
          1 day              → 1.0 pts
          0 days             → 0.0 pts
        """
        if price.daily is None or len(price.daily) < 20:
            return 0.0

        df = price.daily.copy()
        cutoff_30d = df.index[-1] - pd.Timedelta(days=30)
        last_30 = df[df.index >= cutoff_30d]
        if len(last_30) < 10:
            return 0.0

        ranges       = last_30["High"] - last_30["Low"]
        median_range = float(ranges.median())
        if median_range == 0:
            return 0.0

        closes = last_30["Close"]

        expansion_days = 0
        for i in range(len(last_30)):
            day_range = float(ranges.iloc[i])
            day_low   = float(last_30["Low"].iloc[i])
            day_close = float(closes.iloc[i])
            day_high  = float(last_30["High"].iloc[i])
            mid       = day_low + day_range / 2
            if day_range >= 1.5 * median_range and day_close >= mid:
                expansion_days += 1

        if expansion_days >= 5:
            return 5.0
        elif expansion_days == 4:
            return 4.0
        elif expansion_days == 3:
            return 3.0
        elif expansion_days == 2:
            return 2.0
        elif expansion_days == 1:
            return 1.0
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Weekly expansion capacity (0–5)
    # ------------------------------------------------------------------ #

    def _weekly_expansion(self, price: PriceData) -> float:
        """
        Measures multi-week expansion capacity over the last 12 weeks.

        Average weekly O→C move % and count of "strong weeks" (≥3% move)
        together indicate whether the stock makes large weekly swings —
        critical for confirming that daily expansion is part of a sustained
        trend rather than isolated single-day noise.

        Score bands (4 pts max, was 5 pts):
          avg ≥ 4.0% OR strong-weeks ≥ 4 → 4.0 pts
          avg ≥ 2.5% OR strong-weeks ≥ 2 → 3.0 pts
          avg ≥ 1.5%                      → 2.0 pts
          otherwise                       → 1.0 pt
        """
        if price.weekly is None or len(price.weekly) < 8:
            return 0.0

        df = price.weekly.copy()
        cutoff_12w = df.index[-1] - pd.Timedelta(weeks=12)
        last_12w = df[df.index >= cutoff_12w]
        if len(last_12w) < 6:
            return 0.0

        closes = last_12w["Close"]
        opens  = last_12w["Open"]
        weekly_moves = (closes - opens).abs() / opens.replace(0, np.nan) * 100

        avg_weekly_move_pct = (
            float(weekly_moves.dropna().mean()) if not weekly_moves.dropna().empty else 0
        )
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
    #  Volatility quality (0–5)
    #
    #  Three sub-signals:
    #    1. Body/range ratio          (2 pts) — directional vs choppy candles
    #    2. Expansion follow-through  (2 pts) — do expansion days continue?
    #    3. Impulse leg consistency   (1 pt)  — clusters vs isolated spikes
    #
    #  Note: the Setup scorer's Structure Quality sub-score also evaluates
    #  candle organization and gap-and-fade behavior with complementary
    #  logic.  Together they form a complete picture of movement usability.
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

        valid_mask   = ranges > 0
        valid_ranges = ranges[valid_mask]
        valid_bodies = bodies[valid_mask]

        if valid_ranges.empty:
            return 0.0

        score = 0.0

        # --- Sub-signal 1: Body/range ratio (0–2 pts) ---
        avg_body_ratio = float((valid_bodies / valid_ranges).mean())
        if avg_body_ratio >= 0.55:
            score += 2.0
        elif avg_body_ratio >= 0.40:
            score += 1.2
        elif avg_body_ratio >= 0.25:
            score += 0.6

        # --- Sub-signal 2: Expansion follow-through (0–2 pts) ---
        median_range = float(valid_ranges.median())
        if median_range > 0 and len(last_20) >= 3:
            followthrough_ok = 0
            expansion_count  = 0
            df_arr = last_20.reset_index(drop=True)
            for i in range(len(df_arr) - 1):
                day_range = float(df_arr["High"].iloc[i] - df_arr["Low"].iloc[i])
                if day_range < 1.5 * median_range:
                    continue
                expansion_count += 1
                day_open  = float(df_arr["Open"].iloc[i])
                day_close = float(df_arr["Close"].iloc[i])
                next_close = float(df_arr["Close"].iloc[i + 1])
                if day_close > day_open and next_close >= day_open:
                    followthrough_ok += 1
                elif day_close < day_open and next_close <= day_open:
                    followthrough_ok += 1
            if expansion_count >= 2:
                ft_ratio = followthrough_ok / expansion_count
                if ft_ratio >= 0.60:
                    score += 2.0
                elif ft_ratio >= 0.40:
                    score += 1.0

        # --- Sub-signal 3: Impulse leg consistency (0–1 pt) ---
        if median_range > 0 and len(last_20) >= 4:
            day_ranges   = [float(last_20["High"].iloc[i] - last_20["Low"].iloc[i])
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

        return min(5.0, score)
