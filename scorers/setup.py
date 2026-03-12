"""
Setup Quality scorer — 15 points total.

  Move Stage Classification:   5 pts
  Setup / Structure Quality:   5 pts
  Room to Move:                5 pts

Design goal
-----------
Answer the question a short-term scalp trader actually asks:
  "Is this stock ACTIONABLE right now for my style?"

A stock can look great on Technical Trend (clean uptrend) and Movement
(high ADR) but still be a poor trade candidate because:
  - It is already too extended from its recent swing low
  - It is sitting right at heavy overhead resistance with nowhere to go
  - The intraday price action is choppy, gappy, and hard to trade
  - The move is in a late / exhausted phase with high reversal risk

This scorer is NOT about whether the stock is a good long-term investment.
It is about whether the CURRENT SETUP is attractive for a 2–5% scalp trade.

Move Stage (0–5 pts):
  Early reversal / fresh breakout:     5 pts  — best timing for entry
  Re-acceleration after consolidation: 4.5 pts — high-probability re-entry
  Early continuation:                  4 pts   — trend intact, not extended
  Mid-trend continuation:              3 pts   — usable but be selective
  Basing / consolidation:              2.5 pts — waiting phase, patience needed
  Mature continuation:                 2 pts   — late stage, reduced reward
  Chop / unclear:                      1 pt    — avoid unless catalyst
  Failed bounce / rolling over:        0 pts   — wrong direction
  Exhausted / overextended:            0 pts   — high reversal risk

Structure Quality (0–5 pts):
  Reward: shallow constructive pullbacks, organized candle direction,
          multi-day momentum runs, limited gap-and-fade behavior.
  Penalize: deep reversion pullbacks, gap-and-fade patterns, choppy
            directionless candles, no sustained momentum runs.

Room to Move (0–5 pts):
  Reward: breaking to new multi-week highs (no overhead supply),
          clean 10–25% pullback from recent high (room to recover),
          no recent failed-breakout resistance zones nearby.
  Penalize: stock immediately below a resistance cluster, very far
            from recent highs (heavy overhead), or at a well-tested
            ceiling that has rejected price multiple times recently.
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from models.stock_data import PriceData
from models.result import SetupScore

log = logging.getLogger(__name__)

# Human-readable labels for move stage (stored in SetupScore.move_stage_label)
_STAGE_LABELS = {
    5.0:  "Early Reversal / Breakout",
    4.5:  "Re-Acceleration",
    4.0:  "Early Continuation",
    3.0:  "Mid-Trend",
    2.5:  "Basing / Consolidation",
    2.0:  "Mature Continuation",
    1.0:  "Chop / Range",
    0.0:  "Exhausted / Failed",
}


class SetupScorer:

    def score(self, price: Optional[PriceData]) -> SetupScore:
        ss = SetupScore()
        if price is None or not price.data_complete:
            ss.notes.append("Incomplete price data — setup score neutral")
            ss.move_stage = 2.5
            ss.structure_quality = 2.5
            ss.room_to_move = 2.5
            ss.total = 7.5
            ss.move_stage_label = "Unknown"
            return ss

        ss.move_stage, ss.move_stage_label = self._move_stage(price)
        ss.structure_quality = self._structure_quality(price)
        ss.room_to_move = self._room_to_move(price)
        ss.total = min(15.0, max(0.0,
            ss.move_stage + ss.structure_quality + ss.room_to_move
        ))
        return ss

    # ------------------------------------------------------------------ #
    #  Move Stage Classification (0–5)
    # ------------------------------------------------------------------ #

    def _move_stage(self, price: PriceData) -> tuple:
        """
        Classifies where the stock is in its move lifecycle.

        Primary inputs (all computed from daily OHLCV):
          ext_20d    — % extension above the 20-day swing low
          trending_up — price > 10d MA and 10d MA > 20d MA (short-term uptrend)
          mom_5d_pct  — momentum: last-5-day avg close vs prior-5-day avg close
          range_state — expanding vs contracting vs neutral daily ranges
          at_ma_cross — 10d MA within 1% of 20d MA (just crossing or just crossed)

        The lifecycle order (best → worst for scalp entry):
          Early Reversal → Re-Acceleration → Early Continuation →
          Mid-Trend → Basing → Mature → Chop → Exhausted/Failed
        """
        if price.daily is None or len(price.daily) < 20:
            return 2.5, "Unknown"

        df = price.daily
        closes = df["Close"]
        lows   = df["Low"]
        highs  = df["High"]
        n = len(df)

        current = price.current_price or float(closes.iloc[-1])
        if current <= 0:
            return 1.0, "Chop / Range"

        # --- Extension from recent swing lows ---
        low_20d = float(lows.iloc[-20:].min())
        low_40d = float(lows.iloc[-40:].min()) if n >= 40 else low_20d

        ext_20d = (current - low_20d) / low_20d * 100 if low_20d > 0 else 0.0
        ext_40d = (current - low_40d) / low_40d * 100 if low_40d > 0 else 0.0

        # --- Short-term trend direction ---
        ma5  = float(closes.iloc[-5:].mean())
        ma10 = float(closes.iloc[-10:].mean()) if n >= 10 else ma5
        ma20 = price.ma_20d or (float(closes.iloc[-20:].mean()) if n >= 20 else ma10)

        trending_up   = (current > ma10) and (ma5 >= ma10) and (current > ma20 * 0.98)
        trending_down = (current < ma10) and (ma5 <= ma10) and (current < ma20 * 1.02)

        # MA proximity: is 10d MA close to 20d MA? (signals recent cross or near-cross)
        at_ma_cross = abs(ma10 - ma20) / ma20 <= 0.015 if ma20 > 0 else False

        # --- Recent momentum (last 5 days vs prior 5 days) ---
        last_5_avg  = float(closes.iloc[-5:].mean())
        prior_5_avg = float(closes.iloc[-10:-5].mean()) if n >= 10 else last_5_avg
        mom_5d_pct  = (last_5_avg - prior_5_avg) / prior_5_avg * 100 if prior_5_avg > 0 else 0.0

        # --- Range expansion vs contraction ---
        range_expanding   = False
        range_contracting = False
        if n >= 15:
            last_5_rng  = float((highs.iloc[-5:] - lows.iloc[-5:]).mean())
            prior_10_rng = float((highs.iloc[-15:-5] - lows.iloc[-15:-5]).mean())
            if prior_10_rng > 0:
                range_expanding   = last_5_rng > prior_10_rng * 1.15
                range_contracting = last_5_rng < prior_10_rng * 0.70

        # ── Classify ──────────────────────────────────────────────────
        # Exhausted / overextended — check first (hard override)
        if ext_20d > 55:
            return 0.0, "Exhausted / Overextended"

        # Failed bounce / rolling over
        if trending_down and mom_5d_pct < -2.5 and ext_40d > 8:
            return 0.0, "Failed Bounce / Rolling Over"

        # Early reversal / fresh breakout from low
        # Stock was near 20-day low, is NOW turning up, 10d/20d MAs just crossing
        if ext_20d <= 12 and trending_up and mom_5d_pct > 0 and (at_ma_cross or ext_40d >= ext_20d * 0.8):
            return 5.0, "Early Reversal / Breakout"

        # Re-acceleration: was in uptrend, had contraction (base), now expanding again
        if ext_40d <= 35 and range_expanding and trending_up and ext_20d <= 22:
            return 4.5, "Re-Acceleration After Base"

        # Early continuation: clear uptrend, not yet extended
        if ext_20d <= 18 and trending_up and mom_5d_pct >= -1:
            return 4.0, "Early Continuation"

        # Basing / consolidation: not trending clearly yet, low extension
        if ext_20d <= 14 and abs(mom_5d_pct) < 3.0 and not trending_down:
            return 2.5, "Basing / Consolidation"

        # Mid-trend: clear uptrend, moderate extension
        if ext_20d <= 30 and trending_up:
            return 3.0, "Mid-Trend Continuation"

        # Mature continuation: extended but trend still up
        if ext_20d <= 55 and trending_up:
            return 2.0, "Mature Continuation"

        # Chop: no clear direction
        if abs(mom_5d_pct) < 2.5 and not trending_up and not trending_down:
            return 1.0, "Chop / Range"

        # Default: unclear / mixed
        return 1.5, "Mixed / Unclear"

    # ------------------------------------------------------------------ #
    #  Setup / Structure Quality (0–5)
    # ------------------------------------------------------------------ #

    def _structure_quality(self, price: PriceData) -> float:
        """
        Measures whether the intraday/day-to-day price action is organized
        and usable, or choppy, gappy, and hard to trade.

        Three sub-signals (each worth up to ~1.5–2 pts):
          1. Pullback depth ratio    — shallow pullbacks = constructive structure
          2. Gap-and-fade detection  — frequent fades after gap-ups = sloppy tape
          3. Candle direction ratio  — organized directional candles vs doji chaos
          4. Multi-day run count     — sustained momentum runs vs single-day spikes
        """
        if price.daily is None or len(price.daily) < 15:
            return 2.5  # neutral

        cutoff = price.daily.index[-1] - pd.Timedelta(days=28)
        last_20 = price.daily[price.daily.index >= cutoff].copy()
        if len(last_20) < 10:
            return 2.5

        closes = last_20["Close"]
        opens  = last_20["Open"]
        highs  = last_20["High"]
        lows   = last_20["Low"]

        score = 0.0

        # --- Sub-signal 1: Pullback depth ratio (0–2 pts) ---
        # Compare average magnitude of up days vs down days.
        # If down days give back a large fraction of what up days build,
        # the price action is "sloppy" and hard to trade cleanly.
        day_changes = closes.pct_change().dropna()
        up_moves   = day_changes[day_changes > 0.002]   # filter micro-moves
        down_moves = day_changes[day_changes < -0.002]

        if not up_moves.empty and not down_moves.empty:
            avg_up   = float(up_moves.mean())
            avg_down = float(abs(down_moves.mean()))
            # Pullback ratio: how much does the average down day give back?
            ratio = avg_down / avg_up if avg_up > 0 else 1.0
            if ratio <= 0.45:
                score += 2.0   # very shallow pullbacks — highly constructive
            elif ratio <= 0.60:
                score += 1.5   # constructive
            elif ratio <= 0.80:
                score += 1.0   # acceptable — some chop
            elif ratio <= 1.00:
                score += 0.5   # roughly symmetric — moderate chop
            # else: down days larger than up days — do not reward
        elif not up_moves.empty:
            score += 1.0  # only up days — unusual, partial credit

        # --- Sub-signal 2: Gap-and-fade detection (penalty) ---
        # A gap-and-fade = open significantly above prior close but close below open.
        # Multiple such days = distribution, weak tape, hard entries.
        gap_fade_count = 0
        arr = last_20.reset_index(drop=True)
        for i in range(1, len(arr)):
            prior_close = float(arr["Close"].iloc[i - 1])
            cur_open    = float(arr["Open"].iloc[i])
            cur_close   = float(arr["Close"].iloc[i])
            if prior_close > 0:
                gap_pct = (cur_open - prior_close) / prior_close * 100
                if gap_pct > 0.75 and cur_close < cur_open * 0.998:
                    gap_fade_count += 1

        if gap_fade_count >= 6:
            score -= 1.5   # persistent gap-and-fade = unreliable tape
        elif gap_fade_count >= 4:
            score -= 1.0
        elif gap_fade_count >= 2:
            score -= 0.5

        # --- Sub-signal 3: Candle organization ratio (0–2 pts) ---
        # What fraction of candles have a meaningful directional body
        # (as opposed to doji / spinning tops that signal indecision)?
        bodies = (closes - opens).abs()
        ranges = highs - lows
        valid  = ranges > 0
        if valid.sum() >= 6:
            body_ratios = (bodies[valid] / ranges[valid])
            # Also reward: proportion of candles where close is in upper 40%+ of range
            close_position = (closes[valid] - lows[valid]) / ranges[valid]
            close_upper_40 = float((close_position >= 0.40).mean())
            avg_body = float(body_ratios.mean())

            if avg_body >= 0.55 and close_upper_40 >= 0.55:
                score += 2.0   # highly organized, directional candles
            elif avg_body >= 0.45 or close_upper_40 >= 0.50:
                score += 1.5   # reasonably organized
            elif avg_body >= 0.35:
                score += 1.0   # moderate — some indecision
            else:
                score += 0.0   # choppy / doji-heavy — poor structure

        # --- Sub-signal 4: Consecutive momentum run count (0–1 pt) ---
        # Multi-day runs where price closes higher multiple days in a row
        # signal organized tape momentum — not just one-day spike behavior.
        multi_day_runs = 0
        run_len = 0
        close_arr = list(closes)
        for i in range(1, len(close_arr)):
            if close_arr[i] > close_arr[i - 1]:
                run_len += 1
            else:
                if run_len >= 2:
                    multi_day_runs += 1
                run_len = 0
        if run_len >= 2:
            multi_day_runs += 1

        if multi_day_runs >= 3:
            score += 1.0   # multiple sustained runs — organized momentum
        elif multi_day_runs >= 2:
            score += 0.5

        return min(5.0, max(0.0, score))

    # ------------------------------------------------------------------ #
    #  Room to Move (0–5)
    # ------------------------------------------------------------------ #

    def _room_to_move(self, price: PriceData) -> float:
        """
        Estimates whether the stock has clear space to expand vs is boxed in
        by overhead resistance or overextension.

        Approach (all price-based proxies from OHLCV):
          1. Position within recent range — near highs vs near low (breakout potential)
          2. Overhead resistance cluster  — count of recent failed-breakout days
             (intraday high above a level but close back below it)
          3. 52-week high context         — how much overhead supply remains
          4. Near-term breakout detection — is stock making new multi-week highs NOW?

        Scoring intuition:
          Breaking to new 40-day highs   → most room (no recent overhead supply)
          10–20% below 40d high, trending → good room (recovery play)
          Right at 20d high (not breaking)→ some resistance — slight caution
          Far below all recent highs      → heavy overhead supply
          Multiple recent failed attempts → resistance wall nearby
        """
        if price.daily is None or len(price.daily) < 20:
            return 2.5

        df = price.daily
        closes = df["Close"]
        highs  = df["High"]
        lows   = df["Low"]
        n = len(df)

        current = price.current_price or float(closes.iloc[-1])
        if current <= 0:
            return 2.5

        high_20d = float(highs.iloc[-20:].max())
        high_40d = float(highs.iloc[-40:].max()) if n >= 40 else high_20d
        low_20d  = float(lows.iloc[-20:].min())

        pct_below_20d_high = (high_20d - current) / high_20d * 100 if high_20d > 0 else 0.0
        pct_below_40d_high = (high_40d - current) / high_40d * 100 if high_40d > 0 else 0.0

        # 52-week context
        pct_below_52w = 0.0
        if price.price_52w_high and price.price_52w_high > 0:
            pct_below_52w = (price.price_52w_high - current) / price.price_52w_high * 100

        # Is stock making new multi-week highs right now?
        at_new_20d_high = current >= high_20d * 0.99
        at_new_40d_high = (current >= high_40d * 0.99) if n >= 40 else at_new_20d_high

        # --- Overhead resistance cluster ---
        # Count sessions in last 40 days where price pushed to an intraday high
        # that is between current price and 12% above it, but closed back below
        # that intraday high (failed to hold the level).  These create supply zones.
        failed_attempts = 0
        resistance_ceiling = current * 1.12
        lookback = min(40, n)
        for i in range(-lookback, -1):
            day_high  = float(highs.iloc[i])
            day_close = float(closes.iloc[i])
            if current < day_high <= resistance_ceiling and day_close < day_high * 0.975:
                failed_attempts += 1

        # --- Score: base from position relative to recent range ---
        score = 0.0

        if at_new_40d_high and n >= 40:
            score += 3.5   # Breaking 40-day highs — maximum room above
        elif at_new_20d_high:
            score += 2.5   # At 20-day highs — moderate room (possible resistance)
        elif pct_below_20d_high <= 8:
            score += 2.0   # Close to 20d high — some resistance
        elif pct_below_20d_high <= 18:
            score += 3.0   # 8–18% below 20d high: room to recover to prior high
        elif pct_below_20d_high <= 30:
            score += 2.0   # 18–30% below: meaningful room but overhead supply
        elif pct_below_20d_high <= 45:
            score += 1.0   # 30–45% below: heavy overhead supply
        else:
            score += 0.0   # Far below: very heavy overhead

        # --- 52-week context bonus/penalty ---
        if pct_below_52w <= 5:
            score += 1.5   # Near 52-week high: upward momentum, less long-term overhead
        elif pct_below_52w <= 15:
            score += 1.0   # Within 15% of 52-week high
        elif pct_below_52w <= 30:
            score += 0.0   # 15–30% below: neutral
        elif pct_below_52w > 50:
            score -= 0.5   # Far from 52-week high: significant structural overhead

        # --- Overhead resistance cluster penalty ---
        if failed_attempts >= 7:
            score -= 2.0   # Dense resistance wall — very hard to push through
        elif failed_attempts >= 5:
            score -= 1.5
        elif failed_attempts >= 3:
            score -= 1.0
        elif failed_attempts >= 2:
            score -= 0.5

        return min(5.0, max(0.0, score))
