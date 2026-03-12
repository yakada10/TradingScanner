"""
Technical Trend Fitness scorer — 20 points total.
  Monthly Structure:   6 pts  (was 10)
  Weekly Structure:    8 pts  (was 10)
  Daily Structure:     6 pts  (was  8)

Weight reduction rationale (for scalp / short-term trading style):
  Monthly now carries 6/22 of technical weight rather than 10/30.
  A stock in a broader monthly downtrend should NOT be automatically
  penalized so heavily that it can't score well for short-term opportunity.
  The Reversal/Recovery scorer (10 pts, separate) picks up turning-point
  signals that the monthly chart alone cannot yet see.

  Weekly is kept highest (10 pts) because it is the key higher-timeframe
  confirmation layer for a scalp trader working the daily / intraday.

  The 52-week-high reclaim section in monthly is intentionally less
  punishing for stocks that are far from highs but showing early structure
  repair — the Reversal scorer's higher-lows and weekly-reversal signals
  complement those situations.
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
        ts.weekly_structure  = self._weekly(price)
        ts.daily_structure   = self._daily(price)
        ts.total = ts.monthly_structure + ts.weekly_structure + ts.daily_structure
        return ts

    # ------------------------------------------------------------------ #
    #  Monthly Structure (0–6)
    # ------------------------------------------------------------------ #

    def _monthly(self, price: PriceData) -> float:
        """
        Monthly structure sub-score (0–6 pts).

        Reduced from 10 pts to 6 pts.  Monthly context still matters —
        it tells us whether the long-term trend is with us or against us —
        but it no longer dominates the technical score.  A stock in a
        monthly downtrend can still show excellent weekly + daily setups
        that are fully tradable at short timeframes.

        Sub-components:
          Price vs monthly MAs:      0–2.5 pts  (was 0–4)
          Monthly HH/HL structure:   0–2.0 pts  (was 0–3)
          52W high proximity/reclaim: 0–1.5 pts  (was 0–3)

        Key change vs prior version:
          Stocks far from their 52-week high are penalized LESS.  The
          "chart too far from highs" situation no longer scores 0 if there
          is any HH/HL improvement at all.  The Reversal scorer picks up
          the near-term signal; the monthly just confirms whether the
          longer-term trend is repairing.
        """
        score = 0.0

        if price.monthly is None or len(price.monthly) < 3:
            return 0.0

        closes  = price.monthly["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # 1. Price vs monthly MAs (0–2.5 pts)
        ma10 = price.ma_10m
        ma20 = price.ma_20m
        if ma10 and ma20:
            if current > ma10:
                score += 1.25
            if current > ma20:
                score += 0.75
            if ma10 > ma20:
                score += 0.5
        elif ma10 and current > ma10:
            score += 1.25

        # 2. Higher-high / higher-low structure over last 12 months (0–2 pts)
        cutoff_12m = closes.index[-1] - pd.DateOffset(months=12)
        recent_monthly = closes[closes.index >= cutoff_12m]
        hh_hl_val = 0.0
        if len(recent_monthly) >= 6:
            hh_hl_val = self._check_hh_hl(recent_monthly)
            if hh_hl_val >= 0.6:
                score += 2.0
            elif hh_hl_val >= 0.4:
                score += 1.0
            elif hh_hl_val >= 0.25:
                score += 0.5   # partial credit: something starting to form

        # 3. 52-week high reclaim / proximity quality (0–1.5 pts)
        #
        # We do NOT simply reward proximity.  A broken chart near its high
        # is NOT the same as a healthy base or continuation.
        #
        # Key change: stocks far from highs now receive partial credit if
        # any HH/HL improvement is present (even if modest).  The reversal
        # scorer handles the near-term specifics; here we just confirm
        # whether the monthly trend is repairing at all.
        if price.price_52w_high and current:
            pct_from_high = (price.price_52w_high - current) / price.price_52w_high
            ma_bullish = (
                ma10 and ma20 and current > ma10 and ma10 > ma20
            ) if (ma10 and ma20) else (ma10 and current > ma10)
            ma_mixed = (
                ma10 and current > ma10 and (not ma20 or ma10 <= ma20)
            ) if ma10 else False

            if pct_from_high <= 0.10:
                # Near highs — only reward if MAs support it
                if ma_bullish:
                    score += 1.5
                elif ma_mixed:
                    score += 0.75
                else:
                    score += 0.25   # near high but MAs broken — likely false signal

            elif pct_from_high <= 0.20:
                # Constructive basing within 20% — reward if structure supports it
                if ma_bullish and hh_hl_val >= 0.4:
                    score += 1.25
                elif ma_bullish or (ma_mixed and hh_hl_val >= 0.3):
                    score += 0.75
                else:
                    score += 0.25

            elif pct_from_high <= 0.40:
                # Recovering — reward if trend is repairing even modestly
                if hh_hl_val >= 0.5 and (ma_bullish or ma_mixed):
                    score += 1.0
                elif hh_hl_val >= 0.3:
                    score += 0.5
                elif hh_hl_val >= 0.15:
                    score += 0.25   # early repair — partial credit

            else:
                # Far from highs — softer treatment than before.
                # A stock does not need to be near its high to be tradable
                # short-term if weekly/daily structure is improving.
                if hh_hl_val >= 0.65 and ma_bullish:
                    score += 1.0   # exceptional monthly recovery despite distance
                elif hh_hl_val >= 0.4:
                    score += 0.5   # meaningful repair in progress
                elif hh_hl_val >= 0.2:
                    score += 0.25  # early signs — give partial credit
                # else 0 — genuinely broken, no structure forming

        return min(6.0, score)

    # ------------------------------------------------------------------ #
    #  Weekly Structure (0–10)
    # ------------------------------------------------------------------ #

    def _weekly(self, price: PriceData) -> float:
        """
        Weekly structure sub-score (0–8 pts).

        Reduced from 10 pts to 8 pts.  The weekly is the key higher-timeframe
        confirmation for a scalp trader.  Freed-up points fund the new Setup
        Quality scorer (15 pts) which directly answers "is this actionable now?"

        Sub-components:
          Price vs weekly MAs:            0–3.5 pts
          Weekly HH/HL (last 16 weeks):   0–3.5 pts
          Constructive base/continuation: 0–3.0 pts
        (total capped at 8)
        """
        score = 0.0

        if price.weekly is None or len(price.weekly) < 10:
            return 0.0

        closes  = price.weekly["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # Price vs weekly MAs (0–3.5 pts)
        ma20w = price.ma_20w
        ma40w = price.ma_40w
        if ma20w and current > ma20w:
            score += 1.75
        if ma40w and current > ma40w:
            score += 1.0
        if ma20w and ma40w and ma20w > ma40w:
            score += 0.75

        # Weekly HH/HL over last 16 weeks (0–3.5 pts)
        cutoff_16w = closes.index[-1] - pd.Timedelta(weeks=16)
        recent = closes[closes.index >= cutoff_16w]
        if len(recent) >= 8:
            hh_hl = self._check_hh_hl(recent)
            if hh_hl >= 0.65:
                score += 3.5
            elif hh_hl >= 0.45:
                score += 1.75
            elif hh_hl >= 0.3:
                score += 1.0
            elif hh_hl >= 0.15:
                score += 0.5   # something starting to form — partial credit

        # Constructive base or continuation — last 8 weeks (0–3 pts)
        cutoff_8w = closes.index[-1] - pd.Timedelta(weeks=8)
        last_8w = closes[closes.index >= cutoff_8w]
        if len(last_8w) >= 4:
            max_wk    = float(last_8w.max())
            min_wk    = float(last_8w.min())
            last_close = float(last_8w.iloc[-1])
            if max_wk > 0:
                range_pct = (max_wk - min_wk) / max_wk
                pos_in_range = (last_close - min_wk) / (max_wk - min_wk) if max_wk != min_wk else 0.5
                # Tight range = basing behavior
                if range_pct < 0.10 and pos_in_range >= 0.4:
                    score += 3.0
                elif pos_in_range >= 0.5:
                    score += 1.75
                elif pos_in_range >= 0.25:
                    score += 1.0

        return min(8.0, score)

    # ------------------------------------------------------------------ #
    #  Daily Structure (0–6)
    # ------------------------------------------------------------------ #

    def _daily(self, price: PriceData) -> float:
        """
        Daily structure sub-score (0–6 pts).

        Reduced from 8 pts to 6 pts.  The daily chart confirms the
        short-term setup but is less decisive than the weekly for a
        trader using multi-timeframe confirmation.

        Sub-components:
          Price vs 50-day MA:             0–2.5 pts  (was 0–3)
          20-day MA vs 50-day MA:         0–1.5 pts  (was 0–2)
          Recent pullbacks holding:       0–1.5 pts  (was 0–2)
          Not in heavy distribution:      0–0.5 pts  (was 0–1)
        """
        score = 0.0

        if price.daily is None or len(price.daily) < 20:
            return 0.0

        closes  = price.daily["Close"]
        current = price.current_price or float(closes.iloc[-1])

        # Price vs 50-day MA (0–2.5 pts)
        ma50 = price.ma_50d
        if ma50 and current > ma50:
            score += 2.5

        # 20-day MA vs 50-day MA alignment (0–1.5 pts)
        ma20 = price.ma_20d
        if ma20 and ma50:
            if ma20 > ma50:
                score += 1.5
            elif ma20 > ma50 * 0.98:
                score += 0.75

        # Recent pullbacks holding above 20-day MA (0–1.5 pts)
        if ma20:
            cutoff_10d = closes.index[-1] - pd.Timedelta(days=10)
            last_10 = closes[closes.index >= cutoff_10d]
            if not last_10.empty:
                min_last_10 = float(last_10.min())
                if min_last_10 >= ma20 * 0.97:
                    score += 1.5
                elif min_last_10 >= ma20 * 0.93:
                    score += 0.75

        # Not in heavy distribution (0–0.5 pts)
        # Proxy: net close-to-close direction last 10 days
        cutoff_10d_dist = closes.index[-1] - pd.Timedelta(days=10)
        last_10d = closes[closes.index >= cutoff_10d_dist]
        if len(last_10d) >= 5:
            ups = (last_10d.diff().dropna() > 0).sum()
            if ups >= 6:
                score += 0.5
            elif ups >= 4:
                score += 0.25

        return min(6.0, score)

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
