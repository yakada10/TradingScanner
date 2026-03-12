"""
Reversal / Recovery Opportunity scorer — 8 points total (capped from 10).

  Defended lows / rejection wicks:  up to 3 pts  (contributes toward 8 cap)
  Higher lows forming:              up to 3 pts
  Post-earnings positive reaction:  up to 2 pts
  Weekly reversal structure:        up to 2 pts
  (sub-scores internally sum toward 10 but total is capped at 8)

Design goal
-----------
Surface early-stage reversal / reclaim setups that matter for short-term
scalp traders even when the longer-term monthly chart is not yet fully
repaired.  This scorer deliberately does NOT require a clean established
uptrend — that is what the Technical Trend scorer rewards.  Instead this
scorer rewards the TURNING-POINT phase:

  - Sellers pushed price down but buyers defended a level (wicks)
  - The stock is making higher lows on the daily chart (structure building)
  - A positive earnings beat was met with genuine follow-through buying
  - The weekly chart is stabilizing / reversing after a prior decline

A stock that scores well here + scores acceptably on Movement and
Liquidity can still be a strong short-term trade candidate even if its
monthly trend is not perfect.

PATH-style example: daily rejection + weekly stabilization + positive
earnings reaction → should score 5–8 here, boosting total score
meaningfully above what the monthly chart alone would allow.

UMAC-style example: strong impulse moves with higher lows and weekly
momentum → should score 6–9 here, consistent with the opportunity
the chart is showing.
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd

from models.stock_data import PriceData, EarningsData
from models.result import ReversalScore

log = logging.getLogger(__name__)


class ReversalScorer:

    def score(
        self,
        price: Optional[PriceData],
        earnings: Optional[EarningsData],
    ) -> ReversalScore:
        rs = ReversalScore()
        if price is None or not price.data_complete:
            rs.notes.append("Incomplete price data — reversal score zeroed")
            return rs

        rs.defended_lows = self._defended_lows(price)
        rs.higher_lows_forming = self._higher_lows(price)
        rs.post_earnings_reaction = self._post_earnings(price, earnings)
        rs.weekly_reversal = self._weekly_reversal(price)
        rs.total = min(8.0, (
            rs.defended_lows
            + rs.higher_lows_forming
            + rs.post_earnings_reaction
            + rs.weekly_reversal
        ))
        return rs

    # ------------------------------------------------------------------ #
    #  Defended Lows / Rejection Wicks (0–3)
    # ------------------------------------------------------------------ #

    def _defended_lows(self, price: PriceData) -> float:
        """
        Detects meaningful bullish rejection wicks in the last 25 trading days.

        A strong lower wick (hammer / doji-hammer) signals that sellers pushed
        price down intraday but buyers stepped in and defended the level,
        closing back near the high of the range.  This is one of the clearest
        short-term reversal / defended-level signals.

        Criteria for a quality rejection wick:
          - Lower wick (low to min(open, close)) > 1.5× the candle body
          - Lower wick > 1.5% of the day's closing price
          - Candle closed above the midpoint of the day's high–low range
        """
        if price.daily is None or len(price.daily) < 10:
            return 0.0

        cutoff = price.daily.index[-1] - pd.Timedelta(days=30)
        last_20 = price.daily[price.daily.index >= cutoff].copy()
        if len(last_20) < 5:
            return 0.0

        quality_wicks = 0
        strong_wicks = 0

        for i in range(len(last_20)):
            high  = float(last_20["High"].iloc[i])
            low   = float(last_20["Low"].iloc[i])
            open_ = float(last_20["Open"].iloc[i])
            close = float(last_20["Close"].iloc[i])

            day_range = high - low
            if day_range <= 0 or close <= 0:
                continue

            body = abs(close - open_)
            body_low = min(open_, close)
            lower_wick = body_low - low

            if lower_wick <= 0:
                continue

            mid = low + day_range / 2
            wick_pct_of_price = lower_wick / close * 100
            wick_vs_body = lower_wick / max(body, close * 0.001)

            # Quality wick: meaningful size, wick bigger than body, close above mid
            if wick_vs_body >= 1.5 and wick_pct_of_price >= 1.5 and close >= mid:
                quality_wicks += 1
                # Strong wick: >3× body OR >3% of price (clear hammer / massive rejection)
                if wick_vs_body >= 3.0 or wick_pct_of_price >= 3.0:
                    strong_wicks += 1

        if strong_wicks >= 2:
            return 3.0
        elif strong_wicks >= 1 and quality_wicks >= 2:
            return 2.5
        elif strong_wicks >= 1:
            return 2.0
        elif quality_wicks >= 3:
            return 2.0
        elif quality_wicks >= 2:
            return 1.5
        elif quality_wicks >= 1:
            return 1.0
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Higher Lows Forming (0–3)
    # ------------------------------------------------------------------ #

    def _higher_lows(self, price: PriceData) -> float:
        """
        Detects whether the stock is forming higher lows on the daily chart.

        Higher lows are one of the earliest and most reliable signals that a
        trend is turning or that a support level is being established.  We
        compare rolling minimum prices across overlapping windows:

          Primary:    last 10 days vs 10–20 days ago
          Secondary:  last 5 days  vs  5–15 days ago

        Both agreements provide high confidence.  Either alone provides
        moderate confidence.  The magnitude of improvement (as % of price)
        determines the exact score.
        """
        if price.daily is None or len(price.daily) < 20:
            return 0.0

        lows = price.daily["Low"]
        if len(lows) < 20:
            return 0.0

        current_price = price.current_price or float(price.daily["Close"].iloc[-1])
        if current_price <= 0:
            return 0.0

        # Primary: last 10d vs prior 10d
        recent_10_low = float(lows.iloc[-10:].min())
        prior_10_low  = float(lows.iloc[-20:-10].min())
        pct_improvement = (recent_10_low - prior_10_low) / current_price * 100

        # Secondary: last 5d vs 5–15d ago
        if len(lows) >= 15:
            recent_5_low = float(lows.iloc[-5:].min())
            prior_5_low  = float(lows.iloc[-15:-5].min())
            secondary_improvement = (recent_5_low - prior_5_low) / current_price * 100
        else:
            secondary_improvement = pct_improvement  # fallback

        primary_hl  = pct_improvement >= 1.0       # at least 1% improvement
        strong_primary = pct_improvement >= 2.5    # clear / obvious higher low
        secondary_hl   = secondary_improvement >= 0.5
        both_hl = primary_hl and secondary_hl

        if strong_primary and both_hl:
            return 3.0
        elif primary_hl and both_hl:
            return 2.5
        elif strong_primary:
            return 2.0
        elif primary_hl:
            return 1.5
        elif pct_improvement > 0 and secondary_improvement > 0:
            return 1.0
        elif pct_improvement > -0.5:
            # Roughly flat / bottoming / stabilizing — partial credit
            return 0.5
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Post-Earnings Positive Reaction (0–2)
    # ------------------------------------------------------------------ #

    def _post_earnings(
        self,
        price: PriceData,
        earnings: Optional[EarningsData],
    ) -> float:
        """
        Detects a positive post-earnings reaction — one of the cleanest
        reversal / recovery catalysts for short-term trading.

        Logic:
          1. Most recent earnings must be a beat (surprise_pct > 0)
          2. Price in the trading days after the earnings date should be UP
             vs the earnings date close.  We locate the earnings day in the
             daily OHLCV data and compare forward vs backward.

        If the report date is not in the price history (very recent), we fall
        back to a magnitude-based estimate using the surprise % alone.
        """
        if earnings is None or not earnings.recent_earnings:
            return 0.0

        if price.daily is None or len(price.daily) < 5:
            return 0.0

        recent = earnings.recent_earnings[0]
        if recent.surprise_pct is None:
            return 0.0

        surprise = recent.surprise_pct

        # Earnings miss — no positive catalyst
        if surprise <= -2.0:
            return 0.0
        # Very slight in-line — not enough to call a catalyst
        if -2.0 < surprise <= 0:
            return 0.0

        # Beat — assess post-earnings price behavior
        report_date = recent.report_date
        closes = price.daily["Close"]

        if report_date:
            try:
                report_dt = pd.Timestamp(report_date)
                dates_on_or_after = closes.index[closes.index >= report_dt]

                if len(dates_on_or_after) < 2:
                    # Report was very recent — use surprise magnitude only
                    if surprise >= 10.0:
                        return 1.5
                    elif surprise >= 5.0:
                        return 1.0
                    else:
                        return 0.5

                earnings_day_close = float(closes[dates_on_or_after[0]])
                current_close = float(closes.iloc[-1])
                post_reaction_pct = (
                    (current_close - earnings_day_close) / earnings_day_close * 100
                ) if earnings_day_close > 0 else 0.0

                if post_reaction_pct >= 5.0 and surprise >= 3.0:
                    return 2.0   # Strong beat + strong follow-through buying
                elif post_reaction_pct >= 2.0:
                    return 1.5   # Beat + held gains
                elif post_reaction_pct >= 0:
                    return 1.0   # Beat + roughly flat (absorbed, not sold off)
                elif post_reaction_pct > -3.0 and surprise >= 5.0:
                    return 0.5   # Strong beat but partial give-back — still noteworthy
                else:
                    return 0.0   # Beat but significant sell-the-news

            except Exception:
                pass  # Fall through to magnitude fallback

        # Fallback: use surprise magnitude alone
        if surprise >= 10.0:
            return 1.5
        elif surprise >= 5.0:
            return 1.0
        elif surprise >= 2.0:
            return 0.5
        return 0.0

    # ------------------------------------------------------------------ #
    #  Weekly Reversal Structure (0–2)
    # ------------------------------------------------------------------ #

    def _weekly_reversal(self, price: PriceData) -> float:
        """
        Detects early weekly reversal / stabilization after a prior decline.

        Unlike the Technical weekly structure score (which rewards established
        uptrends with HH/HL and MAs fully aligned), this score rewards
        TURNING POINTS — a stock that was declining but now shows early
        constructive weekly behavior:

          - Recent 4-week average close higher than prior 4-week average
          - Recent 4-week low higher than prior 4-week low (defended level)
          - Current weekly close in upper half of the recent 4-week range

        This specifically targets the PATH-style setup: weekly starting to
        climb / recover even if the monthly is still in a broader downtrend.
        """
        if price.weekly is None or len(price.weekly) < 6:
            return 0.0

        closes = price.weekly["Close"]
        lows   = price.weekly["Low"]
        highs  = price.weekly["High"]
        n = len(closes)

        if n < 6:
            return 0.0

        # Recent 4 weeks vs prior 4 weeks
        recent_4w_closes = closes.iloc[-4:]
        prior_4w_closes  = closes.iloc[-8:-4] if n >= 8 else closes.iloc[:-4]

        if len(prior_4w_closes) < 2:
            return 0.0

        recent_avg = float(recent_4w_closes.mean())
        prior_avg  = float(prior_4w_closes.mean())

        recent_low = float(lows.iloc[-4:].min())
        prior_low  = float(lows.iloc[-8:-4].min()) if n >= 8 else float(lows.iloc[:-4].min())

        current_close   = float(closes.iloc[-1])
        range_4w_low    = float(lows.iloc[-4:].min())
        range_4w_high   = float(highs.iloc[-4:].max())
        range_4w        = range_4w_high - range_4w_low
        close_in_range  = (
            (current_close - range_4w_low) / range_4w
        ) if range_4w > 0 else 0.5

        prior_avg_safe = prior_avg if prior_avg != 0 else 1.0
        prior_low_safe = prior_low if prior_low != 0 else 1.0

        close_improvement_pct = (recent_avg - prior_avg) / abs(prior_avg_safe) * 100
        low_improvement_pct   = (recent_low - prior_low)  / abs(prior_low_safe)  * 100

        improving_closes      = close_improvement_pct > 0
        strong_improving_cls  = close_improvement_pct >= 2.0
        improving_lows        = low_improvement_pct   > 0
        strong_improving_lows = low_improvement_pct   >= 1.5
        close_upper_half      = close_in_range >= 0.50
        close_upper_third     = close_in_range >= 0.67

        if strong_improving_cls and strong_improving_lows and close_upper_third:
            return 2.0
        elif improving_closes and improving_lows and close_upper_half:
            return 1.5
        elif improving_closes and close_upper_half:
            return 1.0
        elif improving_closes or improving_lows:
            return 0.5
        else:
            return 0.0
