"""
News / Earnings / Event Quality scorer — 15 points total.

  Earnings proximity:      3 pts
  Recent earnings quality: 4 pts
  Event-driven news score: 5 pts  (replaces generic sentiment balance — now event-first)
  Filing / event risk:     3 pts  (reduced from 4 — weight shifted to event news above)

Design philosophy:
  This scorer prioritizes CONCRETE business/capital-market events over generic
  sentiment weighting of headlines. The old approach gave too much weight to
  vague positive/negative sentiment and not enough to actionable events like:
    - offerings, shelf registrations, reverse splits, going concern
    - strong product/business wins
    - strong earnings + strong stock reaction
    - legal/regulatory issues

  Sentiment analysis is kept as a SECONDARY, low-weight signal only.
  Concrete events always outweigh sentiment signals in this category.

Earnings proximity anti-double-count rule:
  Earnings proximity is penalized ONCE here in the score (0-3 pts).
  The penalty engine applies a small ADDITIONAL deduction ONLY for within-3-day
  proximity (a tiny -1pt) since the classifier also downgrade fires there.
  We do NOT apply a full penalty AND a full score zero simultaneously.
"""
import logging
from typing import Optional, List

from models.stock_data import EarningsData, NewsItem, EventRiskData
from models.result import NewsEventScore

log = logging.getLogger(__name__)

# Freshness multiplier for event weighting
_FRESHNESS_MULT = {"0-7d": 1.5, "8-30d": 1.0, "31-90d": 0.4, "90d+": 0.1, "unknown": 0.4}

# High-signal event categories — these are concrete business/capital-market events
_HARD_NEGATIVE_CATEGORIES = {
    "dilution_or_capital_raise",
    "legal_or_regulatory",
    "delisting_or_listing_issue",
    "negative_financial",
}
_HARD_POSITIVE_CATEGORIES = {
    "positive_business",
    "positive_financial",
    "product_or_catalyst",
}
# Low-signal noise categories — near-zero positive effect
_NOISE_CATEGORIES = {"promotional_noise", "neutral"}


class NewsEventScorer:

    def score(
        self,
        earnings: Optional[EarningsData],
        news: Optional[List[NewsItem]],
        event_risk: Optional[EventRiskData],
    ) -> NewsEventScore:
        ns = NewsEventScore()

        ns.earnings_proximity = self._earnings_proximity(earnings)
        ns.earnings_quality = self._earnings_quality(earnings)
        ns.news_balance = self._event_news_score(news, event_risk)
        ns.filing_event_risk = self._filing_event_risk(event_risk, news)
        ns.total = min(12.0, (
            ns.earnings_proximity
            + ns.earnings_quality
            + ns.news_balance
            + ns.filing_event_risk
        ))
        return ns

    # ------------------------------------------------------------------ #
    #  Earnings Proximity (0–3)
    #
    #  This is scored ONCE here. The penalty engine adds only a very small
    #  supplementary deduction for within-3-day proximity to avoid double-
    #  counting. The classifier also downgrade-caps to Watchlist Only within
    #  3 trading days. No further stacking beyond that.
    # ------------------------------------------------------------------ #

    def _earnings_proximity(self, earnings: Optional[EarningsData]) -> float:
        if earnings is None or earnings.next_earnings_date is None:
            return 2.0  # unknown — neutral, not penalized

        days = earnings.trading_days_to_earnings
        if days is None or days < 0:
            return 2.0
        if days > 10:
            return 3.0
        elif days >= 6:
            return 2.0
        elif days >= 4:
            return 1.0
        else:
            return 0.0  # within 3 trading days — classifier handles downgrade

    # ------------------------------------------------------------------ #
    #  Recent Earnings Quality (0–4)
    # ------------------------------------------------------------------ #

    def _earnings_quality(self, earnings: Optional[EarningsData]) -> float:
        if earnings is None or not earnings.recent_earnings:
            return 2.0  # no data — neutral

        records = earnings.recent_earnings[:4]  # last 4 quarters
        beat_count = 0
        miss_count = 0
        surprise_total = 0.0
        surprise_count = 0

        for rec in records:
            sp = rec.surprise_pct
            if sp is not None:
                surprise_total += sp
                surprise_count += 1
                if sp > 0:
                    beat_count += 1
                else:
                    miss_count += 1

        if surprise_count > 0:
            avg_surprise = surprise_total / surprise_count
            beat_ratio = beat_count / surprise_count
            if avg_surprise >= 5.0 and beat_ratio >= 0.75:
                score = 4.0
            elif avg_surprise >= 2.0 and beat_ratio >= 0.5:
                score = 3.0
            elif avg_surprise >= 0:
                score = 2.0
            elif avg_surprise > -5.0:
                score = 1.0
            else:
                score = 0.0
        else:
            # No surprise data — infer from EPS trend
            epses = [r.reported_eps for r in records if r.reported_eps is not None]
            if epses and len(epses) >= 2:
                improving = sum(1 for i in range(1, len(epses)) if epses[i] >= epses[i-1])
                score = 2.0 + (improving / (len(epses) - 1)) * 2.0
            else:
                score = 2.0

        return min(4.0, max(0.0, score))

    # ------------------------------------------------------------------ #
    #  Event-Driven News Score (0–5)
    #
    #  Replaces the old generic sentiment balance.
    #  Concrete capital-market/business events dominate this score.
    #  Generic sentiment is a secondary input only.
    #
    #  Scoring approach:
    #    Start at 2.5 (neutral midpoint for 0-5 range).
    #    Hard negative events pull score down significantly.
    #    Hard positive events provide modest upside.
    #    Sentiment (positive/negative tone) adds a small nudge only.
    #    Promotional noise = near-zero effect.
    # ------------------------------------------------------------------ #

    def _event_news_score(
        self,
        news: Optional[List[NewsItem]],
        event_risk: Optional[EventRiskData],
    ) -> float:
        if not news:
            return 2.5  # no news = neutral

        score = 2.5
        event_adjustment = 0.0
        sentiment_adjustment = 0.0

        for item in news:
            fm = _FRESHNESS_MULT.get(item.freshness_bucket or "unknown", 0.4)
            cat = item.category or "neutral"
            direction = item.direction or "neutral"
            severity = item.severity or "low"

            sev_weight = {"low": 0.5, "medium": 1.0, "high": 1.8, "critical": 3.5}.get(severity, 0.5)

            if cat in _HARD_NEGATIVE_CATEGORIES:
                # Concrete bad event — full weight by severity and freshness
                event_adjustment -= sev_weight * fm

            elif cat in _HARD_POSITIVE_CATEGORIES:
                # Concrete positive event — positive effect but capped (can't cancel structural weakness)
                if severity in ("high", "critical"):
                    event_adjustment += sev_weight * fm * 0.7
                else:
                    # Low/medium positive events get modest credit
                    event_adjustment += sev_weight * fm * 0.4

            elif cat == "sector_headwind":
                event_adjustment -= 0.5 * fm

            elif cat == "sector_tailwind":
                event_adjustment += 0.3 * fm

            elif cat in _NOISE_CATEGORIES:
                pass  # deliberate no-op: promotional noise adds nothing

            else:
                # Generic headline sentiment — very low weight secondary signal
                if direction == "positive":
                    sentiment_adjustment += 0.15 * fm
                elif direction == "negative":
                    sentiment_adjustment -= 0.2 * fm

        # Event signal dominates; sentiment is a small nudge
        score += event_adjustment
        score += sentiment_adjustment * 0.3  # sentiment gets 30% of its raw weight

        return min(5.0, max(0.0, score))

    # ------------------------------------------------------------------ #
    #  Filing / Event Risk (0–3)
    #
    #  Reduced from 4pts (weight shifted to event news score above).
    #  Focuses on hard structural red flags from the event_risk data object:
    #  going concern, reverse split, delisting, recent offerings.
    # ------------------------------------------------------------------ #

    def _filing_event_risk(
        self,
        event_risk: Optional[EventRiskData],
        news: Optional[List[NewsItem]],
    ) -> float:
        if event_risk is None:
            return 1.5  # no data — cautious neutral

        # Hard structural events — collapse to zero
        if event_risk.has_bankruptcy_restructuring:
            return 0.0
        if event_risk.has_going_concern:
            return 0.0
        if event_risk.has_active_delisting_warning:
            return 0.0
        if event_risk.has_reverse_split_12m:
            return 0.0

        score = 3.0  # start at max, deduct

        if event_risk.has_recent_offering_30d:
            score -= 1.5
        if event_risk.has_shelf_registration_180d:
            score -= 0.75

        # Critical or high-severity negative events from news reinforce filing risk deduction
        if news:
            critical_neg = [
                n for n in news
                if n.severity == "critical"
                and n.direction == "negative"
                and n.freshness_bucket in ("0-7d", "8-30d")
            ]
            if critical_neg:
                score -= len(critical_neg) * 1.0

        return max(0.0, min(3.0, score))
