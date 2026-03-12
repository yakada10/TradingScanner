"""
Penalty Engine — applies structured deductions on top of the base score.
Returns a PenaltyResult with itemized deductions and hard-reject flags.

Key design decisions:
  Price floor:
    < $5  => hard reject (in hard filter)
    $5–$8 => soft penalty here (-3 pts) — caution zone
    >= $8 => no price penalty

  Earnings proximity — anti-double-count rule:
    Earnings proximity is SCORED in news_event.py (0-3 pts continuous).
    The classifier also caps classification to Watchlist Only within 3 days.
    To avoid triple-punishing the same event, this engine applies:
      - Within 3 trading days: SMALL supplementary deduction (-1 pt only)
      - 4-5 trading days: minimal deduction (-2 pts)
      - Beyond 5 days: no penalty (handled entirely by the scorer)
    We do NOT stack full score zeroing + classification downgrade + heavy penalty.
"""
import logging
from typing import Optional, List

from models.stock_data import PriceData, ReferenceData, EventRiskData, NewsItem, EarningsData
from models.result import PenaltyResult, PenaltyItem
from config import get_config

log = logging.getLogger(__name__)


class PenaltyEngine:

    def __init__(self):
        self._cfg = get_config()

    def compute(
        self,
        price: Optional[PriceData],
        reference: Optional[ReferenceData],
        event_risk: Optional[EventRiskData],
        earnings: Optional[EarningsData],
        news: Optional[List[NewsItem]],
    ) -> PenaltyResult:
        market_cap = (reference.market_cap or 0) if reference else 0
        result = PenaltyResult()

        # ------------------------------------------------------------------ #
        #  Critical penalties — force Avoid (hard override, not deduction)
        # ------------------------------------------------------------------ #

        if event_risk:
            if event_risk.has_reverse_split_12m:
                result.forces_avoid = True
                result.force_avoid_reasons.append("Reverse split within last 12 months")

            if event_risk.has_active_delisting_warning:
                result.forces_avoid = True
                result.force_avoid_reasons.append("Active delisting warning")

            if event_risk.has_going_concern:
                result.forces_avoid = True
                result.force_avoid_reasons.append("Going concern warning")

            if event_risk.has_bankruptcy_restructuring:
                result.forces_avoid = True
                result.force_avoid_reasons.append("Bankruptcy / restructuring distress")

        # Biotech / clinical-stage exclusion
        if reference and self._cfg.biotech_excluded:
            sector = (reference.sector or "").lower()
            industry = (reference.industry or "").lower()
            description = (reference.description or "").lower()
            biotech_signals = [
                any(ex.lower() in sector for ex in self._cfg.excluded_sectors),
                any(ex.lower() in industry for ex in self._cfg.excluded_industries),
                "clinical" in description and ("trial" in description or "phase" in description),
                "pre-revenue" in description and "drug" in description,
            ]
            if any(biotech_signals):
                result.forces_avoid = True
                result.force_avoid_reasons.append(
                    f"Biotech/clinical-stage pharma excluded (sector: {reference.sector}, industry: {reference.industry})"
                )

        # Price hard fail — below absolute floor
        if price and price.current_price and price.current_price < self._cfg.min_price:
            result.forces_avoid = True
            result.force_avoid_reasons.append(
                f"Price ${price.current_price:.2f} below minimum ${self._cfg.min_price}"
            )

        # ------------------------------------------------------------------ #
        #  Price caution zone ($5–$8) — soft penalty
        #  Stocks above min_price but below min_price_clean pass the filter
        #  but receive a structural penalty for being in the danger zone.
        # ------------------------------------------------------------------ #

        if price and price.current_price:
            cur = price.current_price
            if self._cfg.min_price <= cur < self._cfg.min_price_clean:
                result.items.append(PenaltyItem(
                    f"Price ${cur:.2f} in caution zone ($5–$8)", -3.0
                ))

        # ------------------------------------------------------------------ #
        #  Major penalties
        # ------------------------------------------------------------------ #

        if event_risk:
            if event_risk.has_recent_offering_30d:
                result.items.append(PenaltyItem("Public offering within last 30 days", -12.0))

            if event_risk.has_shelf_registration_180d and not event_risk.has_recent_offering_30d:
                # Large caps (>$5B) routinely file S-3 for employee plans; reduce penalty weight
                shelf_penalty = -2.0 if market_cap >= 5_000_000_000 else -6.0
                result.items.append(PenaltyItem("Shelf registration within last 180 days", shelf_penalty))

            if event_risk.share_count_yoy_pct_change is not None:
                yoy = event_risk.share_count_yoy_pct_change
                if yoy > 25:
                    result.items.append(PenaltyItem(f"Share count dilution {yoy:.0f}% YoY", -8.0))
                elif yoy > 15:
                    result.items.append(PenaltyItem(f"Notable share dilution {yoy:.0f}% YoY", -4.0))

        # Cash burn / runway risk
        if news:
            cash_burn_items = [
                n for n in news
                if "cash burn" in (n.headline or "").lower()
                or "runway" in (n.headline or "").lower()
                or "going concern" in (n.headline or "").lower()
            ]
            if cash_burn_items:
                result.items.append(PenaltyItem("Cash burn / runway concern in news", -6.0))

        # Debt stress
        debt_stress_items = [
            n for n in (news or [])
            if any(kw in (n.headline or "").lower() for kw in
                   ["debt covenant", "credit facility", "refinancing risk", "debt burden"])
        ]
        if debt_stress_items:
            result.items.append(PenaltyItem("Debt stress signals in news", -5.0))

        # Legal / regulatory
        legal_items = [
            n for n in (news or [])
            if n.category == "legal_or_regulatory" and n.severity in ("high", "critical")
        ]
        if legal_items:
            result.items.append(PenaltyItem(
                f"Legal/regulatory overhang ({len(legal_items)} item(s))", -6.0
            ))

        # Promotional noise dominance
        promo_items = [n for n in (news or []) if n.category == "promotional_noise"]
        all_news = news or []
        if promo_items and len(all_news) > 0:
            promo_ratio = len(promo_items) / len(all_news)
            if promo_ratio > 0.5:
                result.items.append(PenaltyItem("News dominated by promotional/hype content", -4.0))

        # ------------------------------------------------------------------ #
        #  Earnings-related penalties — anti-double-count
        #
        #  Earnings proximity is already handled in news_event.py (0–3 pts).
        #  The classifier caps at Watchlist Only within 3 days.
        #  Here we apply only a SMALL supplementary deduction to avoid stacking
        #  multiple large punishments for the same event.
        # ------------------------------------------------------------------ #

        if earnings:
            days = earnings.trading_days_to_earnings
            if days is not None and 0 <= days <= 3:
                # Scorer gave 0pts + classifier downgrade — tiny extra here only
                result.items.append(PenaltyItem(
                    f"Earnings within {days} trading day(s) — binary risk", -1.0
                ))
            elif days is not None and 4 <= days <= self._cfg.earnings_caution_days:
                result.items.append(PenaltyItem(
                    f"Earnings within {days} trading days — caution", -2.0
                ))

            # Poor earnings reaction — separate from proximity
            if earnings.recent_earnings:
                recent = earnings.recent_earnings[0]
                if recent.surprise_pct is not None and recent.surprise_pct < -5.0:
                    result.items.append(PenaltyItem(
                        f"Recent earnings miss ({recent.surprise_pct:.1f}%)", -4.0
                    ))

        # ------------------------------------------------------------------ #
        #  Minor penalties
        # ------------------------------------------------------------------ #

        # Worsening margins
        margin_warn = [
            n for n in (news or [])
            if any(kw in (n.headline or "").lower() for kw in
                   ["margin compression", "gross margin declined", "margin pressure"])
        ]
        if margin_warn:
            result.items.append(PenaltyItem("Margin compression signals", -2.0))

        # Cyclical weakness signals
        cyclical_warn = [
            n for n in (news or [])
            if any(kw in (n.headline or "").lower() for kw in
                   ["cyclical", "downturn", "recession", "demand weakness"])
        ]
        if cyclical_warn:
            result.items.append(PenaltyItem("Cyclical/demand weakness signals", -2.0))

        # ------------------------------------------------------------------ #
        #  Apply cap and total
        # ------------------------------------------------------------------ #

        raw_total = sum(item.points for item in result.items)
        result.total = max(-self._cfg.max_total_penalty, raw_total)  # capped at -25

        return result
