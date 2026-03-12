"""
Fundamental Stability scorer — 15 points total.
  Revenue trend:           4 pts  (was 5)
  Earnings/profitability:  3 pts  (was 4)
  Balance sheet health:    4 pts  (was 5)
  Business durability:     2 pts  (was 3)
  Capital discipline:      2 pts  (was 3)

Weight reduction rationale:
  Fundamentals still matter — we are NOT building a pure momentum
  scanner and junk avoidance remains a design goal.  However, the
  old 20 pt weight over-rewarded "investment quality" companies at
  the expense of actual short-term trade opportunity.

  A stock does NOT need to be profitable, high-margin, and debt-free
  to be a good short-term trade.  It needs to be "not junk" — i.e.,
  not a going-concern, not a chronic diluter, not a cash-burn disaster.

  Reducing fundamentals to 15 pts frees up 5 pts that are reallocated
  to Movement (now 28 pts) and the new Reversal scorer (10 pts),
  making the scanner better at surfacing real trading opportunity.
"""
import logging
from typing import Optional, List

import numpy as np

from models.stock_data import (
    FundamentalsData, FinancialStatement, BalanceSheetData, ReferenceData, EventRiskData
)
from models.result import FundamentalsScore

log = logging.getLogger(__name__)


class FundamentalsScorer:

    def score(
        self,
        fundamentals: Optional[FundamentalsData],
        reference: Optional[ReferenceData],
        event_risk: Optional[EventRiskData],
    ) -> FundamentalsScore:
        fs = FundamentalsScore()

        if fundamentals is None or not fundamentals.data_complete:
            fs.notes.append("Incomplete fundamentals — score estimated conservatively")
            # Conservative neutral estimates scaled to new 15-pt total
            fs.revenue_trend      = 1.6   # ~40% of 4 pts
            fs.earnings_trend     = 1.2   # ~40% of 3 pts
            fs.balance_sheet      = 1.6   # ~40% of 4 pts
            fs.business_durability = 1.0  # ~50% of 2 pts
            fs.capital_discipline  = 1.0  # ~50% of 2 pts
            fs.total = sum([
                fs.revenue_trend, fs.earnings_trend, fs.balance_sheet,
                fs.business_durability, fs.capital_discipline
            ])
            return fs

        stmts = sorted(
            [s for s in fundamentals.income_statements if s.revenue is not None],
            key=lambda x: x.period_end or __import__("datetime").date.min
        )
        balance_sheets = sorted(
            [b for b in fundamentals.balance_sheets if b.period_end is not None],
            key=lambda x: x.period_end
        )

        fs.revenue_trend = self._revenue_trend(stmts)
        fs.earnings_trend = self._earnings_trend(stmts)
        fs.balance_sheet = self._balance_sheet(balance_sheets)
        fs.business_durability = self._business_durability(reference, stmts)
        fs.capital_discipline = self._capital_discipline(balance_sheets, event_risk)
        fs.total = (
            fs.revenue_trend + fs.earnings_trend + fs.balance_sheet
            + fs.business_durability + fs.capital_discipline
        )
        return fs

    # ------------------------------------------------------------------ #
    #  Revenue Trend (0–4)
    # ------------------------------------------------------------------ #

    def _revenue_trend(self, stmts: List[FinancialStatement]) -> float:
        if not stmts:
            return 0.0
        revenues = [s.revenue for s in stmts if s.revenue]
        if len(revenues) < 2:
            return 2.0  # insufficient data — neutral

        # YoY changes
        yoy_changes = []
        for i in range(1, len(revenues)):
            if revenues[i-1] and revenues[i-1] > 0:
                yoy_changes.append((revenues[i] - revenues[i-1]) / revenues[i-1])

        if not yoy_changes:
            return 2.0

        latest_yoy = yoy_changes[-1]

        score = 0.0

        # Latest year growth — primary signal
        if latest_yoy > 0.20:
            score += 2.5
        elif latest_yoy > 0.10:
            score += 2.0
        elif latest_yoy > 0.02:
            score += 1.5
        elif latest_yoy > -0.05:
            score += 1.0
        elif latest_yoy > -0.15:
            score += 0.5
        # else: bad decline = 0

        # Multi-year consistency
        positive_years = sum(1 for c in yoy_changes if c > 0)
        ratio = positive_years / len(yoy_changes)
        if ratio >= 0.75:
            score += 1.5
        elif ratio >= 0.50:
            score += 0.75
        elif ratio >= 0.25:
            score += 0.25

        # Trajectory bonus: is growth ACCELERATING? (improving trend even if not perfect)
        # Reward: most recent YoY is higher than prior YoY — direction matters
        if len(yoy_changes) >= 2:
            prev_yoy = yoy_changes[-2]
            if latest_yoy > prev_yoy + 0.05:
                # Meaningfully accelerating
                score += 1.0
            elif latest_yoy > prev_yoy:
                # Modestly improving
                score += 0.5

        return min(4.0, score)

    # ------------------------------------------------------------------ #
    #  Earnings / Profitability Trend (0–3)
    # ------------------------------------------------------------------ #

    def _earnings_trend(self, stmts: List[FinancialStatement]) -> float:
        """
        Rewards profitable companies AND companies on a clear improvement trajectory.
        Shrinking losses meaningfully → earning a real score, not just 0 or 0.5.
        The goal: distinguish "improving real company" from "perpetual cash burner."
        """
        if not stmts:
            return 0.0

        incomes = [s.net_income for s in stmts if s.net_income is not None]
        op_incomes = [s.operating_income for s in stmts if s.operating_income is not None]

        if not incomes:
            return 1.0

        score = 0.0
        latest_income = incomes[-1]

        if latest_income > 0:
            # Currently profitable
            score += 2.0

            # Improving profitability?
            if len(incomes) >= 2 and incomes[-1] > incomes[-2]:
                score += 1.0

            # Long-term profit stability
            if len(incomes) >= 3:
                profitable_years = sum(1 for i in incomes if i > 0)
                if profitable_years == len(incomes):
                    score += 1.0
                elif profitable_years >= len(incomes) * 0.67:
                    score += 0.5
        else:
            # Not yet profitable — reward CLEAR improving trajectory
            if len(incomes) >= 2:
                neg_incomes = [i for i in incomes if i < 0]
                all_losses = len(neg_incomes) == len(incomes)

                if all_losses and len(incomes) >= 3:
                    # Check loss shrinkage trend across all periods
                    # Loss is shrinking if consecutive values are moving toward zero
                    shrinking_count = sum(
                        1 for i in range(1, len(incomes))
                        if incomes[i] > incomes[i-1]  # less negative = improving
                    )
                    shrink_ratio = shrinking_count / (len(incomes) - 1)
                    if shrink_ratio >= 0.75:
                        score += 2.0  # strong consistent loss reduction — real improvement
                    elif shrink_ratio >= 0.5:
                        score += 1.0  # moderate improvement
                    else:
                        score += 0.0  # no consistent improvement

                elif len(incomes) >= 2:
                    # At least check most recent vs prior
                    if incomes[-1] > incomes[-2]:
                        score += 1.0  # improving
                    # If now at near-zero loss or breakeven after prior losses
                    if incomes[-1] > -0.05 * abs(incomes[0]) if incomes[0] else False:
                        score += 0.5  # near breakeven

            # Operating income improvement is a meaningful forward indicator
            if op_incomes and len(op_incomes) >= 2:
                if op_incomes[-1] > op_incomes[-2]:
                    score += 0.5  # operating leverage improving

        return min(3.0, score)

    # ------------------------------------------------------------------ #
    #  Balance Sheet Health (0–4)
    # ------------------------------------------------------------------ #

    def _balance_sheet(self, balance_sheets: List[BalanceSheetData]) -> float:
        """
        Evaluates BOTH the current snapshot AND the trend (improving vs deteriorating).
        A company with a weak-but-improving balance sheet scores better than one with
        a historically strong sheet that is now deteriorating.
        """
        if not balance_sheets:
            return 1.2  # no data — neutral-conservative (scaled from 1.5)

        latest = balance_sheets[-1]
        score = 0.0

        cash = latest.cash_and_equivalents or 0
        debt = latest.total_debt or 0
        equity = latest.total_equity
        cur_assets = latest.current_assets or 0
        cur_liab = latest.current_liabilities or 1  # avoid div/0

        # Snapshot: Cash vs debt
        if cash > debt * 0.5:
            score += 1.5
        elif cash > 0:
            score += 0.5

        # Snapshot: Current ratio
        current_ratio = cur_assets / cur_liab if cur_liab > 0 else None
        if current_ratio:
            if current_ratio >= 2.0:
                score += 1.5
            elif current_ratio >= 1.2:
                score += 1.0
            elif current_ratio >= 1.0:
                score += 0.5

        # Snapshot: Debt/equity
        if equity and equity > 0 and debt is not None:
            de_ratio = debt / equity
            if de_ratio < 0.3:
                score += 1.5
            elif de_ratio < 0.8:
                score += 1.0
            elif de_ratio < 1.5:
                score += 0.5
        elif equity and equity > 0:
            score += 1.0  # positive equity, no debt info

        # Negative equity = major concern
        if equity and equity < 0:
            score = max(0, score - 2.0)

        # Trend: compare latest to prior balance sheet (is it improving or deteriorating?)
        if len(balance_sheets) >= 2:
            prior = balance_sheets[-2]
            improvements = 0

            # Cash trend
            if (latest.cash_and_equivalents or 0) > (prior.cash_and_equivalents or 0):
                improvements += 1

            # Debt trend (lower debt = better)
            if (prior.total_debt or 0) > 0 and (latest.total_debt or 0) < (prior.total_debt or 0):
                improvements += 1

            # Equity trend (growing equity = better)
            if (latest.total_equity or 0) > (prior.total_equity or 0):
                improvements += 1

            # Reward improving balance sheet trajectory
            if improvements >= 2:
                score += 0.5   # clear balance sheet improvement
            elif improvements == 0 and len(balance_sheets) >= 2:
                score -= 0.5   # all metrics deteriorating = small penalty

        return min(4.0, max(0.0, score))

    # ------------------------------------------------------------------ #
    #  Business Durability (0–2)
    # ------------------------------------------------------------------ #

    def _business_durability(
        self,
        reference: Optional[ReferenceData],
        stmts: List[FinancialStatement],
    ) -> float:
        score = 1.0  # baseline for real company with data (scaled from 1.5)

        if reference is None:
            return 0.67

        # Revenue existence
        has_revenue = any(s.revenue and s.revenue > 0 for s in stmts)
        if not has_revenue:
            return 0.0

        # Sector quality heuristic
        sector = (reference.sector or "").lower()
        industry = (reference.industry or "").lower()

        durable_sectors = [
            "technology", "consumer", "industrial", "energy", "financial",
            "healthcare", "real estate", "utilities", "materials", "communication"
        ]
        if any(s in sector for s in durable_sectors):
            score += 0.33

        # Gross margin quality (level)
        margins = [s.gross_margin for s in stmts if s.gross_margin is not None]
        if margins:
            avg_gm = np.mean(margins)
            if avg_gm >= 0.50:
                score += 0.67
            elif avg_gm >= 0.25:
                score += 0.33
            # Bonus: margin improving (even if not great yet)
            if len(margins) >= 2 and margins[-1] > margins[-2] + 0.02:
                score += 0.17  # improving gross margin trajectory

        # Operating margin (level + trend)
        op_margins = [s.operating_margin for s in stmts if s.operating_margin is not None]
        if op_margins:
            if np.mean(op_margins) > 0:
                score += 0.33
            # Bonus: operating margin improving (path to profitability signal)
            if len(op_margins) >= 2 and op_margins[-1] > op_margins[-2]:
                score += 0.17

        return min(2.0, score)

    # ------------------------------------------------------------------ #
    #  Capital Discipline / Share Count (0–2)
    # ------------------------------------------------------------------ #

    def _capital_discipline(
        self,
        balance_sheets: List[BalanceSheetData],
        event_risk: Optional[EventRiskData],
    ) -> float:
        score = 2.0  # start full, deduct (scaled from 3.0)

        if event_risk:
            yoy_change = event_risk.share_count_yoy_pct_change
            if yoy_change is not None:
                if yoy_change > 25:
                    score -= 1.67  # severe dilution
                elif yoy_change > 10:
                    score -= 1.0
                elif yoy_change > 5:
                    score -= 0.33
                elif yoy_change < 0:
                    score = min(2.0, score + 0.33)  # buyback = reward

            if event_risk.has_recent_offering_30d:
                score -= 0.67
            if event_risk.has_shelf_registration_180d:
                score -= 0.33

        # Share count trend from balance sheet
        if balance_sheets and len(balance_sheets) >= 2:
            shares = [b.shares_outstanding for b in balance_sheets if b.shares_outstanding]
            if len(shares) >= 2:
                pct_change = (shares[-1] - shares[0]) / shares[0] * 100 if shares[0] else 0
                if pct_change > 20:
                    score -= 0.67
                elif pct_change > 10:
                    score -= 0.33

        return max(0.0, min(2.0, score))
