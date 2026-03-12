"""
Classifier — maps final score + override rules to Classification label.
"""
import logging
from datetime import date

from models.result import TickerResult, Classification
from config import get_config

log = logging.getLogger(__name__)


class Classifier:

    def __init__(self):
        self._cfg = get_config()

    def classify(self, result: TickerResult) -> Classification:
        cfg = self._cfg

        # Hard reject always wins
        if result.hard_reject_flag:
            return Classification.AVOID

        score = result.final_score

        # Base classification from score
        if score >= cfg.ideal_fit_min:
            label = Classification.IDEAL_FIT
        elif score >= cfg.tradable_min:
            label = Classification.TRADABLE
        elif score >= cfg.watchlist_min:
            label = Classification.WATCHLIST_ONLY
        else:
            label = Classification.AVOID

        # ------------------------------------------------------------------ #
        #  Override rules
        # ------------------------------------------------------------------ #

        # If score >= Ideal Fit but earnings within 3 trading days → Watchlist Only
        earnings_days = result.breakdown.news_event.earnings_proximity
        # earnings_proximity score of 0 means earnings within 3 trading days
        if label == Classification.IDEAL_FIT and earnings_days == 0.0:
            log.info(
                "%s: Downgraded Ideal Fit → Watchlist Only (earnings within 3 trading days)",
                result.ticker,
            )
            label = Classification.WATCHLIST_ONLY

        # Critical event risk can downgrade one full band
        if result.breakdown.penalties.forces_avoid:
            label = Classification.AVOID

        # Any critical news item can downgrade one band
        filing_score = result.breakdown.news_event.filing_event_risk
        if filing_score == 0.0 and label == Classification.IDEAL_FIT:
            label = Classification.TRADABLE
        elif filing_score == 0.0 and label == Classification.TRADABLE:
            label = Classification.WATCHLIST_ONLY

        return label
