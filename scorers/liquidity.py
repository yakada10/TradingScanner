"""
Liquidity / Tradability scorer — 10 points total.

  Avg daily dollar volume:  5 pts   (was 4 — increased to reflect its primacy)
  Avg daily share volume:   2 pts
  Spread / quote quality:   2.5 pts (was 2)
  Market cap quality:       0.5 pts (was 2 — demoted to sanity filter, not reward)

Market cap philosophy:
  Market cap is an eligibility floor ($500M min handled by the hard filter), NOT a
  strong positive scoring signal. Large caps are not inherently better trading candidates
  than mid-caps for an active trader. The scoring weight has been deliberately reduced so
  that giant mega-caps don't automatically rank higher than well-moving mid-caps.
  The 0.5pt cap here means market cap contributes a minor stability signal, nothing more.
"""
import logging
from typing import Optional

from models.stock_data import PriceData, ReferenceData
from models.result import LiquidityScore

log = logging.getLogger(__name__)


class LiquidityScorer:

    def score(
        self,
        price: Optional[PriceData],
        reference: Optional[ReferenceData],
    ) -> LiquidityScore:
        ls = LiquidityScore()

        ls.avg_dollar_volume = self._dollar_vol(price)
        ls.avg_share_volume = self._share_vol(price)
        ls.spread_quality = self._spread_quality(price, reference)
        ls.market_cap_quality = self._market_cap(reference)
        ls.total = (
            ls.avg_dollar_volume
            + ls.avg_share_volume
            + ls.spread_quality
            + ls.market_cap_quality
        )
        return ls

    # ------------------------------------------------------------------ #
    #  Avg daily dollar volume (0–5)
    #  Primary liquidity signal — how much money actually trades per day.
    # ------------------------------------------------------------------ #

    def _dollar_vol(self, price: Optional[PriceData]) -> float:
        if price is None:
            return 0.0
        dvol = price.avg_daily_dollar_volume_30d
        if dvol is None:
            return 0.0
        if dvol >= 150_000_000:
            return 5.0
        elif dvol >= 75_000_000:
            return 3.5
        elif dvol >= 40_000_000:
            return 2.5
        elif dvol >= 20_000_000:
            return 1.0
        else:
            return 0.0  # hard fail handled in universe filter

    # ------------------------------------------------------------------ #
    #  Avg daily share volume (0–2)
    # ------------------------------------------------------------------ #

    def _share_vol(self, price: Optional[PriceData]) -> float:
        if price is None:
            return 0.0
        svol = price.avg_daily_volume_30d
        if svol is None:
            return 0.0
        if svol >= 5_000_000:
            return 2.0
        elif svol >= 1_000_000:
            return 1.0
        elif svol >= 500_000:
            return 0.5
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Spread quality proxy (0–2.5)
    #  No live bid/ask — proxy from dollar volume + price level.
    #  High dollar volume + adequate price = tighter spread assumption.
    # ------------------------------------------------------------------ #

    def _spread_quality(
        self,
        price: Optional[PriceData],
        reference: Optional[ReferenceData],
    ) -> float:
        if price is None:
            return 0.0

        dvol = price.avg_daily_dollar_volume_30d or 0
        cur = price.current_price or 0

        if dvol >= 100_000_000 and cur >= 10:
            return 2.5
        elif dvol >= 40_000_000 and cur >= 8:
            return 1.5
        elif dvol >= 20_000_000:
            return 0.5
        else:
            return 0.0

    # ------------------------------------------------------------------ #
    #  Market cap quality (0–0.5)
    #
    #  Deliberately low weight. Market cap acts as a stability sanity check
    #  only. $500M minimum is enforced as a hard filter. Beyond that, being
    #  a $200B mega-cap provides negligible additional trading fitness over
    #  a well-structured $2B mid-cap. We do NOT reward size for its own sake.
    # ------------------------------------------------------------------ #

    def _market_cap(self, reference: Optional[ReferenceData]) -> float:
        if reference is None or reference.market_cap is None:
            return 0.0
        mc = reference.market_cap
        # Only meaningful distinction: confirmed mid/large vs micro-cap boundary
        if mc >= 2_000_000_000:
            return 0.5   # large or mid-cap: minor positive signal
        elif mc >= 500_000_000:
            return 0.25  # small-cap that met the floor: marginal signal
        else:
            return 0.0   # below floor (hard filter should catch this first)
