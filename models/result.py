"""
Output models for scoring results and final classification.

Score distribution (100 pts total):
  Technical Trend Fitness       22 pts  (monthly 6 / weekly 10 / daily 6)
  Expansion / Movement          28 pts  (ADR 10 / daily-exp 7 / weekly-exp 5 / vol-qual 6)
  Reversal / Recovery           10 pts  (defended-lows 3 / higher-lows 3 / post-earnings 2 / weekly-rev 2)
  Liquidity / Tradability       10 pts
  Fundamental Stability         15 pts  (revenue 4 / earnings 3 / balance-sheet 4 / durability 2 / capital 2)
  News / Events                 15 pts
  Penalties overlay           0 to -25 pts
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum


class Classification(str, Enum):
    IDEAL_FIT = "Ideal Fit"
    TRADABLE = "Tradable"
    WATCHLIST_ONLY = "Watchlist Only"
    AVOID = "Avoid"


@dataclass
class TechnicalScore:
    monthly_structure: float = 0.0    # 0-6
    weekly_structure: float = 0.0     # 0-10
    daily_structure: float = 0.0      # 0-6
    total: float = 0.0                # 0-22
    notes: List[str] = field(default_factory=list)


@dataclass
class MovementScore:
    atr_adr_relative: float = 0.0     # 0-10
    daily_expansion: float = 0.0      # 0-7
    weekly_expansion: float = 0.0     # 0-5
    volatility_quality: float = 0.0   # 0-6
    total: float = 0.0                # 0-28
    notes: List[str] = field(default_factory=list)


@dataclass
class ReversalScore:
    """
    Reversal / Recovery Opportunity (0–10 pts).
    Detects early-stage turning points useful for scalp traders even when
    the longer-term chart is not yet fully repaired.
    """
    defended_lows: float = 0.0          # 0-3  — rejection wicks / defended levels
    higher_lows_forming: float = 0.0    # 0-3  — ascending low structure on daily
    post_earnings_reaction: float = 0.0 # 0-2  — beat + held gains / followed through
    weekly_reversal: float = 0.0        # 0-2  — weekly stabilization / reversal structure
    total: float = 0.0                  # 0-10
    notes: List[str] = field(default_factory=list)


@dataclass
class LiquidityScore:
    avg_dollar_volume: float = 0.0    # 0-4
    avg_share_volume: float = 0.0     # 0-2
    spread_quality: float = 0.0       # 0-2
    market_cap_quality: float = 0.0   # 0-2
    total: float = 0.0                # 0-10
    notes: List[str] = field(default_factory=list)


@dataclass
class FundamentalsScore:
    revenue_trend: float = 0.0        # 0-4
    earnings_trend: float = 0.0       # 0-3
    balance_sheet: float = 0.0        # 0-4
    business_durability: float = 0.0  # 0-2
    capital_discipline: float = 0.0   # 0-2
    total: float = 0.0                # 0-15
    notes: List[str] = field(default_factory=list)


@dataclass
class NewsEventScore:
    earnings_proximity: float = 0.0   # 0-3
    earnings_quality: float = 0.0     # 0-4
    news_balance: float = 0.0         # 0-4
    filing_event_risk: float = 0.0    # 0-4
    total: float = 0.0                # 0-15
    notes: List[str] = field(default_factory=list)


@dataclass
class PenaltyItem:
    reason: str
    points: float


@dataclass
class PenaltyResult:
    items: List[PenaltyItem] = field(default_factory=list)
    total: float = 0.0   # negative value, capped at -25
    forces_avoid: bool = False
    force_avoid_reasons: List[str] = field(default_factory=list)


@dataclass
class ScoreBreakdown:
    technical: TechnicalScore = field(default_factory=TechnicalScore)
    movement: MovementScore = field(default_factory=MovementScore)
    reversal: ReversalScore = field(default_factory=ReversalScore)
    liquidity: LiquidityScore = field(default_factory=LiquidityScore)
    fundamentals: FundamentalsScore = field(default_factory=FundamentalsScore)
    news_event: NewsEventScore = field(default_factory=NewsEventScore)
    penalties: PenaltyResult = field(default_factory=PenaltyResult)

    @property
    def base_score(self) -> float:
        return (
            self.technical.total
            + self.movement.total
            + self.reversal.total
            + self.liquidity.total
            + self.fundamentals.total
            + self.news_event.total
        )

    @property
    def final_score(self) -> float:
        raw = self.base_score + self.penalties.total
        return max(0.0, min(100.0, raw))


@dataclass
class TickerResult:
    # Identity
    ticker: str
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    # Key metrics
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    avg_daily_volume_30d: Optional[float] = None
    avg_daily_dollar_volume_30d: Optional[float] = None
    next_earnings_date: Optional[str] = None

    # Hard rejection
    hard_reject_flag: bool = False
    hard_reject_reasons: List[str] = field(default_factory=list)

    # Scores
    breakdown: ScoreBreakdown = field(default_factory=ScoreBreakdown)
    final_score: float = 0.0
    classification: Classification = Classification.AVOID

    # Confidence
    confidence_score: float = 100.0
    low_confidence_warning: bool = False

    # Explanations
    fit_reasons: List[str] = field(default_factory=list)
    concern_reasons: List[str] = field(default_factory=list)
    summary_paragraph: str = ""

    # Supporting data
    supporting_metrics: Dict[str, Any] = field(default_factory=dict)
    key_news_flags: List[str] = field(default_factory=list)
    key_earnings_flags: List[str] = field(default_factory=list)
