"""
Admin-configurable parameters for tradetuu.
All thresholds, windows, and feature flags live here.
Edit this file to tune the system globally.
"""
from dataclasses import dataclass, field
from typing import List, Optional
import os


@dataclass
class AgentConfig:
    # ------------------------------------------------------------------ #
    #  Hard Universe Thresholds
    # ------------------------------------------------------------------ #
    # Price floors:
    #   < min_price       => hard reject (too low / likely junk)
    #   min_price to min_price_clean => caution zone — passes filter but gets soft penalty
    #   >= min_price_clean => no price concern
    min_price: float = 5.0              # hard reject below $5
    min_price_clean: float = 8.0        # below $8 triggers a soft penalty (-3 pts)
    min_market_cap: float = 500_000_000   # $500M eligibility floor — filter only, not a scoring reward
    min_avg_daily_volume: float = 500_000         # shares
    min_avg_dollar_volume: float = 20_000_000     # $20M

    # ------------------------------------------------------------------ #
    #  Sector Exclusions
    # ------------------------------------------------------------------ #
    biotech_excluded: bool = True
    excluded_sectors: List[str] = field(default_factory=lambda: [
        "Biotechnology",
        "Pharmaceutical",       # clinical-stage inferred
    ])
    excluded_industries: List[str] = field(default_factory=lambda: [
        "Biotechnology",
        "Drug Manufacturers - Specialty & Generic",
        "Pharmaceutical Retailers",
        "Clinical-stage",
    ])

    # ------------------------------------------------------------------ #
    #  Structural Risk Windows
    # ------------------------------------------------------------------ #
    reverse_split_lookback_days: int = 365
    offering_penalty_window_days: int = 30
    shelf_penalty_window_days: int = 180

    # ------------------------------------------------------------------ #
    #  Earnings Proximity Rules
    # ------------------------------------------------------------------ #
    earnings_caution_days: int = 5       # penalty applied
    earnings_watchlist_days: int = 3     # downgrade to Watchlist Only

    # ------------------------------------------------------------------ #
    #  Penalty Caps
    # ------------------------------------------------------------------ #
    max_total_penalty: float = 25.0

    # ------------------------------------------------------------------ #
    #  Scan Settings
    # ------------------------------------------------------------------ #
    top_scan_limit: int = 25
    news_lookback_days: int = 30
    filings_lookback_days: int = 180

    # ------------------------------------------------------------------ #
    #  Classification Thresholds
    #
    #  Lowered from 85/70/55 to 82/67/52.
    #
    #  Rationale: The scoring rebalance (more weight on Movement/Expansion
    #  and new Reversal scorer, less on Technical monthly and Fundamentals)
    #  means that "clean uptrend + strong balance sheet" mega-caps will
    #  score lower than before.  The thresholds are adjusted so that the
    #  meaningful bands (Ideal Fit = best short-term opportunity, Tradable
    #  = usable setup) remain appropriately selective.
    #
    #  A stock with: excellent movement + early reversal + acceptable
    #  fundamentals + good news context should reach ~75–82 and land in
    #  Tradable–Ideal Fit.  A slow large-cap with perfect fundamentals
    #  but low ADR will score ~55–65 — Watchlist Only or lower.
    # ------------------------------------------------------------------ #
    ideal_fit_min: float = 82.0
    tradable_min: float = 67.0
    watchlist_min: float = 52.0
    # below watchlist_min => Avoid

    # ------------------------------------------------------------------ #
    #  API Keys (populated from env vars or direct assignment)
    # ------------------------------------------------------------------ #
    finnhub_api_key: str = field(
        default_factory=lambda: os.environ.get("FINNHUB_API_KEY", "")
    )
    fmp_api_key: str = field(
        default_factory=lambda: os.environ.get("FMP_API_KEY", "")
    )

    # ------------------------------------------------------------------ #
    #  Cache Settings
    #  DATA_DIR env var controls where the price/fundamentals cache lives.
    #  On Render: set DATA_DIR=/data (mounted persistent disk).
    #  Locally: defaults to <project_root>/data/.cache
    # ------------------------------------------------------------------ #
    cache_dir: str = field(
        default_factory=lambda: os.path.join(
            os.environ.get(
                "DATA_DIR",
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
            ),
            ".cache"
        )
    )
    price_cache_hours: int = 24
    fundamentals_cache_hours: int = 24
    earnings_cache_hours: int = 24
    news_cache_hours: int = 6
    reference_cache_hours: int = 168   # 1 week

    # ------------------------------------------------------------------ #
    #  Logging
    # ------------------------------------------------------------------ #
    log_level: str = "INFO"
    log_file: Optional[str] = None

    # ------------------------------------------------------------------ #
    #  Manual Overrides
    # ------------------------------------------------------------------ #
    # Tickers listed here bypass hard-reject rules (use with caution)
    manual_override_tickers: List[str] = field(default_factory=list)


# Global singleton loaded once per process
_config: Optional[AgentConfig] = None


def get_config() -> AgentConfig:
    global _config
    if _config is None:
        _config = AgentConfig()
    return _config


def set_config(cfg: AgentConfig) -> None:
    global _config
    _config = cfg
