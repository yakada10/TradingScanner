"""
Data models for raw stock data fetched from providers.
All providers normalize their output into these structures.
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import date, datetime
import pandas as pd


@dataclass
class PriceData:
    """OHLCV data and derived price metrics."""
    ticker: str

    # Daily OHLCV - pandas DataFrame with columns: Open, High, Low, Close, Volume
    daily: Optional[pd.DataFrame] = None
    # Weekly OHLCV
    weekly: Optional[pd.DataFrame] = None
    # Monthly OHLCV
    monthly: Optional[pd.DataFrame] = None

    # Derived metrics (computed after fetch)
    current_price: Optional[float] = None
    price_52w_high: Optional[float] = None
    price_52w_low: Optional[float] = None
    all_time_high: Optional[float] = None

    avg_daily_volume_30d: Optional[float] = None
    avg_daily_dollar_volume_30d: Optional[float] = None

    atr_14: Optional[float] = None          # ATR(14) in absolute dollars
    adr_20_pct: Optional[float] = None      # 20-day ADR as % of price

    # Moving averages (daily)
    ma_20d: Optional[float] = None
    ma_50d: Optional[float] = None

    # Moving averages (weekly close-based)
    ma_20w: Optional[float] = None
    ma_40w: Optional[float] = None

    # Moving averages (monthly close-based)
    ma_10m: Optional[float] = None
    ma_20m: Optional[float] = None

    fetch_timestamp: Optional[datetime] = None
    data_complete: bool = False


@dataclass
class ReferenceData:
    """Company/security reference data."""
    ticker: str
    company_name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    float_shares: Optional[float] = None
    security_type: Optional[str] = None   # "Common Stock", "ETF", etc.
    description: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    fetch_timestamp: Optional[datetime] = None
    data_complete: bool = False


@dataclass
class FinancialStatement:
    """Single period financial data."""
    period_end: Optional[date] = None
    period_type: str = "annual"   # "annual" or "quarterly"
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    eps: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None


@dataclass
class BalanceSheetData:
    """Single period balance sheet."""
    period_end: Optional[date] = None
    period_type: str = "annual"
    total_debt: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    total_assets: Optional[float] = None
    total_equity: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    shares_outstanding: Optional[float] = None  # For share count trend


@dataclass
class FundamentalsData:
    """All fundamental / financial statement data."""
    ticker: str
    income_statements: List[FinancialStatement] = field(default_factory=list)
    balance_sheets: List[BalanceSheetData] = field(default_factory=list)
    free_cash_flow_annual: Optional[List[float]] = None  # recent years, descending
    fetch_timestamp: Optional[datetime] = None
    data_complete: bool = False


@dataclass
class EarningsRecord:
    """Single earnings event."""
    period: Optional[str] = None
    reported_eps: Optional[float] = None
    estimated_eps: Optional[float] = None
    surprise_pct: Optional[float] = None
    revenue_actual: Optional[float] = None
    revenue_estimated: Optional[float] = None
    report_date: Optional[date] = None


@dataclass
class EarningsData:
    """Earnings calendar and history."""
    ticker: str
    next_earnings_date: Optional[date] = None
    trading_days_to_earnings: Optional[int] = None
    recent_earnings: List[EarningsRecord] = field(default_factory=list)
    fetch_timestamp: Optional[datetime] = None
    data_complete: bool = False


@dataclass
class NewsItem:
    """Single news item with classification tags."""
    headline: str
    source: Optional[str] = None
    published_at: Optional[datetime] = None
    url: Optional[str] = None
    summary: Optional[str] = None

    # Classification applied by news_service
    category: Optional[str] = None    # see spec section 20
    direction: Optional[str] = None   # positive / negative / mixed / neutral
    severity: Optional[str] = None    # low / medium / high / critical
    freshness_bucket: Optional[str] = None  # "0-7d", "8-30d", "31-90d", "90d+"


@dataclass
class EventRiskData:
    """Structural risk signals from filings and history."""
    ticker: str

    # Hard rejection flags
    has_reverse_split_12m: bool = False
    has_active_delisting_warning: bool = False
    has_going_concern: bool = False
    has_bankruptcy_restructuring: bool = False

    # Soft / major penalty flags
    has_recent_offering_30d: bool = False      # public/secondary offering <30 days
    has_shelf_registration_180d: bool = False  # shelf reg <180 days
    offering_date: Optional[date] = None
    shelf_date: Optional[date] = None

    # Share count YoY change
    share_count_yoy_pct_change: Optional[float] = None  # positive = dilution

    # Reverse split dates (for lookback)
    reverse_split_dates: List[date] = field(default_factory=list)

    # Filing-derived notes (raw strings for context)
    risk_notes: List[str] = field(default_factory=list)

    fetch_timestamp: Optional[datetime] = None
    data_complete: bool = False


@dataclass
class StockData:
    """Aggregated container for all data about one ticker."""
    ticker: str
    reference: Optional[ReferenceData] = None
    price: Optional[PriceData] = None
    fundamentals: Optional[FundamentalsData] = None
    earnings: Optional[EarningsData] = None
    news: Optional[List[NewsItem]] = None
    event_risk: Optional[EventRiskData] = None

    # Metadata
    fetch_errors: List[str] = field(default_factory=list)
    fetch_warnings: List[str] = field(default_factory=list)
