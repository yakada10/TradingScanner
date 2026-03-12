"""
Main scoring pipeline.
Orchestrates: data fetch → hard filters → scoring → penalties → classification → report.
"""
import logging
from typing import List, Optional

from models.stock_data import StockData
from models.result import ScoreBreakdown, TickerResult, Classification

from providers.market_data import MarketDataProvider
from providers.fundamentals import FundamentalsProvider
from providers.earnings import EarningsProvider
from providers.news import NewsProvider
from providers.event_risk import EventRiskProvider, EventRiskData

from scorers.technical import TechnicalScorer
from scorers.movement import MovementScorer
from scorers.liquidity import LiquidityScorer
from scorers.fundamentals import FundamentalsScorer
from scorers.news_event import NewsEventScorer
from scorers.penalty import PenaltyEngine

from engine.classifier import Classifier
from engine.report_generator import ReportGenerator

from config import get_config

log = logging.getLogger(__name__)


class ScoringPipeline:

    def __init__(self):
        cfg = get_config()
        self._cfg = cfg

        # Providers
        self._market = MarketDataProvider()
        self._fundamentals = FundamentalsProvider()
        self._earnings = EarningsProvider()
        self._news = NewsProvider()
        self._event_risk = EventRiskProvider()

        # Scorers
        self._technical = TechnicalScorer()
        self._movement = MovementScorer()
        self._liquidity = LiquidityScorer()
        self._fundamentals_scorer = FundamentalsScorer()
        self._news_event = NewsEventScorer()
        self._penalty = PenaltyEngine()

        # Post-scoring
        self._classifier = Classifier()
        self._reporter = ReportGenerator()

    # ------------------------------------------------------------------ #
    #  Single ticker
    # ------------------------------------------------------------------ #

    def evaluate(self, ticker: str) -> TickerResult:
        log.info("=== Evaluating %s ===", ticker)
        ticker = ticker.upper().strip()

        # 1. Fetch all data
        stock = self._fetch_all(ticker)

        # 2. Hard universe filter
        reject_reasons = self._hard_filter(stock)
        if reject_reasons and ticker not in self._cfg.manual_override_tickers:
            return self._build_rejected(stock, reject_reasons)

        # 3. Score
        result = self._score(stock)

        log.info(
            "%s => %s | %.1f/100 | confidence %.0f%%",
            ticker,
            result.classification.value,
            result.final_score,
            result.confidence_score,
        )
        return result

    # ------------------------------------------------------------------ #
    #  Multi-ticker scan
    # ------------------------------------------------------------------ #

    def scan(self, tickers: List[str]) -> List[TickerResult]:
        results = []
        total = len(tickers)
        for i, ticker in enumerate(tickers, 1):
            log.info("Progress: %d/%d — %s", i, total, ticker)
            try:
                result = self.evaluate(ticker)
                results.append(result)
            except Exception as exc:
                log.error("Pipeline error for %s: %s", ticker, exc)
                err_result = TickerResult(ticker=ticker)
                err_result.hard_reject_flag = True
                err_result.hard_reject_reasons.append(f"Pipeline error: {exc}")
                err_result.classification = Classification.AVOID
                results.append(err_result)

        # Sort descending by score
        results.sort(key=lambda r: (0 if r.hard_reject_flag else r.final_score), reverse=True)
        return results

    # ------------------------------------------------------------------ #
    #  Data fetch
    # ------------------------------------------------------------------ #

    def _fetch_all(self, ticker: str) -> StockData:
        stock = StockData(ticker=ticker)

        try:
            stock.price = self._market.get_price_data(ticker)
        except Exception as exc:
            stock.fetch_errors.append(f"price: {exc}")

        try:
            stock.reference = self._market.get_reference_data(ticker)
        except Exception as exc:
            stock.fetch_errors.append(f"reference: {exc}")

        try:
            stock.fundamentals = self._fundamentals.get_fundamentals(ticker)
        except Exception as exc:
            stock.fetch_errors.append(f"fundamentals: {exc}")

        try:
            stock.earnings = self._earnings.get_earnings(ticker)
        except Exception as exc:
            stock.fetch_errors.append(f"earnings: {exc}")

        try:
            stock.news = self._news.get_news(ticker)
        except Exception as exc:
            stock.fetch_errors.append(f"news: {exc}")

        # Event risk needs news to be pre-fetched
        try:
            stock.event_risk = self._event_risk.get_event_risk(ticker, stock.news)
        except Exception as exc:
            stock.fetch_errors.append(f"event_risk: {exc}")

        # Compute share count YoY from balance sheets
        if stock.event_risk and stock.fundamentals and stock.fundamentals.balance_sheets:
            EventRiskProvider.compute_share_count_yoy(
                stock.event_risk, stock.fundamentals.balance_sheets
            )

        return stock

    # ------------------------------------------------------------------ #
    #  Hard filter
    # ------------------------------------------------------------------ #

    def _hard_filter(self, stock: StockData) -> List[str]:
        reasons = []
        price = stock.price
        ref = stock.reference

        if price:
            # Hard reject below min_price ($5). Stocks in the $5–$8 caution zone pass
            # here and receive a soft penalty in the penalty engine instead.
            if price.current_price and price.current_price < self._cfg.min_price:
                reasons.append(f"Price ${price.current_price:.2f} < ${self._cfg.min_price} minimum (hard floor)")
            if price.avg_daily_volume_30d and price.avg_daily_volume_30d < self._cfg.min_avg_daily_volume:
                reasons.append(
                    f"Avg daily volume {price.avg_daily_volume_30d:,.0f} < {self._cfg.min_avg_daily_volume:,.0f} minimum"
                )
            if price.avg_daily_dollar_volume_30d and price.avg_daily_dollar_volume_30d < self._cfg.min_avg_dollar_volume:
                reasons.append(
                    f"Avg daily dollar volume ${price.avg_daily_dollar_volume_30d/1e6:.1f}M < ${self._cfg.min_avg_dollar_volume/1e6:.0f}M minimum"
                )
        else:
            reasons.append("No price data available")

        if ref:
            if ref.market_cap and ref.market_cap < self._cfg.min_market_cap:
                reasons.append(f"Market cap ${ref.market_cap/1e6:.0f}M < ${self._cfg.min_market_cap/1e6:.0f}M minimum")

            # Security type filter
            qt = (ref.security_type or "").upper()
            excluded_types = {"ETF", "ETN", "WARRANT", "RIGHT", "FUND", "CLOSED-END FUND"}
            if qt in excluded_types:
                reasons.append(f"Excluded security type: {qt}")

            # Exchange filter (basic)
            exchange = (ref.exchange or "").upper()
            valid_exchanges = {"NYQ", "NMS", "NGM", "NCM", "AMEX", "NYSE", "NASDAQ", "NYSE ARCA"}
            if exchange and exchange not in valid_exchanges:
                reasons.append(f"Non-primary exchange: {exchange}")

        return reasons

    # ------------------------------------------------------------------ #
    #  Score
    # ------------------------------------------------------------------ #

    def _score(self, stock: StockData) -> TickerResult:
        p = stock.price
        r = stock.reference
        f = stock.fundamentals
        e = stock.earnings
        n = stock.news
        er = stock.event_risk

        breakdown = ScoreBreakdown()
        breakdown.technical = self._technical.score(p)
        breakdown.movement = self._movement.score(p)
        breakdown.liquidity = self._liquidity.score(p, r)
        breakdown.fundamentals = self._fundamentals_scorer.score(f, r, er)
        breakdown.news_event = self._news_event.score(e, n, er)
        breakdown.penalties = self._penalty.compute(p, r, er, e, n)

        result = TickerResult(ticker=stock.ticker)
        result.breakdown = breakdown
        result.hard_reject_flag = breakdown.penalties.forces_avoid
        result.hard_reject_reasons = breakdown.penalties.force_avoid_reasons[:]

        result.final_score = breakdown.final_score
        result.classification = self._classifier.classify(result)

        # Populate metadata
        if r:
            result.company_name = r.company_name
            result.sector = r.sector
            result.industry = r.industry
        if p:
            result.current_price = p.current_price
            result.avg_daily_volume_30d = p.avg_daily_volume_30d
            result.avg_daily_dollar_volume_30d = p.avg_daily_dollar_volume_30d
        if r and r.market_cap:
            result.market_cap = r.market_cap
        if e and e.next_earnings_date:
            result.next_earnings_date = str(e.next_earnings_date)

        result.confidence_score = self._compute_confidence(stock)
        result.low_confidence_warning = result.confidence_score < 60

        # Generate explanation
        result = self._reporter.generate(result, stock)

        return result

    # ------------------------------------------------------------------ #
    #  Confidence
    # ------------------------------------------------------------------ #

    def _compute_confidence(self, stock: StockData) -> float:
        score = 100.0
        r = stock.reference
        f = stock.fundamentals
        e = stock.earnings
        n = stock.news
        p = stock.price

        if r is None or not r.sector:
            score -= 10
        if f is None or not f.data_complete:
            score -= 20
        if e is None or e.next_earnings_date is None:
            score -= 10
        if not n:
            score -= 15
        if p is None or p.avg_daily_dollar_volume_30d is None:
            score -= 5
        if stock.fetch_errors:
            score -= min(15, len(stock.fetch_errors) * 5)

        return max(0.0, min(100.0, score))

    # ------------------------------------------------------------------ #
    #  Build rejected result
    # ------------------------------------------------------------------ #

    def _build_rejected(self, stock: StockData, reject_reasons: List[str]) -> TickerResult:
        result = TickerResult(ticker=stock.ticker)
        result.hard_reject_flag = True
        result.hard_reject_reasons = reject_reasons
        result.final_score = 0.0
        result.classification = Classification.AVOID

        if stock.reference:
            result.company_name = stock.reference.company_name
            result.sector = stock.reference.sector
            result.industry = stock.reference.industry
        if stock.price:
            result.current_price = stock.price.current_price
            result.avg_daily_volume_30d = stock.price.avg_daily_volume_30d
            result.avg_daily_dollar_volume_30d = stock.price.avg_daily_dollar_volume_30d

        result.confidence_score = self._compute_confidence(stock)
        result.summary_paragraph = (
            f"{stock.ticker} fails the hard universe filter and is classified as Avoid. "
            f"Reasons: {'; '.join(reject_reasons)}."
        )
        return result
