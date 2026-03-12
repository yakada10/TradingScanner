"""
Report Generator — populates fit_reasons, concern_reasons, summary, and supporting_metrics.
Also formats the plain-English terminal output.
"""
import logging
from typing import Optional

from models.stock_data import StockData
from models.result import TickerResult, Classification

log = logging.getLogger(__name__)


class ReportGenerator:

    def generate(self, result: TickerResult, stock: StockData) -> TickerResult:
        self._populate_reasons(result, stock)
        self._populate_metrics(result, stock)
        self._populate_news_flags(result, stock)
        self._populate_earnings_flags(result, stock)
        self._write_summary(result)
        return result

    # ------------------------------------------------------------------ #
    #  Fit / concern reasons
    # ------------------------------------------------------------------ #

    def _populate_reasons(self, result: TickerResult, stock: StockData) -> None:
        bd = result.breakdown
        p = stock.price
        r = stock.reference
        f = stock.fundamentals
        e = stock.earnings
        er = stock.event_risk

        fit = result.fit_reasons
        concern = result.concern_reasons

        # Technical (thresholds calibrated for new 22-pt scale: monthly 6, weekly 10, daily 6)
        if bd.technical.monthly_structure >= 5:
            fit.append("Strong long-term monthly trend structure")
        elif bd.technical.monthly_structure >= 3:
            fit.append("Generally constructive monthly chart")
        elif bd.technical.monthly_structure < 2:
            concern.append("Weak or damaged monthly trend structure — look for reversal signals below")

        if bd.technical.weekly_structure >= 8:
            fit.append("Excellent weekly structure with expansion potential")
        elif bd.technical.weekly_structure >= 5:
            fit.append("Healthy weekly trend")
        elif bd.technical.weekly_structure < 3:
            concern.append("Weak weekly structure")

        if bd.technical.daily_structure >= 5:
            fit.append("Bullish daily chart or constructive pullback in uptrend")
        elif bd.technical.daily_structure < 2.5:
            concern.append("Weak or broken daily structure")

        # Reversal / Recovery opportunity signals
        if bd.reversal.total >= 6:
            fit.append("Strong reversal / recovery opportunity — early-stage turn in progress")
        elif bd.reversal.total >= 3.5:
            fit.append("Reversal / recovery signals present — structure starting to improve")
        elif bd.reversal.total >= 2:
            fit.append("Modest reversal signals — watch for confirmation")
        if bd.reversal.defended_lows >= 2:
            fit.append("Meaningful bullish wick rejections — buyers defending key levels")
        if bd.reversal.higher_lows_forming >= 2.5:
            fit.append("Clear higher lows forming on daily chart — ascending support")
        if bd.reversal.post_earnings_reaction >= 1.5:
            fit.append("Positive earnings reaction with sustained price follow-through")

        # Setup Quality signals (new scorer)
        stage = bd.setup.move_stage_label
        stage_score = bd.setup.move_stage
        if stage_score >= 4.5:
            fit.append(f"Excellent setup timing — {stage} (highest-probability entry zone)")
        elif stage_score >= 3.5:
            fit.append(f"Good setup timing — {stage}")
        elif stage_score >= 2.5:
            fit.append(f"Acceptable setup timing — {stage}")
        elif stage_score <= 1.0:
            concern.append(f"Poor setup timing — {stage} (avoid or wait for better entry)")
        if bd.setup.structure_quality >= 4.0:
            fit.append("Highly organized price action — clean, tradable structure")
        elif bd.setup.structure_quality >= 2.5:
            fit.append("Reasonably clean structure — acceptable for entry")
        elif bd.setup.structure_quality < 1.5:
            concern.append("Choppy / sloppy price action — hard to trade cleanly")
        if bd.setup.room_to_move >= 4.0:
            fit.append("Clear room to expand — minimal near-term overhead resistance")
        elif bd.setup.room_to_move >= 2.5:
            fit.append("Adequate room to move — some overhead but manageable")
        elif bd.setup.room_to_move < 1.5:
            concern.append("Limited room to move — significant overhead resistance nearby")

        # Proximity to 52-week high
        if p and p.price_52w_high and p.current_price:
            pct_from_high = (p.price_52w_high - p.current_price) / p.price_52w_high
            if pct_from_high <= 0.10:
                fit.append(f"Within {pct_from_high:.0%} of 52-week high — strong reclaim potential")
            elif pct_from_high <= 0.20:
                fit.append(f"Within {pct_from_high:.0%} of 52-week high")
            elif pct_from_high > 0.40:
                concern.append(f"Far from 52-week high ({pct_from_high:.0%} below)")

        # Movement
        if p and p.adr_20_pct:
            if p.adr_20_pct >= 3.0:
                fit.append(f"Strong daily movement — ADR {p.adr_20_pct:.1f}% of price")
            elif p.adr_20_pct < 1.5:
                concern.append(f"Low movement for active trading — ADR only {p.adr_20_pct:.1f}%")

        # Liquidity
        if p and p.avg_daily_dollar_volume_30d:
            dvol = p.avg_daily_dollar_volume_30d
            if dvol >= 100_000_000:
                fit.append(f"Excellent liquidity — ${dvol/1e6:.0f}M avg daily dollar volume")
            elif dvol >= 40_000_000:
                fit.append(f"Good liquidity — ${dvol/1e6:.0f}M avg daily dollar volume")
            elif dvol < 25_000_000:
                concern.append(f"Borderline liquidity — ${dvol/1e6:.1f}M avg daily dollar volume")

        # Fundamentals
        if f and f.data_complete:
            stmts = [s for s in f.income_statements if s.revenue is not None]
            if stmts:
                latest = stmts[-1]
                if latest.net_income and latest.net_income > 0:
                    fit.append("Profitable business with positive net income")
                else:
                    concern.append("Not currently profitable")

                if len(stmts) >= 2:
                    rev_latest = stmts[-1].revenue or 0
                    rev_prior = stmts[-2].revenue or 0
                    if rev_prior > 0:
                        yoy = (rev_latest - rev_prior) / rev_prior
                        if yoy > 0.10:
                            fit.append(f"Revenue growing {yoy:.0%} YoY")
                        elif yoy < -0.10:
                            concern.append(f"Revenue declining {abs(yoy):.0%} YoY")
        else:
            concern.append("Limited fundamental data available")

        # Balance sheet
        if f and f.balance_sheets:
            bs = f.balance_sheets[-1]
            if bs.cash_and_equivalents and bs.total_debt:
                if bs.cash_and_equivalents > bs.total_debt:
                    fit.append("Cash exceeds total debt — strong balance sheet")
                elif bs.total_debt > bs.cash_and_equivalents * 3:
                    concern.append("Significant debt relative to cash position")
            if bs.total_equity and bs.total_equity < 0:
                concern.append("Negative shareholders' equity")

        # Event risk
        if er:
            if er.has_recent_offering_30d:
                concern.append("Recent equity offering — near-term dilution pressure")
            if er.has_shelf_registration_180d:
                concern.append("Active shelf registration — potential future dilution")
            if er.share_count_yoy_pct_change and er.share_count_yoy_pct_change > 10:
                concern.append(f"Share count growing {er.share_count_yoy_pct_change:.0f}% YoY")

        # Earnings proximity
        if e and e.trading_days_to_earnings is not None:
            days = e.trading_days_to_earnings
            if 0 <= days <= 3:
                concern.append(f"Earnings in {days} trading day(s) — binary event risk")
            elif days <= 10:
                concern.append(f"Earnings in ~{days} trading days — be aware of risk")
            else:
                fit.append(f"Earnings not imminent ({days}+ trading days away)")

        # Penalties
        for item in bd.penalties.items:
            concern.append(f"Penalty: {item.reason} ({item.points:.0f} pts)")

    # ------------------------------------------------------------------ #
    #  Supporting metrics
    # ------------------------------------------------------------------ #

    def _populate_metrics(self, result: TickerResult, stock: StockData) -> None:
        m = result.supporting_metrics
        p = stock.price
        r = stock.reference
        bd = result.breakdown

        if p:
            m["current_price"] = p.current_price
            m["52w_high"] = p.price_52w_high
            m["52w_low"] = p.price_52w_low
            m["all_time_high"] = p.all_time_high
            m["adr_20d_pct"] = p.adr_20_pct
            m["atr_14"] = p.atr_14
            m["ma_20d"] = p.ma_20d
            m["ma_50d"] = p.ma_50d
            m["ma_20w"] = p.ma_20w
            m["ma_40w"] = p.ma_40w
            m["ma_10m"] = p.ma_10m
            m["ma_20m"] = p.ma_20m
            m["avg_daily_vol_30d"] = p.avg_daily_volume_30d
            m["avg_daily_dollar_vol_30d"] = p.avg_daily_dollar_volume_30d

        m["score_breakdown"] = {
            "technical": round(bd.technical.total, 1),
            "movement": round(bd.movement.total, 1),
            "liquidity": round(bd.liquidity.total, 1),
            "fundamentals": round(bd.fundamentals.total, 1),
            "news_event": round(bd.news_event.total, 1),
            "penalties": round(bd.penalties.total, 1),
            "base_score": round(bd.base_score, 1),
            "final_score": round(bd.final_score, 1),
        }

        if r:
            m["market_cap"] = r.market_cap
            m["shares_outstanding"] = r.shares_outstanding
            m["float_shares"] = r.float_shares

    # ------------------------------------------------------------------ #
    #  News flags
    # ------------------------------------------------------------------ #

    def _populate_news_flags(self, result: TickerResult, stock: StockData) -> None:
        news = stock.news or []
        flags = result.key_news_flags
        for item in news[:10]:
            if item.severity in ("high", "critical"):
                flags.append(
                    f"[{item.severity.upper()}] [{item.direction}] {item.headline[:100]}"
                )

    # ------------------------------------------------------------------ #
    #  Earnings flags
    # ------------------------------------------------------------------ #

    def _populate_earnings_flags(self, result: TickerResult, stock: StockData) -> None:
        e = stock.earnings
        if not e:
            return
        flags = result.key_earnings_flags
        if e.next_earnings_date:
            days = e.trading_days_to_earnings
            flags.append(f"Next earnings: {e.next_earnings_date} (~{days} trading days away)")
        for rec in (e.recent_earnings or [])[:3]:
            if rec.surprise_pct is not None:
                direction = "beat" if rec.surprise_pct >= 0 else "miss"
                flags.append(
                    f"{rec.report_date}: EPS {direction} {rec.surprise_pct:+.1f}%"
                    f" (est: {rec.estimated_eps}, actual: {rec.reported_eps})"
                )

    # ------------------------------------------------------------------ #
    #  Summary paragraph
    # ------------------------------------------------------------------ #

    def _write_summary(self, result: TickerResult) -> None:
        bd = result.breakdown
        ticker = result.ticker
        cls = result.classification.value
        score = result.final_score
        company = result.company_name or ticker
        sector = result.sector or "unknown sector"

        if result.hard_reject_flag:
            reasons_str = "; ".join(result.hard_reject_reasons[:3])
            result.summary_paragraph = (
                f"{company} ({ticker}) is classified as Avoid due to hard structural rejection. "
                f"Key reasons: {reasons_str}. "
                f"This ticker does not meet the minimum quality and risk standards for consideration."
            )
            return

        # Build narrative (calibrated for new scale: tech/20, move/22, rev/8, setup/15, fund/13, news/12)
        tech_q  = "strong"   if bd.technical.total   >= 15 else "moderate" if bd.technical.total   >= 9 else "weak"
        move_q  = "high"     if bd.movement.total    >= 16 else "moderate" if bd.movement.total    >= 10 else "low"
        setup_q = bd.setup.move_stage_label if bd.setup.move_stage_label else "unclear"
        rev_q   = "present"  if bd.reversal.total    >=  3.5 else "absent"
        fund_q  = "solid"    if bd.fundamentals.total >= 10 else "acceptable" if bd.fundamentals.total >= 6 else "weak"

        price_str = f"${result.current_price:.2f}" if result.current_price else "N/A"
        mcap_str = f"${result.market_cap/1e9:.1f}B" if result.market_cap else "N/A"

        penalty_note = ""
        if bd.penalties.total < -5:
            penalty_note = (
                f" Penalty deductions of {bd.penalties.total:.0f} points were applied "
                f"due to: {', '.join(i.reason for i in bd.penalties.items[:2])}."
            )

        result.summary_paragraph = (
            f"{company} ({ticker}) is a {sector} company trading at {price_str} "
            f"with a market cap of {mcap_str}. "
            f"Higher-timeframe technical structure is {tech_q}, "
            f"movement/expansion fitness is {move_q}, "
            f"current move stage is '{setup_q}', "
            f"reversal/recovery opportunity is {rev_q}, "
            f"and fundamental stability is {fund_q}. "
            f"{penalty_note}"
            f"Overall score: {score:.1f}/100 -> Classification: {cls}. "
            f"Confidence: {result.confidence_score:.0f}/100."
        )

    # ------------------------------------------------------------------ #
    #  Terminal output formatting
    # ------------------------------------------------------------------ #

    @staticmethod
    def format_single(result: TickerResult) -> str:
        bd = result.breakdown
        lines = []
        sep = "-" * 70

        lines.append(f"\n{sep}")
        lines.append(
            f"  {result.ticker}  --  {result.classification.value.upper()}  --  "
            f"{result.final_score:.1f}/100"
        )
        if result.company_name:
            lines.append(f"  {result.company_name}  |  {result.sector or 'N/A'}  |  {result.industry or 'N/A'}")
        lines.append(sep)

        if result.hard_reject_flag:
            lines.append("  HARD REJECT:")
            for r in result.hard_reject_reasons:
                lines.append(f"    [X] {r}")
        else:
            lines.append(f"  Score Breakdown:")
            lines.append(f"    Technical Trend:      {bd.technical.total:5.1f} / 20")
            lines.append(f"    Expansion/Movement:   {bd.movement.total:5.1f} / 22")
            lines.append(f"    Reversal/Recovery:    {bd.reversal.total:5.1f} /  8")
            lines.append(f"    Setup Quality:        {bd.setup.total:5.1f} / 15  [{bd.setup.move_stage_label}]")
            lines.append(f"    Liquidity:            {bd.liquidity.total:5.1f} / 10")
            lines.append(f"    Fundamentals:         {bd.fundamentals.total:5.1f} / 13")
            lines.append(f"    News/Earnings/Events: {bd.news_event.total:5.1f} / 12")
            lines.append(f"    Penalties:            {bd.penalties.total:5.1f}")
            lines.append(f"    -------------------------------")
            lines.append(f"    Final Score:          {result.final_score:5.1f} / 100")
            lines.append(f"    Confidence:           {result.confidence_score:5.0f} / 100")
            if result.low_confidence_warning:
                lines.append("    [!] LOW CONFIDENCE -- incomplete data")

        # Key metrics
        m = result.supporting_metrics
        lines.append("")
        lines.append("  Key Metrics:")
        if result.current_price:
            lines.append(f"    Price:         ${result.current_price:.2f}")
        if result.market_cap:
            lines.append(f"    Market Cap:    ${result.market_cap/1e9:.2f}B")
        if result.avg_daily_dollar_volume_30d:
            lines.append(f"    Avg $Vol/Day:  ${result.avg_daily_dollar_volume_30d/1e6:.1f}M")
        if m.get("adr_20d_pct"):
            lines.append(f"    ADR(20):       {m['adr_20d_pct']:.1f}%")
        if m.get("52w_high"):
            lines.append(f"    52W High:      ${m['52w_high']:.2f}")
        if result.next_earnings_date:
            lines.append(f"    Next Earnings: {result.next_earnings_date}")

        # Fit reasons
        if result.fit_reasons:
            lines.append("")
            lines.append("  Why it fits:")
            for r in result.fit_reasons[:5]:
                lines.append(f"    + {r}")

        # Concern reasons
        if result.concern_reasons:
            lines.append("")
            lines.append("  Main concerns:")
            for r in result.concern_reasons[:5]:
                lines.append(f"    - {r}")

        # Penalties
        if bd.penalties.items:
            lines.append("")
            lines.append("  Penalties applied:")
            for item in bd.penalties.items:
                lines.append(f"    [{item.points:.0f}] {item.reason}")

        # News flags
        if result.key_news_flags:
            lines.append("")
            lines.append("  Key news flags:")
            for flag in result.key_news_flags[:4]:
                lines.append(f"    ! {flag}")

        # Earnings flags
        if result.key_earnings_flags:
            lines.append("")
            lines.append("  Earnings context:")
            for flag in result.key_earnings_flags[:4]:
                lines.append(f"    > {flag}")

        # Summary
        lines.append("")
        lines.append("  Summary:")
        lines.append(f"    {result.summary_paragraph}")
        lines.append(sep)

        return "\n".join(lines)

    @staticmethod
    def format_scan_summary(results: list) -> str:
        from models.result import Classification
        lines = []
        sep = "=" * 70

        ideal = [r for r in results if r.classification == Classification.IDEAL_FIT]
        tradable = [r for r in results if r.classification == Classification.TRADABLE]
        watchlist = [r for r in results if r.classification == Classification.WATCHLIST_ONLY]
        avoid = [r for r in results if r.classification == Classification.AVOID]

        lines.append(f"\n{sep}")
        lines.append(f"  STOCK FITNESS SCAN RESULTS")
        lines.append(f"  Evaluated: {len(results)} tickers")
        lines.append(f"  Ideal Fit: {len(ideal)}  |  Tradable: {len(tradable)}  |  "
                     f"Watchlist: {len(watchlist)}  |  Avoid: {len(avoid)}")
        lines.append(sep)

        if ideal:
            lines.append(f"\n  [A] IDEAL FIT ({len(ideal)})")
            for r in ideal:
                lines.append(
                    f"    {r.ticker:<8} {r.final_score:5.1f}/100  "
                    f"{(r.company_name or '')[:30]:<30}  {r.sector or 'N/A'}"
                )

        if tradable:
            lines.append(f"\n  [B] TRADABLE ({len(tradable)})")
            for r in tradable:
                lines.append(
                    f"    {r.ticker:<8} {r.final_score:5.1f}/100  "
                    f"{(r.company_name or '')[:30]:<30}  {r.sector or 'N/A'}"
                )

        if watchlist:
            lines.append(f"\n  [C] WATCHLIST ONLY ({len(watchlist)})")
            for r in watchlist:
                lines.append(
                    f"    {r.ticker:<8} {r.final_score:5.1f}/100  "
                    f"{(r.company_name or '')[:30]:<30}"
                )

        lines.append(f"\n  [D] AVOID ({len(avoid)})")
        for r in avoid[:15]:
            if r.hard_reject_reasons:
                reasons = "; ".join(r.hard_reject_reasons[:2])
                lines.append(f"    {r.ticker:<8} {r.final_score:5.1f}/100  {reasons[:55]}")
            else:
                lines.append(f"    {r.ticker:<8} {r.final_score:5.1f}/100  Low score")
        if len(avoid) > 15:
            lines.append(f"    ... and {len(avoid)-15} more")

        lines.append(f"\n{sep}")
        return "\n".join(lines)
