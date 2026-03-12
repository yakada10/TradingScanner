"""
Shared result serialization for tradetuu.
Used by both the web API and the background worker so they produce identical JSON output.
"""
from typing import Any, Dict


def result_to_dict(result: Any) -> Dict:
    """Serialize a TickerResult object to a JSON-safe dictionary."""
    bd = result.breakdown
    return {
        "ticker":                      result.ticker,
        "company_name":                result.company_name,
        "sector":                      result.sector,
        "industry":                    result.industry,
        "current_price":               result.current_price,
        "market_cap":                  result.market_cap,
        "avg_daily_dollar_volume_30d": result.avg_daily_dollar_volume_30d,
        "next_earnings_date":          str(result.next_earnings_date) if result.next_earnings_date else None,
        "hard_reject_flag":            result.hard_reject_flag,
        "hard_reject_reasons":         result.hard_reject_reasons,
        "final_score":                 round(result.final_score, 1),
        "classification":              result.classification.name,
        "confidence_score":            round(result.confidence_score, 1),
        "low_confidence_warning":      result.low_confidence_warning,
        "breakdown": {
            "technical_trend_score":        round(bd.technical.total, 1),
            "expansion_movement_score":     round(bd.movement.total, 1),
            "reversal_recovery_score":      round(bd.reversal.total, 1),
            "liquidity_tradability_score":  round(bd.liquidity.total, 1),
            "fundamental_stability_score":  round(bd.fundamentals.total, 1),
            "news_event_score":             round(bd.news_event.total, 1),
            "penalties_total":              round(bd.penalties.total, 1),
            "sub_scores": {
                "monthly_structure":        round(bd.technical.monthly_structure, 1),
                "weekly_structure":         round(bd.technical.weekly_structure, 1),
                "daily_structure":          round(bd.technical.daily_structure, 1),
                "atr_adr_relative":         round(bd.movement.atr_adr_relative, 1),
                "daily_expansion":          round(bd.movement.daily_expansion, 1),
                "weekly_expansion":         round(bd.movement.weekly_expansion, 1),
                "volatility_quality":       round(bd.movement.volatility_quality, 1),
                "defended_lows":            round(bd.reversal.defended_lows, 1),
                "higher_lows_forming":      round(bd.reversal.higher_lows_forming, 1),
                "post_earnings_reaction":   round(bd.reversal.post_earnings_reaction, 1),
                "weekly_reversal":          round(bd.reversal.weekly_reversal, 1),
                "avg_dollar_volume":        round(bd.liquidity.avg_dollar_volume, 1),
                "avg_share_volume":         round(bd.liquidity.avg_share_volume, 1),
                "market_cap_quality":       round(bd.liquidity.market_cap_quality, 1),
                "revenue_trend":            round(bd.fundamentals.revenue_trend, 1),
                "earnings_trend":           round(bd.fundamentals.earnings_trend, 1),
                "balance_sheet":            round(bd.fundamentals.balance_sheet, 1),
                "business_durability":      round(bd.fundamentals.business_durability, 1),
                "capital_discipline":       round(bd.fundamentals.capital_discipline, 1),
                "earnings_proximity":       round(bd.news_event.earnings_proximity, 1),
                "earnings_quality":         round(bd.news_event.earnings_quality, 1),
                "news_balance":             round(bd.news_event.news_balance, 1),
                "filing_event_risk":        round(bd.news_event.filing_event_risk, 1),
            },
            "penalty_items": [
                {"reason": p.reason, "points": p.points}
                for p in bd.penalties.items
            ],
        },
        "fit_reasons":       result.fit_reasons,
        "concern_reasons":   result.concern_reasons,
        "summary_paragraph": result.summary_paragraph,
        "supporting_metrics": {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in result.supporting_metrics.items()
            if not isinstance(v, dict)
        },
        "key_news_flags":     result.key_news_flags,
        "key_earnings_flags": result.key_earnings_flags,
    }
