"""
tradetuu — main entry point.

Usage examples:
  python main.py evaluate AAPL
  python main.py evaluate AAPL MSFT NVDA
  python main.py scan --universe sp500 --top 25
  python main.py scan --file my_tickers.txt
  python main.py scan --tickers AAPL,MSFT,NVDA --json
"""
import argparse
import json
import logging
import sys
import os
import io

# Ensure UTF-8 output on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config, AgentConfig
from engine.pipeline import ScoringPipeline
from engine.universe_loader import UniverseLoader
from engine.report_generator import ReportGenerator


def setup_logging(level: str = "INFO", log_file: str = None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
    # Quiet noisy third-party loggers
    for noisy in ["yfinance", "urllib3", "requests", "peewee"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def result_to_dict(result) -> dict:
    """Serialize TickerResult to JSON-safe dict."""
    bd = result.breakdown
    return {
        "ticker": result.ticker,
        "company_name": result.company_name,
        "sector": result.sector,
        "industry": result.industry,
        "current_price": result.current_price,
        "market_cap": result.market_cap,
        "avg_daily_volume_30d": result.avg_daily_volume_30d,
        "avg_daily_dollar_volume_30d": result.avg_daily_dollar_volume_30d,
        "next_earnings_date": result.next_earnings_date,
        "hard_reject_flag": result.hard_reject_flag,
        "hard_reject_reasons": result.hard_reject_reasons,
        "final_score": round(result.final_score, 1),
        "classification": result.classification.value,
        "confidence_score": round(result.confidence_score, 1),
        "low_confidence_warning": result.low_confidence_warning,
        "breakdown": {
            "technical_trend_score": round(bd.technical.total, 1),
            "expansion_movement_score": round(bd.movement.total, 1),
            "liquidity_tradability_score": round(bd.liquidity.total, 1),
            "fundamental_stability_score": round(bd.fundamentals.total, 1),
            "news_event_score": round(bd.news_event.total, 1),
            "penalties_total": round(bd.penalties.total, 1),
            "sub_scores": {
                "monthly_structure": round(bd.technical.monthly_structure, 1),
                "weekly_structure": round(bd.technical.weekly_structure, 1),
                "daily_structure": round(bd.technical.daily_structure, 1),
                "atr_adr_relative": round(bd.movement.atr_adr_relative, 1),
                "daily_expansion": round(bd.movement.daily_expansion, 1),
                "weekly_expansion": round(bd.movement.weekly_expansion, 1),
                "volatility_quality": round(bd.movement.volatility_quality, 1),
                "avg_dollar_volume": round(bd.liquidity.avg_dollar_volume, 1),
                "avg_share_volume": round(bd.liquidity.avg_share_volume, 1),
                "market_cap_quality": round(bd.liquidity.market_cap_quality, 1),
                "revenue_trend": round(bd.fundamentals.revenue_trend, 1),
                "earnings_trend": round(bd.fundamentals.earnings_trend, 1),
                "balance_sheet": round(bd.fundamentals.balance_sheet, 1),
                "business_durability": round(bd.fundamentals.business_durability, 1),
                "capital_discipline": round(bd.fundamentals.capital_discipline, 1),
                "earnings_proximity": round(bd.news_event.earnings_proximity, 1),
                "earnings_quality": round(bd.news_event.earnings_quality, 1),
                "news_balance": round(bd.news_event.news_balance, 1),
                "filing_event_risk": round(bd.news_event.filing_event_risk, 1),
            },
            "penalty_items": [
                {"reason": p.reason, "points": p.points}
                for p in bd.penalties.items
            ],
        },
        "fit_reasons": result.fit_reasons,
        "concern_reasons": result.concern_reasons,
        "summary_paragraph": result.summary_paragraph,
        "supporting_metrics": {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in result.supporting_metrics.items()
            if not isinstance(v, dict)
        },
        "key_news_flags": result.key_news_flags,
        "key_earnings_flags": result.key_earnings_flags,
    }


def cmd_evaluate(args, pipeline: ScoringPipeline):
    reporter = ReportGenerator()
    tickers = args.tickers
    results = []

    for ticker in tickers:
        result = pipeline.evaluate(ticker)
        results.append(result)

        if not args.json:
            print(reporter.format_single(result))

    if args.json:
        output = [result_to_dict(r) for r in results]
        print(json.dumps(output if len(output) > 1 else output[0], indent=2, default=str))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(
                [result_to_dict(r) for r in results],
                f, indent=2, default=str
            )
        print(f"\nResults saved to {args.out}")


def cmd_scan(args, pipeline: ScoringPipeline):
    loader = UniverseLoader()
    reporter = ReportGenerator()

    # Load universe
    if args.universe == "sp500":
        tickers = loader.sp500()
    elif args.universe == "sp500_extended":
        tickers = loader.sp500_extended()
    elif args.file:
        tickers = loader.from_file(args.file)
    elif args.tickers:
        tickers = loader.from_list(args.tickers.split(","))
    else:
        print("Error: specify --universe, --file, or --tickers for scan mode")
        sys.exit(1)

    if not tickers:
        print("Error: no tickers loaded")
        sys.exit(1)

    top_n = args.top or get_config().top_scan_limit
    print(f"\nScanning {len(tickers)} tickers (showing top {top_n})...")

    results = pipeline.scan(tickers)

    if args.json:
        output = [result_to_dict(r) for r in results[:top_n]]
        print(json.dumps(output, indent=2, default=str))
    else:
        print(reporter.format_scan_summary(results))

        if args.verbose:
            from models.result import Classification
            top_results = [
                r for r in results
                if r.classification in (Classification.IDEAL_FIT, Classification.TRADABLE)
            ][:top_n]
            for r in top_results:
                print(reporter.format_single(r))

    if args.out:
        with open(args.out, "w") as f:
            json.dump([result_to_dict(r) for r in results], f, indent=2, default=str)
        print(f"\nFull results saved to {args.out}")


def main():
    parser = argparse.ArgumentParser(
        description="tradetuu — evaluates stocks for short-term trading opportunity",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py evaluate AAPL
  python main.py evaluate AAPL MSFT NVDA
  python main.py evaluate NVDA --json
  python main.py scan --universe sp500 --top 25
  python main.py scan --tickers AAPL,MSFT,NVDA,TSLA
  python main.py scan --file watchlist.txt --verbose --out results.json

Environment variables:
  FINNHUB_API_KEY   Optional Finnhub API key for richer news/earnings data
  FMP_API_KEY       Optional Financial Modeling Prep API key
        """,
    )

    parser.add_argument(
        "--log-level", default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: WARNING)"
    )
    parser.add_argument("--log-file", help="Optional log file path")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--out", help="Save full results to JSON file")
    parser.add_argument(
        "--min-price", type=float, help="Override minimum price filter"
    )
    parser.add_argument(
        "--min-mktcap", type=float, help="Override minimum market cap (in millions)"
    )
    parser.add_argument(
        "--finnhub-key", help="Finnhub API key (or set FINNHUB_API_KEY env var)"
    )

    subparsers = parser.add_subparsers(dest="command")

    # evaluate subcommand
    eval_parser = subparsers.add_parser("evaluate", help="Evaluate one or more specific tickers")
    eval_parser.add_argument("tickers", nargs="+", help="Ticker symbol(s) to evaluate")

    # scan subcommand
    scan_parser = subparsers.add_parser("scan", help="Scan a universe of tickers")
    scan_parser.add_argument(
        "--universe", choices=["sp500", "sp500_extended"],
        help="Predefined universe to scan"
    )
    scan_parser.add_argument("--file", help="Path to file with tickers (one per line)")
    scan_parser.add_argument("--tickers", help="Comma-separated list of tickers")
    scan_parser.add_argument("--top", type=int, help="Number of top results to show")
    scan_parser.add_argument("--verbose", action="store_true", help="Print full reports for top names")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    setup_logging(args.log_level, args.log_file)

    # Apply config overrides
    cfg = get_config()
    if args.min_price:
        cfg.min_price = args.min_price
    if args.min_mktcap:
        cfg.min_market_cap = args.min_mktcap * 1_000_000
    if args.finnhub_key:
        cfg.finnhub_api_key = args.finnhub_key

    pipeline = ScoringPipeline()

    if args.command == "evaluate":
        cmd_evaluate(args, pipeline)
    elif args.command == "scan":
        cmd_scan(args, pipeline)


if __name__ == "__main__":
    main()
