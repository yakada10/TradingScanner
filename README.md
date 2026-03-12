# Stock Fitness Agent

Evaluates whether a stock is a good fit for an active trading style focused on:
- Strong higher-timeframe bullish structure (monthly / weekly / daily)
- Enough daily and weekly expansion / movement to be worth trading
- Adequate liquidity and dollar volume
- Business stability and fundamental durability
- Low structural risk (no dilution traps, reverse splits, biotech binary events)

This is a **stock fitness evaluator**, not a signal bot. It does not generate entries, exits, or stop-losses.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Evaluate a single ticker
python main.py evaluate AAPL

# 3. Evaluate multiple tickers
python main.py evaluate NVDA MSFT TSLA

# 4. Scan S&P 500 and show top 25
python main.py scan --universe sp500 --top 25

# 5. Scan a custom watchlist
python main.py scan --file my_watchlist.txt --verbose

# 6. Get JSON output
python main.py evaluate AAPL --json

# 7. Save full scan results to file
python main.py scan --tickers AAPL,MSFT,NVDA,TSLA,META --out results.json
```

---

## API Keys (Optional but Recommended)

### Finnhub (Free tier — richer news + earnings calendar)
1. Register at [finnhub.io](https://finnhub.io)
2. Set your key:
   ```bash
   # Windows
   set FINNHUB_API_KEY=your_key_here

   # Mac/Linux
   export FINNHUB_API_KEY=your_key_here
   ```
   Or pass via CLI: `python main.py evaluate AAPL --finnhub-key your_key_here`

Without a Finnhub key the system still works using yfinance for all data.

---

## Classification System

| Score     | Label         | Meaning                                      |
|-----------|---------------|----------------------------------------------|
| 85–100    | Ideal Fit     | Best alignment with trading style            |
| 70–84     | Tradable      | Usable, worth watching                       |
| 55–69     | Watchlist Only| Interesting but not ready or not safe enough |
| 0–54      | Avoid         | Not fit for this style                       |

**Hard reject rules** (force Avoid regardless of score):
- Reverse split in last 12 months
- Active delisting warning
- Going concern warning
- Bankruptcy / restructuring
- Biotech / clinical-stage pharma (configurable)
- Price < $8
- Market cap < $500M
- Avg daily dollar volume < $20M

---

## Score Breakdown (100 pts total)

| Category                    | Weight |
|-----------------------------|--------|
| Technical Trend Fitness     | 30 pts |
| Expansion / Movement        | 20 pts |
| Liquidity / Tradability     | 10 pts |
| Fundamental Stability       | 20 pts |
| News / Earnings / Events    | 15 pts |
| Penalty Overlay             | 0–−25  |

---

## Configuration

Edit [config.py](config.py) to adjust all thresholds:

```python
cfg = AgentConfig(
    min_price=8.0,
    min_market_cap=500_000_000,
    min_avg_daily_volume=500_000,
    min_avg_dollar_volume=20_000_000,
    biotech_excluded=True,
    top_scan_limit=25,
    ideal_fit_min=85.0,
    tradable_min=70.0,
    watchlist_min=55.0,
)
```

---

## Project Structure

```
stock_fitness_agent/
├── main.py                   ← CLI entry point
├── config.py                 ← All configurable parameters
├── cache_layer.py            ← SQLite cache with TTL
│
├── models/
│   ├── stock_data.py         ← Raw data containers
│   └── result.py             ← Scoring output models
│
├── providers/
│   ├── market_data.py        ← yfinance: OHLCV + reference data
│   ├── fundamentals.py       ← yfinance: income stmt, balance sheet, CF
│   ├── earnings.py           ← yfinance + Finnhub: earnings calendar + history
│   ├── news.py               ← yfinance + Finnhub: news with classification
│   └── event_risk.py        ← SEC EDGAR + news: structural risk flags
│
├── scorers/
│   ├── technical.py          ← Monthly/weekly/daily structure (30 pts)
│   ├── movement.py           ← ATR/ADR, expansion behavior (20 pts)
│   ├── liquidity.py          ← Volume, dollar vol, market cap (10 pts)
│   ├── fundamentals.py       ← Revenue, earnings, balance sheet (20 pts)
│   ├── news_event.py         ← Earnings, news, filing risk (15 pts)
│   └── penalty.py            ← Penalty overlay (0 to −25)
│
└── engine/
    ├── universe_loader.py    ← S&P 500 / custom file / explicit list
    ├── pipeline.py           ← Main orchestration
    ├── classifier.py         ← Score → label with override rules
    └── report_generator.py   ← Plain-English output formatting
```

---

## Data Sources

| Data Type              | Source              | API Key Required |
|------------------------|---------------------|-----------------|
| OHLCV / price history  | yfinance            | No              |
| Company reference data | yfinance            | No              |
| Financial statements   | yfinance            | No              |
| Earnings history       | yfinance            | No              |
| Earnings calendar      | Finnhub (preferred) | Optional        |
| News headlines         | yfinance + Finnhub  | Optional        |
| SEC filing risk        | SEC EDGAR API       | No              |

---

## Caching

All data is cached in a local SQLite database (`.cache/stock_fitness_cache.db`).

Cache TTLs (configurable in `config.py`):
- Price data: 24 hours
- Fundamentals: 24 hours
- Earnings: 24 hours
- News: 6 hours
- Reference data: 1 week

---

## Example Output

```
──────────────────────────────────────────────────────────────────────
  NVDA  —  IDEAL FIT  —  87.3/100
  NVIDIA Corporation  |  Technology  |  Semiconductors
──────────────────────────────────────────────────────────────────────
  Score Breakdown:
    Technical Trend:      26.0 / 30
    Expansion/Movement:   17.0 / 20
    Liquidity:             9.5 / 10
    Fundamentals:         18.0 / 20
    News/Earnings/Events: 11.5 / 15
    Penalties:            -2.0
    ───────────────────────────────
    Final Score:          80.0 / 100
    Confidence:           90 / 100

  Key Metrics:
    Price:         $875.00
    Market Cap:    $2.15T
    Avg $Vol/Day:  $3200.0M
    ADR(20):       4.2%
    52W High:      $974.00
    Next Earnings: 2025-05-28

  Why it fits:
    + Strong long-term monthly trend structure
    + Excellent weekly structure with expansion potential
    + Strong daily movement — ADR 4.2% of price
    + Excellent liquidity — $3200M avg daily dollar volume
    + Profitable business with positive net income

  Main concerns:
    - Earnings in ~8 trading days — be aware of risk
```

---

## Disclaimer

This tool is for research and educational purposes only. It does not constitute financial advice. Past performance of any scoring system does not guarantee future results. Always do your own due diligence before trading any security.
