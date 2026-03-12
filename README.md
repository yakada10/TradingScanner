# tradetuu — Stock Scanner

Evaluates whether a stock is a good fit for a short-term / scalp trading style focused on:
- **Short-term trading opportunity** — ADR/ATR, expansion behavior, reversal setups
- **Multi-timeframe confirmation** — weekly and daily structure as primary HTF signals
- **Reversal / Reclaim detection** — defended lows, higher lows, post-earnings reactions
- **Adequate liquidity** — enough dollar volume and movement to enter and exit cleanly
- **Business survivability** — not junk, not a going-concern, not a chronic diluter

This is a **stock selection scanner**, not a signal bot. It does not generate entries, exits, or stop-losses.

---

## Design Philosophy

The scanner balances five dimensions:

1. **Stock health / company quality** — fundamentals, balance sheet, capital discipline
2. **Short-term expansion opportunity** — ADR%, expansion days, weekly range capacity
3. **Multi-timeframe trade opportunity** — weekly and daily technical structure
4. **Reversal / reclaim potential** — defended lows, higher lows forming, post-earnings reaction
5. **Junk avoidance** — hard filters for going-concern, reverse splits, delisting, chronic dilution

A stock does **not** need to be a perfect long-term investment to rank well. It needs:
- Acceptable company health (not junk)
- High short-term tradable opportunity (movement, expansion, or reversal setup)
- Manageable risk (no structural red flags)

**PATH-style example:** Positive earnings reaction + daily defended lows + weekly starting to climb → scores well even if monthly chart is still in a broader downtrend.

**UMAC-style example:** High ADR + strong expansion days + liquidity + decent financials → scores well as a high-opportunity active name.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Evaluate a single ticker
python main.py evaluate AAPL

# 3. Evaluate multiple tickers
python main.py evaluate NVDA MSFT PATH UMAC

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

| Score   | Grade | Label          | Meaning                                              |
|---------|-------|----------------|------------------------------------------------------|
| 82–100  | A     | Ideal Fit      | Best alignment — strong opportunity, acceptable risk |
| 67–81   | B     | Tradable       | Good setup, worth active attention                   |
| 52–66   | C     | Watchlist Only | Some interest but not ready or not safe enough       |
| 0–51    | D     | Avoid          | Not fit for this style                               |

**Hard reject rules** (force Avoid regardless of score):
- Reverse split in last 12 months
- Active delisting warning
- Going concern warning
- Bankruptcy / restructuring
- Biotech / clinical-stage pharma (configurable)
- Price < $5 (hard floor) / $5–$8 = caution zone with soft penalty
- Market cap < $500M
- Avg daily dollar volume < $20M

---

## Score Breakdown (100 pts total)

| Category                          | Weight | Notes                                               |
|-----------------------------------|--------|-----------------------------------------------------|
| **Technical Trend Fitness**       | 22 pts | Monthly 6 / Weekly 10 / Daily 6                     |
| **Expansion / Movement**          | 28 pts | ADR 10 / Daily-exp 7 / Weekly-exp 5 / Vol-qual 6   |
| **Reversal / Recovery**           | 10 pts | Defended-lows 3 / Higher-lows 3 / Post-earn 2 / Weekly-rev 2 |
| **Liquidity / Tradability**       | 10 pts | Dollar vol / Share vol / Spread proxy               |
| **Fundamental Stability**         | 15 pts | Revenue 4 / Earnings 3 / Balance sheet 4 / Durability 2 / Capital 2 |
| **News / Earnings / Events**      | 15 pts | Earnings proximity / quality / news / filing risk   |
| **Penalty Overlay**               | 0–−25  | Reverse split, dilution, offerings, legal, etc.     |

### Weight Rationale

**Movement/Expansion (28 pts) is the largest category** because for a scalp trader
targeting 2–5% moves, a stock that can't deliver range simply can't be traded.

**Reversal/Recovery (10 pts, new)** surfaces turning-point setups that the
Technical scorer can't see yet — defended lows, higher lows forming, positive
earnings reactions. This is what makes PATH-style stocks score correctly.

**Technical Trend (22 pts, reduced from 30)** still confirms multi-timeframe
direction but no longer dominates. A monthly downtrend is less punishing if
the weekly and daily are improving.

**Fundamentals (15 pts, reduced from 20)** ensures junk avoidance without
over-rewarding slow "investment quality" companies at the expense of opportunity.

---

## Movement / ADR Scoring

The primary movement signal (ADR % of price):

| ADR Range | Score  | Assessment                            |
|-----------|--------|---------------------------------------|
| ≥ 6.0%    | 10 pts | Exceptional — very high opportunity   |
| ≥ 4.0%    |  8 pts | Excellent movement for scalp style    |
| ≥ 3.0%    |  6 pts | Good — can deliver 2–5% trades        |
| ≥ 2.0%    |  4 pts | Moderate — workable, tighter targets  |
| ≥ 1.5%    |  2 pts | Below-average — limited opportunity   |
| < 1.5%    |  0 pts | Too slow for this style               |

---

## Reversal / Recovery Scorer (New)

Detects early-stage turning points on stocks that may not have a clean monthly
uptrend but show real near-term reversal signals:

| Signal                    | Max  | What it looks for                                          |
|---------------------------|------|------------------------------------------------------------|
| Defended lows / wicks     | 3 pts| Long lower wicks (>1.5% of price) closing above mid-range |
| Higher lows forming       | 3 pts| Recent 10d low > prior 10d low (ascending support)        |
| Post-earnings reaction    | 2 pts| Beat + price held/followed through after report           |
| Weekly reversal structure | 2 pts| Recent 4-week avg close > prior 4 weeks, lows defended    |

---

## Configuration

Edit [config.py](config.py) to adjust all thresholds:

```python
cfg = AgentConfig(
    min_price=5.0,                 # hard reject below $5
    min_price_clean=8.0,           # soft penalty $5–$8
    min_market_cap=500_000_000,
    min_avg_daily_volume=500_000,
    min_avg_dollar_volume=20_000_000,
    biotech_excluded=True,
    top_scan_limit=25,
    ideal_fit_min=82.0,
    tradable_min=67.0,
    watchlist_min=52.0,
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
│   └── event_risk.py         ← SEC EDGAR + news: structural risk flags
│
├── scorers/
│   ├── technical.py          ← Monthly/weekly/daily structure (22 pts)
│   ├── movement.py           ← ATR/ADR, expansion behavior (28 pts)
│   ├── reversal.py           ← Reversal/recovery opportunity (10 pts) [NEW]
│   ├── liquidity.py          ← Volume, dollar vol, market cap (10 pts)
│   ├── fundamentals.py       ← Revenue, earnings, balance sheet (15 pts)
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
  PATH  —  TRADABLE  —  71.4/100
  UiPath Inc.  |  Technology  |  Software
──────────────────────────────────────────────────────────────────────
  Score Breakdown:
    Technical Trend:      11.0 / 22
    Expansion/Movement:   20.5 / 28
    Reversal/Recovery:     7.0 / 10
    Liquidity:             7.5 / 10
    Fundamentals:          9.0 / 15
    News/Earnings/Events: 11.5 / 15
    Penalties:            -5.0
    ───────────────────────────────
    Final Score:          61.5 / 100
    Confidence:           85 / 100

  Why it fits:
    + Strong daily movement — ADR 4.1% of price
    + Reversal / recovery signals present — structure starting to improve
    + Meaningful bullish wick rejections — buyers defending key levels
    + Higher lows forming on daily chart — ascending support
    + Positive earnings reaction with sustained price follow-through
    + Good liquidity — $180M avg daily dollar volume

  Main concerns:
    - Weak or damaged monthly trend structure — look for reversal signals
    - Not currently profitable
```

---

## Disclaimer

This tool is for research and educational purposes only. It does not constitute financial advice. Past performance of any scoring system does not guarantee future results. Always do your own due diligence before trading any security.
