"""
Universe Loader — defines which tickers to evaluate.

Modes:
  single(ticker)         — single ticker review
  from_list(tickers)     — explicit list
  sp500()                — S&P 500 members (Wikipedia, with fallback)
  sp500_extended()       — S&P 500 + S&P 400 mid-caps
  nasdaq_full()          — All Nasdaq-listed common stocks (official Nasdaq Trader files)
  nyse_nasdaq_full()     — Nasdaq + NYSE/AMEX common stocks (broadest US universe)
  from_file(path)        — load from a plain text file (one ticker per line)

Nasdaq full universe pipeline:
  1. Pull official Nasdaq Trader listing files (nasdaqlisted.txt + otherlisted.txt)
  2. Filter to common stocks only:
       - ETF column = N
       - Test Issue = N
       - Financial Status = N (normal — excludes deficient, delinquent, bankrupt)
       - Security name does not indicate: warrant, right, unit, preferred, depositary/ADR, bond/note
       - Symbol passes format check (no embedded special characters)
  3. Return deduplicated, sorted ticker list
  4. Hard exclusions and scoring filters then applied by the pipeline as normal
"""
import logging
import re
from typing import List, Set

import requests
import pandas as pd
from io import StringIO

from config import get_config

log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  URLs
# ------------------------------------------------------------------ #

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"

# Official Nasdaq Trader symbol directory files (pipe-delimited, refreshed nightly)
_NASDAQ_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
_OTHER_LISTED_URL   = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# Browser-like User-Agent for Wikipedia; Nasdaq Trader accepts anything
_WIKI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_PLAIN_HEADERS = {"User-Agent": "StockFitnessAgent/1.0"}

# ------------------------------------------------------------------ #
#  Name-based exclusion keywords
#  If any of these appear in the security name (case-insensitive),
#  the security is NOT a common stock and is excluded.
# ------------------------------------------------------------------ #

_NAME_EXCLUDE_PATTERNS = [
    r"\bwarrant\b",
    r"\bwarrants\b",
    r"\bright\b",                    # " Right" or "Rights" — avoid "Wright" via word-boundary
    r"\brights\b",
    r"\bunit\b",
    r"\bunits\b",
    r"\bpreferred\b",
    r"\bdepositary\b",               # ADRs — "American Depositary Shares/Receipts"
    r"\bdeposit receipt\b",
    r"\badr\b",
    r"\b\.?\s*note\b",               # bonds/notes
    r"\bnotes\b",
    r"\bdebenture\b",
    r"\bbond\b",
    r"\bsenior note\b",
    r"\bsubordinated\b",
    r"\bperpetual\b",                # preferred/perpetual instruments
    r"\bseries [a-z]\b",             # "Series A Preferred" etc.
]
_NAME_EXCLUDE_RE = re.compile("|".join(_NAME_EXCLUDE_PATTERNS), re.IGNORECASE)

# Valid clean symbol: 1-5 uppercase letters, optional trailing hyphen+letter (e.g. BRK-B)
_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}(-[A-Z])?$")

# ------------------------------------------------------------------ #
#  Fallback list (used if Wikipedia SP500 scrape fails)
# ------------------------------------------------------------------ #

_SP500_FALLBACK = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK-B","JPM","V",
    "XOM","UNH","LLY","JNJ","MA","AVGO","PG","HD","COST","MRK",
    "ABBV","CVX","WMT","BAC","CRM","NFLX","AMD","ACN","PEP","TMO",
    "ADBE","DIS","MCD","CSCO","WFC","TXN","ABT","INTU","IBM","GS",
    "CAT","AMGN","ISRG","SPGI","HON","GE","BKNG","RTX","PFE","LOW",
    "QCOM","VRTX","AMAT","NOW","MS","T","UBER","UNP","NEE","SYK",
    "PM","BA","DHR","ETN","C","SCHW","BX","MDT","AXP","DE",
    "GILD","PANW","ADI","REGN","MU","BSX","ELV","SO","LMT","CI",
    "TJX","LRCX","KKR","SLB","PGR","AON","CB","DUK","MMC","APH",
    "CME","PLD","ZTS","ITW","MCO","CEG","ICE","KLAC","FI","WELL",
    "HCA","USB","CL","CDNS","APD","MSI","TDG","CTAS","ECL","ORLY",
    "GD","NSC","EMR","FCX","WM","MCK","MMM","NOC","AIG","CARR",
    "OKE","COF","PSA","ROP","TFC","MPC","VLO","AFL","AJG","PAYX",
    "AEP","PSX","O","FDX","FAST","GWW","FICO","VRSK","MNST","CPRT",
    "PCAR","KMB","HLT","EW","TEL","ODFL","SRE","AMP","IDXX","SPG",
    "MCHP","AME","FTNT","CTVA","DXCM","ACGL","TROW","MSCI","D","CCI",
    "PH","HES","EOG","DVN","COP","MRO","OXY","HUM","CINF","BIIB",
    "EA","WBD","HPE","HPQ","NTAP","KEYS","VLTO","WAB","EXPD","GEN",
    "SWKS","JBHT","LUV","AAL","UAL","DAL","CCL","RCL","MAR","WYNN",
]


class UniverseLoader:

    def __init__(self):
        self._cfg = get_config()

    # ------------------------------------------------------------------ #
    #  Public universe methods
    # ------------------------------------------------------------------ #

    def single(self, ticker: str) -> List[str]:
        return [ticker.upper().strip()]

    def from_list(self, tickers: List[str]) -> List[str]:
        return [t.upper().strip() for t in tickers if t.strip()]

    def sp500(self) -> List[str]:
        tickers = self._fetch_wikipedia_tickers(_SP500_URL)
        if not tickers:
            log.warning("Wikipedia SP500 scrape failed — using built-in fallback (%d tickers)", len(_SP500_FALLBACK))
            return list(_SP500_FALLBACK)
        return tickers

    def sp500_extended(self) -> List[str]:
        tickers = self._fetch_wikipedia_tickers(_SP500_URL)
        midcap = self._fetch_wikipedia_tickers(_SP400_URL)
        combined = list(dict.fromkeys(tickers + midcap))
        if not combined:
            log.warning("Wikipedia scrape failed — using built-in fallback")
            return list(_SP500_FALLBACK)
        return combined

    def nasdaq_full(self) -> List[str]:
        """
        All Nasdaq-listed common stocks from the official Nasdaq Trader directory.

        Source: nasdaqlisted.txt (pipe-delimited, refreshed nightly)
        Filters applied:
          - ETF = N
          - Test Issue = N
          - Financial Status = N (normal — excludes deficient, delinquent, bankrupt)
          - Security name excludes: warrants, rights, units, preferreds, ADRs, bonds
          - Symbol matches clean common-stock format
        """
        tickers = self._fetch_nasdaq_listed()
        if not tickers:
            log.warning("Nasdaq listed fetch failed — falling back to SP500")
            return self.sp500()
        log.info("nasdaq_full: %d common stocks after filtering", len(tickers))
        return sorted(tickers)

    def nyse_nasdaq_full(self) -> List[str]:
        """
        Broadest US common stock universe: Nasdaq + NYSE + NYSE American (AMEX).

        Combines nasdaqlisted.txt (Nasdaq) and otherlisted.txt (NYSE/AMEX/Arca),
        both from Nasdaq Trader. Applies the same common-stock filters to each.
        Deduplicates. Returns a sorted list.

        Typical size: ~4,000–5,000 eligible common stocks before scoring hard filters.
        """
        nasdaq_tickers = self._fetch_nasdaq_listed()
        nyse_tickers   = self._fetch_other_listed()
        combined = list(dict.fromkeys(sorted(nasdaq_tickers | nyse_tickers)))
        if not combined:
            log.warning("Full universe fetch failed — falling back to SP500")
            return self.sp500()
        log.info("nyse_nasdaq_full: %d common stocks after filtering", len(combined))
        return combined

    def from_file(self, path: str) -> List[str]:
        try:
            with open(path, "r") as f:
                lines = f.readlines()
            tickers = []
            for line in lines:
                t = line.strip().upper()
                if t and not t.startswith("#"):
                    tickers.append(t)
            log.info("Loaded %d tickers from %s", len(tickers), path)
            return tickers
        except Exception as exc:
            log.error("Cannot load ticker file %s: %s", path, exc)
            return []

    # ------------------------------------------------------------------ #
    #  Nasdaq Trader file fetchers
    # ------------------------------------------------------------------ #

    def _fetch_nasdaq_listed(self) -> Set[str]:
        """
        Parse nasdaqlisted.txt — Nasdaq-listed securities.

        Columns (pipe-delimited):
          Symbol | Security Name | Market Category | Test Issue |
          Financial Status | Round Lot Size | ETF | NextShares

        Financial Status codes:
          N = Normal  D = Deficient  E = Delinquent  Q = Bankrupt
          G = Deficient+Bankrupt  H = Deficient+Delinquent  J = Delinquent+Bankrupt
        """
        try:
            resp = requests.get(_NASDAQ_LISTED_URL, headers=_PLAIN_HEADERS, timeout=20)
            resp.raise_for_status()
            # Last line is a metadata "File Creation Time" line — skip it
            text = "\n".join(
                line for line in resp.text.splitlines()
                if not line.startswith("File Creation Time")
            )
            df = pd.read_csv(StringIO(text), sep="|", dtype=str)
            df.columns = [c.strip() for c in df.columns]

            # Apply filters
            mask = (
                (df["ETF"].str.strip() == "N") &
                (df["Test Issue"].str.strip() == "N") &
                (df["Financial Status"].str.strip() == "N")
            )
            df = df[mask].copy()

            tickers: Set[str] = set()
            for _, row in df.iterrows():
                sym  = str(row.get("Symbol", "")).strip().upper()
                name = str(row.get("Security Name", "")).strip()
                if self._is_common_stock(sym, name):
                    tickers.add(sym)

            log.info("nasdaqlisted.txt: %d common stocks", len(tickers))
            return tickers

        except Exception as exc:
            log.warning("Failed to fetch nasdaqlisted.txt: %s", exc)
            return set()

    def _fetch_other_listed(self) -> Set[str]:
        """
        Parse otherlisted.txt — NYSE, NYSE American (AMEX), NYSE Arca, and other
        exchange securities reported through Nasdaq's system.

        Columns (pipe-delimited):
          ACT Symbol | Security Name | Exchange | CQS Symbol |
          ETF | Round Lot Size | Test Issue | NASDAQ Symbol

        Exchange codes:
          N = NYSE  A = NYSE American (AMEX)  P = NYSE Arca
          Z = BATS  V = IEX  Q = NASDAQ (rare)

        We include NYSE (N) and NYSE American (A) only.
        NYSE Arca (P) is excluded — heavy ETF concentration.
        """
        # Exchanges we want (primary listing venues for common stocks)
        _VALID_EXCHANGES = {"N", "A"}

        try:
            resp = requests.get(_OTHER_LISTED_URL, headers=_PLAIN_HEADERS, timeout=20)
            resp.raise_for_status()
            text = "\n".join(
                line for line in resp.text.splitlines()
                if not line.startswith("File Creation Time")
            )
            df = pd.read_csv(StringIO(text), sep="|", dtype=str)
            df.columns = [c.strip() for c in df.columns]

            mask = (
                (df["ETF"].str.strip() == "N") &
                (df["Test Issue"].str.strip() == "N") &
                (df["Exchange"].str.strip().isin(_VALID_EXCHANGES))
            )
            df = df[mask].copy()

            tickers: Set[str] = set()
            for _, row in df.iterrows():
                # otherlisted uses "ACT Symbol" as the primary symbol column
                sym  = str(row.get("ACT Symbol", "")).strip().upper()
                name = str(row.get("Security Name", "")).strip()
                if self._is_common_stock(sym, name):
                    tickers.add(sym)

            log.info("otherlisted.txt (NYSE/AMEX): %d common stocks", len(tickers))
            return tickers

        except Exception as exc:
            log.warning("Failed to fetch otherlisted.txt: %s", exc)
            return set()

    # ------------------------------------------------------------------ #
    #  Common stock validation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_common_stock(symbol: str, name: str) -> bool:
        """
        Returns True if the symbol/name combination appears to be a common stock.

        Exclusion criteria:
          1. Symbol fails the clean-format regex (too long, embedded special chars)
          2. Security name contains a keyword indicating a non-common instrument
        """
        if not symbol or not name:
            return False

        # Symbol format check — common stocks are 1-5 uppercase letters
        # Allow one trailing hyphen+letter for dual-class shares (e.g. BRK-B)
        if not _SYMBOL_RE.match(symbol):
            return False

        # Name keyword check
        if _NAME_EXCLUDE_RE.search(name):
            return False

        return True

    # ------------------------------------------------------------------ #
    #  Wikipedia fetcher (for SP500 / SP500 extended)
    # ------------------------------------------------------------------ #

    def _fetch_wikipedia_tickers(self, url: str) -> List[str]:
        try:
            resp = requests.get(url, headers=_WIKI_HEADERS, timeout=15)
            resp.raise_for_status()
            tables = pd.read_html(StringIO(resp.text), header=0)
            for table in tables:
                for col in ["Symbol", "Ticker", "Ticker symbol"]:
                    if col in table.columns:
                        tickers = table[col].tolist()
                        cleaned = []
                        for t in tickers:
                            t = str(t).strip().replace("\n", "").replace(".", "-")
                            if re.match(r"^[A-Z0-9\-]+$", t):
                                cleaned.append(t)
                        if cleaned:
                            log.info("Loaded %d tickers from Wikipedia: %s", len(cleaned), url)
                            return cleaned
        except Exception as exc:
            log.warning("Wikipedia load failed for %s: %s", url, exc)
        return []
