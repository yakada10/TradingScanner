"""
Fundamentals provider — yfinance income statement, balance sheet, cash flow.
Normalizes multi-period data into FinancialStatement / BalanceSheetData lists.
"""
import logging
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
import yfinance as yf

from models.stock_data import FundamentalsData, FinancialStatement, BalanceSheetData
from cache_layer import get_cache
from config import get_config

log = logging.getLogger(__name__)


class FundamentalsProvider:

    def __init__(self):
        self._cfg = get_config()
        self._cache = get_cache()

    def get_fundamentals(self, ticker: str) -> FundamentalsData:
        cache_key = f"fundamentals:{ticker}"
        cached = self._cache.get(cache_key, max_age_hours=self._cfg.fundamentals_cache_hours)
        if cached:
            return self._deserialize(cached, ticker)

        fd = FundamentalsData(ticker=ticker, fetch_timestamp=datetime.utcnow())
        try:
            yf_ticker = yf.Ticker(ticker)
            fd.income_statements = self._parse_income(yf_ticker)
            fd.balance_sheets = self._parse_balance_sheet(yf_ticker)
            fd.free_cash_flow_annual = self._parse_fcf(yf_ticker)
            fd.data_complete = bool(fd.income_statements)
            self._cache.set(cache_key, self._serialize(fd))
        except Exception as exc:
            log.error("FundamentalsProvider error for %s: %s", ticker, exc)
        return fd

    # ------------------------------------------------------------------ #
    #  Income Statement
    # ------------------------------------------------------------------ #

    def _parse_income(self, yf_ticker: yf.Ticker) -> List[FinancialStatement]:
        results = []
        try:
            df = yf_ticker.financials  # annual, columns = periods
            if df is None or df.empty:
                return results
            df = df.T.sort_index()
            for idx, row in df.iterrows():
                stmt = FinancialStatement(period_type="annual")
                try:
                    stmt.period_end = pd.Timestamp(idx).date()
                except Exception:
                    pass
                stmt.revenue = self._val(row, ["Total Revenue"])
                stmt.gross_profit = self._val(row, ["Gross Profit"])
                stmt.operating_income = self._val(row, ["Operating Income", "Ebit"])
                stmt.net_income = self._val(row, ["Net Income"])
                # Margins
                if stmt.revenue and stmt.revenue > 0:
                    if stmt.gross_profit is not None:
                        stmt.gross_margin = stmt.gross_profit / stmt.revenue
                    if stmt.operating_income is not None:
                        stmt.operating_margin = stmt.operating_income / stmt.revenue
                results.append(stmt)
        except Exception as exc:
            log.warning("Income parse error: %s", exc)
        return results

    # ------------------------------------------------------------------ #
    #  Balance Sheet
    # ------------------------------------------------------------------ #

    def _parse_balance_sheet(self, yf_ticker: yf.Ticker) -> List[BalanceSheetData]:
        results = []
        try:
            df = yf_ticker.balance_sheet
            if df is None or df.empty:
                return results
            df = df.T.sort_index()
            for idx, row in df.iterrows():
                bs = BalanceSheetData(period_type="annual")
                try:
                    bs.period_end = pd.Timestamp(idx).date()
                except Exception:
                    pass
                bs.cash_and_equivalents = self._val(
                    row, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"]
                )
                bs.total_debt = self._val(row, ["Total Debt", "Long Term Debt"])
                bs.total_assets = self._val(row, ["Total Assets"])
                bs.total_equity = self._val(row, ["Total Stockholder Equity", "Stockholders Equity"])
                bs.current_assets = self._val(row, ["Current Assets", "Total Current Assets"])
                bs.current_liabilities = self._val(row, ["Current Liabilities", "Total Current Liabilities"])
                bs.shares_outstanding = self._val(row, ["Share Issued", "Common Stock Shares Outstanding"])
                results.append(bs)
        except Exception as exc:
            log.warning("Balance sheet parse error: %s", exc)
        return results

    # ------------------------------------------------------------------ #
    #  Free Cash Flow
    # ------------------------------------------------------------------ #

    def _parse_fcf(self, yf_ticker: yf.Ticker) -> Optional[List[float]]:
        try:
            cf = yf_ticker.cashflow
            if cf is None or cf.empty:
                return None
            cf = cf.T.sort_index()
            ocf_col = next((c for c in cf.columns if "Operating" in str(c) and "Cash" in str(c)), None)
            capex_col = next((c for c in cf.columns if "Capital" in str(c) and "Expenditure" in str(c)), None)
            if ocf_col is None:
                return None
            vals = []
            for _, row in cf.iterrows():
                ocf = row.get(ocf_col, np.nan)
                capex = row.get(capex_col, 0) if capex_col else 0
                if not np.isnan(ocf):
                    vals.append(float(ocf) - float(capex if not np.isnan(capex) else 0))
            return vals if vals else None
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _val(row: pd.Series, candidates: list) -> Optional[float]:
        for name in candidates:
            for col in row.index:
                if str(col).lower() == name.lower():
                    v = row[col]
                    if v is not None and not (isinstance(v, float) and np.isnan(v)):
                        return float(v)
        return None

    # ------------------------------------------------------------------ #
    #  Serialization
    # ------------------------------------------------------------------ #

    @staticmethod
    def _serialize(fd: FundamentalsData) -> dict:
        def stmt_to_dict(s):
            return {k: str(v) if hasattr(v, 'isoformat') else v
                    for k, v in s.__dict__.items()}

        return {
            "ticker": fd.ticker,
            "income_statements": [stmt_to_dict(s) for s in fd.income_statements],
            "balance_sheets": [stmt_to_dict(s) for s in fd.balance_sheets],
            "free_cash_flow_annual": fd.free_cash_flow_annual,
            "data_complete": fd.data_complete,
            "fetch_timestamp": str(fd.fetch_timestamp),
        }

    @staticmethod
    def _deserialize(d: dict, ticker: str) -> FundamentalsData:
        from datetime import date
        fd = FundamentalsData(ticker=ticker)
        fd.data_complete = d.get("data_complete", False)
        fd.free_cash_flow_annual = d.get("free_cash_flow_annual")

        for raw in d.get("income_statements", []):
            s = FinancialStatement()
            for k, v in raw.items():
                if k == "period_end" and v:
                    try:
                        setattr(s, k, date.fromisoformat(str(v)))
                    except Exception:
                        pass
                else:
                    try:
                        setattr(s, k, v)
                    except Exception:
                        pass
            fd.income_statements.append(s)

        for raw in d.get("balance_sheets", []):
            b = BalanceSheetData()
            for k, v in raw.items():
                if k == "period_end" and v:
                    try:
                        setattr(b, k, date.fromisoformat(str(v)))
                    except Exception:
                        pass
                else:
                    try:
                        setattr(b, k, v)
                    except Exception:
                        pass
            fd.balance_sheets.append(b)
        return fd
