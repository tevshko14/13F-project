"""Stock Screener — data aggregation for DCF, Monte Carlo & Comps.

Pulls from existing cached data sources (fundamentals, client, earnings,
market_data) and bundles everything into a single JSON-serialisable dict
for client-side valuation calculations.
"""

from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _extract_row_values(
    statement: dict | None,
    label: str,
) -> list[tuple[str, float]]:
    """Return [(period, value), ...] for a row label in a statement dict.

    *statement* has shape ``{"periods": [...], "rows": [{"label": ..., "values": {period: val}}]}``.
    """
    if not statement:
        return []
    for row in statement.get("rows", []):
        if row.get("label") == label:
            vals = row.get("values", {})
            return [
                (p, vals[p])
                for p in statement.get("periods", [])
                if vals.get(p) is not None
            ]
    return []


def _row_values_list(statement: dict | None, label: str) -> list[float]:
    """Return just the values (period-ordered) for a label."""
    return [v for _, v in _extract_row_values(statement, label)]


def _row_values_with_periods(
    statement: dict | None, label: str,
) -> tuple[list[float], list[str]]:
    """Return (values, periods) for a label."""
    pairs = _extract_row_values(statement, label)
    return [v for _, v in pairs], [p for p, _ in pairs]


# ═══════════════════════════════════════════════════════════════════════
# 52-Week Range & Beta from Tiingo EOD
# ═══════════════════════════════════════════════════════════════════════

def _get_52w_and_beta(ticker: str) -> dict:
    """Compute 52-week high/low and beta from Tiingo EOD data.

    Returns {"fifty_two_week_high": ..., "fifty_two_week_low": ..., "beta": ...}.
    Any field may be None if data is unavailable.
    """
    result: dict = {"fifty_two_week_high": None, "fifty_two_week_low": None, "beta": None}

    try:
        from filings import tiingo
        if not tiingo.has_tiingo_key():
            return result

        # 1y of daily data for the ticker
        eod = tiingo.get_eod_history(ticker, period_days=370)
        if not eod or len(eod) < 20:
            return result

        # 52-week high / low from adjHigh / adjLow (split-adjusted)
        highs = [d.get("adjHigh") or d.get("high") for d in eod if d.get("adjHigh") or d.get("high")]
        lows = [d.get("adjLow") or d.get("low") for d in eod if d.get("adjLow") or d.get("low")]

        if highs:
            result["fifty_two_week_high"] = round(max(highs), 2)
        if lows:
            result["fifty_two_week_low"] = round(min(lows), 2)

        # Beta = cov(stock_returns, spy_returns) / var(spy_returns)
        closes = [d.get("adjClose") or d.get("close") for d in eod
                  if d.get("adjClose") or d.get("close")]
        if len(closes) < 60:
            return result

        spy_eod = tiingo.get_eod_history("SPY", period_days=370)
        if not spy_eod or len(spy_eod) < 60:
            return result

        spy_closes = [d.get("adjClose") or d.get("close") for d in spy_eod
                      if d.get("adjClose") or d.get("close")]

        # Align by length (both should be ~252 trading days)
        n = min(len(closes), len(spy_closes))
        closes = closes[-n:]
        spy_closes = spy_closes[-n:]

        # Daily returns
        stock_ret = [(closes[i] / closes[i - 1]) - 1 for i in range(1, n)]
        spy_ret = [(spy_closes[i] / spy_closes[i - 1]) - 1 for i in range(1, n)]

        m = len(stock_ret)
        if m < 30:
            return result

        mean_s = sum(stock_ret) / m
        mean_b = sum(spy_ret) / m
        cov = sum((stock_ret[i] - mean_s) * (spy_ret[i] - mean_b) for i in range(m)) / (m - 1)
        var_b = sum((spy_ret[i] - mean_b) ** 2 for i in range(m)) / (m - 1)

        if var_b > 0:
            result["beta"] = round(cov / var_b, 2)

    except Exception:
        logger.debug("Tiingo 52w/beta fallback failed for %s", ticker)

    return result


# ═══════════════════════════════════════════════════════════════════════
# Peer Discovery
# ═══════════════════════════════════════════════════════════════════════

def _discover_peers(
    ticker: str,
    yf_info: dict,
    max_peers: int = 5,
) -> list[dict]:
    """Find 3-5 peer companies by matching sector from S&P 500 constituents."""
    from filings import market_data
    from filings.client import get_yfinance_info

    sector = yf_info.get("sector", "")
    market_cap = yf_info.get("marketCap") or 0
    if not sector or market_cap == 0:
        return []

    try:
        constituents = market_data.get_sp500_constituents()
    except Exception:
        logger.warning("Failed to fetch S&P 500 constituents for peer discovery")
        return []

    # Filter by matching sector, exclude the target ticker
    candidates = [
        c for c in constituents
        if c.get("sector", "") == sector
        and c.get("ticker", "").upper() != ticker.upper()
    ]

    if not candidates:
        return []

    # Score by market cap proximity (log-space distance)
    scored: list[tuple[float, dict, dict]] = []
    log_target = math.log10(market_cap) if market_cap > 0 else 0

    for c in candidates[:20]:  # limit yfinance fetches
        try:
            peer_info = get_yfinance_info(c["ticker"])
        except Exception:
            continue
        peer_mcap = peer_info.get("marketCap") or 0
        if peer_mcap == 0:
            continue
        dist = abs(math.log10(peer_mcap) - log_target)
        scored.append((dist, c, peer_info))

    scored.sort(key=lambda x: x[0])

    peers: list[dict] = []
    for dist, c, pinfo in scored[:max_peers]:
        pmcap = pinfo.get("marketCap")
        # P/S ratio
        p_revenue = pinfo.get("totalRevenue") or pinfo.get("revenue")
        ps = None
        if pmcap and p_revenue and p_revenue > 0:
            ps = round(pmcap / p_revenue, 2)

        peers.append({
            "ticker": c.get("ticker", "").upper(),
            "name": c.get("name", pinfo.get("longName") or pinfo.get("shortName") or ""),
            "current_price": pinfo.get("currentPrice") or pinfo.get("regularMarketPrice"),
            "market_cap": pmcap,
            "trailing_pe": pinfo.get("trailingPE"),
            "forward_pe": pinfo.get("forwardPE"),
            "ev_ebitda": pinfo.get("enterpriseToEbitda"),
            "price_to_sales": ps,
            "trailing_eps": pinfo.get("trailingEps"),
        })

    return peers


# ═══════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════

def get_screener_data(ticker: str) -> dict | None:
    """Aggregate all data needed for DCF / Monte Carlo / Comps.

    Returns a JSON-serialisable dict or None if the ticker has no data.
    """
    from filings.client import get_yfinance_info
    from filings import fundamentals, earnings

    ticker = ticker.upper()

    # ── 1. yfinance info ──────────────────────────────────────────────
    try:
        yf_info = get_yfinance_info(ticker)
    except Exception:
        logger.warning("yfinance info failed for %s", ticker)
        yf_info = {}

    current_price = (
        yf_info.get("currentPrice")
        or yf_info.get("regularMarketPrice")
    )

    # Fallback: try Tiingo for price if yfinance failed
    if not current_price:
        try:
            from filings import tiingo
            if tiingo.has_tiingo_key():
                tq = tiingo.get_quote(ticker)
                if tq and tq.get("last"):
                    current_price = tq["last"]
        except Exception:
            pass

    # Fallback: try market_data chart data for price
    if not current_price:
        try:
            from filings import market_data
            chart = market_data.get_overview_chart_data(ticker, "1D")
            if chart and chart.get("price"):
                current_price = chart["price"]
        except Exception:
            pass

    # Fallback: try standalone yfinance without custom session
    if not current_price:
        try:
            import yfinance as yf
            tk = yf.Ticker(ticker)
            info = tk.info or {}
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            # Merge into yf_info if we got data
            if info and not yf_info:
                yf_info = info
        except Exception:
            pass

    if not current_price:
        return None  # can't do valuation without a price

    # If yfinance was empty, try to get shares_outstanding from a fallback
    shares_outstanding = yf_info.get("sharesOutstanding")
    if not shares_outstanding and yf_info.get("marketCap") and current_price:
        shares_outstanding = int(yf_info["marketCap"] / current_price)

    # ── 2. Fundamentals (SEC XBRL) ────────────────────────────────────
    try:
        fund_data = fundamentals.get_fundamentals(ticker)
    except Exception:
        logger.warning("fundamentals failed for %s", ticker)
        fund_data = None

    # Fallback: shares outstanding from SEC XBRL (in-memory cache from fundamentals)
    if not shares_outstanding and fund_data:
        try:
            xbrl = fundamentals._fetch_xbrl_facts(ticker)
            if xbrl:
                for concept in ("CommonStockSharesOutstanding",
                                "EntityCommonStockSharesOutstanding",
                                "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
                                "WeightedAverageNumberOfDilutedSharesOutstanding"):
                    fact = xbrl.get(concept)
                    if not fact:
                        continue
                    units = fact.get("units", {})
                    shares_data = units.get("shares", [])
                    if not shares_data:
                        continue
                    # Get the most recent 10-K or 10-Q filing value
                    best = None
                    for entry in shares_data:
                        form = entry.get("form", "")
                        if form in ("10-K", "10-Q", "10-K/A", "10-Q/A"):
                            val = entry.get("val")
                            end = entry.get("end", "")
                            if val and val > 0:
                                if best is None or end > best[1]:
                                    best = (val, end)
                    if best:
                        shares_outstanding = int(best[0])
                        break
        except Exception:
            logger.debug("SEC shares outstanding fallback failed for %s", ticker)

    annual = fund_data.get("annual") if fund_data else None
    quarterly = fund_data.get("quarterly") if fund_data else None

    # Extract key line items
    annual_income = annual.get("income") if annual else None
    annual_balance = annual.get("balance") if annual else None
    annual_cashflow = annual.get("cashflow") if annual else None
    annual_ratios = annual.get("ratios") if annual else None
    quarterly_ratios = quarterly.get("ratios") if quarterly else None

    # FCF
    annual_fcf_vals, annual_fcf_periods = _row_values_with_periods(annual_ratios, "Free Cash Flow")
    quarterly_fcf_vals, quarterly_fcf_periods = _row_values_with_periods(quarterly_ratios, "Free Cash Flow")

    # Revenue
    annual_revenue_vals, annual_revenue_periods = _row_values_with_periods(annual_income, "Revenue")

    # Operating Income
    annual_oi_vals = _row_values_list(annual_income, "Operating Income")

    # Revenue Growth YoY
    revenue_growth_vals = _row_values_list(annual_ratios, "Revenue Growth YoY")

    # Operating Margin
    operating_margin_vals = _row_values_list(annual_ratios, "Operating Margin")

    # Balance sheet items (most recent period)
    def _latest(stmt: dict | None, label: str) -> float | None:
        pairs = _extract_row_values(stmt, label)
        return pairs[0][1] if pairs else None

    cash = _latest(annual_balance, "Cash & Equivalents")
    lt_debt = _latest(annual_balance, "Long-Term Debt")
    st_debt = _latest(annual_balance, "Short-Term Debt")
    total_debt = None
    if lt_debt is not None or st_debt is not None:
        total_debt = (lt_debt or 0) + (st_debt or 0)
    total_equity = _latest(annual_balance, "Total Equity")

    # Market cap fallback: shares_outstanding × current_price
    mcap = yf_info.get("marketCap")
    if not mcap and shares_outstanding and current_price:
        mcap = int(shares_outstanding * current_price)

    # P/S ratio
    ps_ratio = None
    if mcap and annual_revenue_vals and annual_revenue_vals[0] and annual_revenue_vals[0] > 0:
        ps_ratio = round(mcap / annual_revenue_vals[0], 2)

    # ── 3. Forward estimates ──────────────────────────────────────────
    fwd_estimates: dict[str, Any] = {"eps": [], "revenue": []}
    try:
        fwd = earnings.get_forward_estimates(ticker)
        if fwd:
            fwd_estimates = fwd
    except Exception:
        logger.warning("forward estimates failed for %s", ticker)

    # ── 4. Peers ──────────────────────────────────────────────────────
    try:
        peers = _discover_peers(ticker, yf_info)
    except Exception:
        logger.warning("peer discovery failed for %s", ticker)
        peers = []

    # Company name fallback from S&P constituents
    company_name = yf_info.get("longName") or yf_info.get("shortName")
    sector = yf_info.get("sector")
    industry = yf_info.get("industry")
    if not company_name:
        try:
            from filings import market_data
            for c in market_data.get_sp500_constituents():
                if c.get("ticker", "").upper() == ticker:
                    company_name = c.get("name")
                    if not sector:
                        sector = c.get("sector")
                    break
        except Exception:
            pass

    # Trailing EPS fallback from SEC net income / shares
    trailing_eps = yf_info.get("trailingEps")
    if not trailing_eps and shares_outstanding:
        net_income = _latest(annual_income, "Net Income")
        if net_income and shares_outstanding > 0:
            trailing_eps = round(net_income / shares_outstanding, 2)

    # Forward P/E from forward estimates + price
    forward_pe = yf_info.get("forwardPE")
    forward_eps = yf_info.get("forwardEps")
    if not forward_eps and fwd_estimates.get("eps"):
        for est in fwd_estimates["eps"]:
            if est.get("period_key") == "0y" and est.get("avg"):
                forward_eps = est["avg"]
                break
    if not forward_pe and forward_eps and current_price and forward_eps > 0:
        forward_pe = round(current_price / forward_eps, 1)

    # Trailing P/E from trailing EPS + price
    trailing_pe = yf_info.get("trailingPE")
    if not trailing_pe and trailing_eps and current_price and trailing_eps > 0:
        trailing_pe = round(current_price / trailing_eps, 1)

    # ── 4b. Beta & 52-week range (yfinance → Tiingo → yf history) ────
    beta = yf_info.get("beta")
    w52_high = yf_info.get("fiftyTwoWeekHigh")
    w52_low = yf_info.get("fiftyTwoWeekLow")

    # Fallback 1: Tiingo EOD (fast, reliable on prod)
    if not beta or not w52_high or not w52_low:
        tiingo_data = _get_52w_and_beta(ticker)
        if not beta and tiingo_data.get("beta"):
            beta = tiingo_data["beta"]
        if not w52_high and tiingo_data.get("fifty_two_week_high"):
            w52_high = tiingo_data["fifty_two_week_high"]
        if not w52_low and tiingo_data.get("fifty_two_week_low"):
            w52_low = tiingo_data["fifty_two_week_low"]

    # Fallback 2: yfinance history download (single ticker, works locally)
    if not beta or not w52_high or not w52_low:
        try:
            import yfinance as yf
            hist = yf.download(
                [ticker, "SPY"], period="1y", progress=False, timeout=15,
            )
            if hist is not None and not hist.empty:
                # 52-week range from High/Low columns
                if not w52_high or not w52_low:
                    try:
                        highs = hist["High"][ticker].dropna()
                        lows = hist["Low"][ticker].dropna()
                        if len(highs) > 10:
                            w52_high = round(float(highs.max()), 2)
                        if len(lows) > 10:
                            w52_low = round(float(lows.min()), 2)
                    except Exception:
                        pass
                # Beta from daily returns
                if not beta:
                    try:
                        closes = hist["Close"]
                        if ticker in closes.columns and "SPY" in closes.columns:
                            stock_c = closes[ticker].dropna()
                            spy_c = closes["SPY"].dropna()
                            # Align on shared dates
                            common = stock_c.index.intersection(spy_c.index)
                            if len(common) > 60:
                                sr = stock_c.loc[common].pct_change().dropna()
                                br = spy_c.loc[common].pct_change().dropna()
                                common2 = sr.index.intersection(br.index)
                                sr = sr.loc[common2]
                                br = br.loc[common2]
                                if len(sr) > 30:
                                    cov = sr.cov(br)
                                    var_b = br.var()
                                    if var_b > 0:
                                        beta = round(float(cov / var_b), 2)
                    except Exception:
                        pass
        except Exception:
            logger.debug("yfinance history fallback failed for %s", ticker)

    # ── 5. Assemble response ──────────────────────────────────────────
    return {
        "ticker": ticker,
        "company_name": company_name or ticker,
        "sector": sector,
        "industry": industry,
        "current_price": current_price,
        "market_cap": mcap,
        "shares_outstanding": shares_outstanding,
        "beta": beta,
        "trailing_pe": trailing_pe,
        "forward_pe": forward_pe,
        "trailing_eps": trailing_eps,
        "forward_eps": forward_eps,
        "dividend_yield": yf_info.get("dividendYield"),
        "fifty_two_week_high": w52_high,
        "fifty_two_week_low": w52_low,
        "ev_ebitda": yf_info.get("enterpriseToEbitda"),
        "financials": {
            "quarterly_fcf": quarterly_fcf_vals,
            "quarterly_fcf_periods": quarterly_fcf_periods,
            "annual_fcf": annual_fcf_vals,
            "annual_fcf_periods": annual_fcf_periods,
            "annual_revenue": annual_revenue_vals,
            "annual_revenue_periods": annual_revenue_periods,
            "annual_operating_income": annual_oi_vals,
            "revenue_growth_yoy": revenue_growth_vals,
            "operating_margin": operating_margin_vals,
            "cash_and_equivalents": cash,
            "total_debt": total_debt,
            "total_equity": total_equity,
            "price_to_sales": ps_ratio,
        },
        "peers": peers,
        "forward_estimates": fwd_estimates,
    }
