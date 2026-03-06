from dataclasses import dataclass


@dataclass
class SearchResult:
    name: str
    cik: str
    ticker: str | None


@dataclass
class Holding:
    issuer_name: str
    title_of_class: str
    cusip: str
    value: int
    shares: int
    share_type: str  # "SH" (shares) or "PRN" (principal amount)


@dataclass
class FundInfo:
    name: str
    cik: str
    report_period: str
    filing_date: str
    total_value: int
    total_holdings: int


@dataclass
class HoldingChange:
    issuer_name: str
    cusip: str
    status: str  # "NEW", "CLOSED", "INCREASED", "DECREASED", "UNCHANGED"
    current_shares: int
    previous_shares: int
    share_change: int
    current_value: int
    previous_value: int


@dataclass
class EnrichedHolding(Holding):
    """Holding with ticker, portfolio %, and activity info."""

    ticker: str | None = None
    pct_of_portfolio: float = 0.0
    activity: str | None = None  # "NEW BUY", "ADD", "REDUCE", "SOLD"
    share_change: int = 0


@dataclass
class SuperinvestorSummary:
    """Summary data for one superinvestor on the homepage."""

    cik: str
    display_name: str
    fund_name: str
    portfolio_value: int
    num_holdings: int
    top_holdings: list[str]  # Top tickers, e.g. ["AAPL", "BAC"]
    report_period: str
    filing_date: str


@dataclass
class ActivityItem:
    """A single activity entry for the activity feed."""

    fund_display_name: str
    fund_cik: str
    issuer_name: str
    ticker: str | None
    cusip: str
    action: str  # "NEW BUY", "ADD", "REDUCE", "SOLD"
    share_change: int
    current_value: int
    filing_date: str


@dataclass
class EnrichedActivityItem:
    """Activity entry enriched with conviction score and price data."""

    fund_display_name: str
    fund_cik: str
    fund_aum: int  # total_value from fund cache
    issuer_name: str
    ticker: str | None
    cusip: str
    action: str  # "NEW BUY", "ADD", "REDUCE", "SOLD"
    signal: str  # "NEW ENTRY", "HEAVY ADD", "ADD", "TRIM", "LIQUIDATED", "REDUCE"
    share_change: int
    current_value: int
    portfolio_weight: float  # % of this holding in the fund
    conviction: float  # abs(share_change) × portfolio_weight / 100
    filing_date: str
    price_at_filing: float | None  # stock price near filing_date (future)
    current_price: float | None  # latest stock price
    price_change_pct: float | None  # % change from filing to current
    fund_total_holdings: int = 0  # number of positions in this fund
    portfolio_impact: float = 0.0  # current_value / fund_aum * 100 (trade as % of AUM)
    pct_share_change: float | None = None  # share_change / previous_shares * 100
    trade_value: float = (
        0.0  # abs(share_change) * price - estimated dollar value of trade
    )


@dataclass
class ActivityCluster:
    """Multiple investors acting on the same ticker — consensus signal."""

    ticker: str | None
    cusip: str
    issuer_name: str
    action_summary: str  # "5 Buying / 2 Selling"
    net_sentiment: str  # "BULLISH", "BEARISH", "NEUTRAL"
    investor_count: int
    combined_value: int
    avg_conviction: float
    items: list[EnrichedActivityItem]  # Individual entries (for expand)
    buy_value: float = 0.0  # total $ bought by all investors in cluster
    sell_value: float = 0.0  # total $ sold by all investors in cluster
    net_flow: float = 0.0  # buy_value - sell_value (signed net dollar flow)


@dataclass
class GrandPortfolioEntry:
    """A single stock in the grand portfolio aggregation."""

    issuer_name: str
    ticker: str | None
    cusip: str
    num_holders: int
    combined_value: int
    pct_of_aggregate: float
    holders: list[str]  # Display names of holders


@dataclass
class StockInfo:
    """Basic stock identity — used for any ticker, even without superinvestor data."""

    ticker: str
    issuer_name: str | None
    cusip: str | None
    logo_domain: str | None = None  # e.g. "apple.com" for Clearbit logo lookup
    # SEO-enrichment fields (from yfinance .info, zero extra API cost)
    long_business_summary: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: int | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None
    dividend_yield: float | None = None
    beta: float | None = None
    fifty_two_week_high: float | None = None
    fifty_two_week_low: float | None = None
    current_price: float | None = None
    recommendation_key: str | None = None  # e.g. "buy", "hold"
    exchange: str | None = None  # e.g. "NMS" (NASDAQ)


@dataclass
class StockHolder:
    """One superinvestor's position in a specific stock."""

    fund_display_name: str
    fund_cik: str
    pct_of_portfolio: float
    value: int
    shares: int
    activity: str | None  # "NEW BUY", "ADD", "REDUCE", "SOLD"
    share_change: int


@dataclass
class StockDetail:
    """Aggregated detail for a single stock across all superinvestors."""

    issuer_name: str
    ticker: str | None
    cusip: str
    num_holders: int
    combined_value: int
    holders: list[StockHolder]


@dataclass
class StockQuarterEntry:
    """One investor's activity on a stock in a single quarter."""

    fund_display_name: str
    fund_cik: str
    activity: str  # "NEW BUY", "ADD", "REDUCE", "SOLD"
    share_change: int
    pct_change: float  # % change in share count vs previous quarter


@dataclass
class StockQuarter:
    """All activity on a stock in one quarter, across all superinvestors."""

    period: str  # e.g. "Q3 2025"
    report_date: str  # e.g. "09-30-2025"
    entries: list[StockQuarterEntry]


@dataclass
class AnalystRating:
    """A single analyst (firm-level) rating for a stock."""

    firm: str  # e.g. "JP Morgan", "Goldman Sachs"
    action: str  # "upgrade", "downgrade", "maintain", "init", "reiterate"
    from_grade: str  # e.g. "Hold", "" if N/A
    to_grade: str  # e.g. "Buy"
    date: str  # ISO date string "2025-01-15"
    # Optional price target fields (populated when available from data sources)
    current_price_target: float | None = None
    prior_price_target: float | None = None
    price_target_action: str | None = None
    source: str = "yfinance"
