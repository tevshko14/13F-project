from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from filings.models import SearchResult, Holding, FundInfo, HoldingChange

console = Console()


def display_search_results(results: list[SearchResult]) -> None:
    """Print a numbered table of search results."""
    table = Table(title="Fund Manager Search Results")
    table.add_column("#", style="dim", width=4)
    table.add_column("Name", style="bold")
    table.add_column("CIK", style="cyan")
    table.add_column("Ticker", style="green")

    for i, r in enumerate(results, 1):
        table.add_row(str(i), r.name, r.cik, r.ticker or "-")

    console.print(table)
    console.print(
        "\n[dim]Use the CIK number with:[/dim]  "
        "[bold]filings holdings <CIK>[/bold]  or  "
        "[bold]filings compare <CIK>[/bold]"
    )


def display_holdings(
    fund: FundInfo, holdings: list[Holding], showing: int | None = None
) -> None:
    """Print fund summary and holdings table."""
    total_display = f"${fund.total_value:,.0f}"
    summary = (
        f"[bold]{fund.name}[/bold]\n"
        f"CIK: {fund.cik}  |  Report Period: {fund.report_period}  |  "
        f"Filed: {fund.filing_date}\n"
        f"Total Value: [green]{total_display}[/green]  |  "
        f"Total Holdings: {fund.total_holdings}"
    )
    console.print(Panel(summary, title="13F Holdings Report", border_style="blue"))

    table = Table()
    table.add_column("#", style="dim", width=4)
    table.add_column("Issuer", style="bold", max_width=30)
    table.add_column("Class", max_width=15)
    table.add_column("CUSIP", style="dim")
    table.add_column("Value ($)", justify="right", style="green")
    table.add_column("Shares", justify="right", style="cyan")

    for i, h in enumerate(holdings, 1):
        value_display = f"${h.value:,.0f}"
        shares_display = f"{h.shares:,}"
        table.add_row(
            str(i),
            h.issuer_name,
            h.title_of_class,
            h.cusip,
            value_display,
            shares_display,
        )

    console.print(table)

    if showing and showing < fund.total_holdings:
        console.print(
            f"\n[dim]Showing top {showing} of {fund.total_holdings} holdings "
            f"(sorted by value)[/dim]"
        )


def display_comparison(
    current: FundInfo,
    previous: FundInfo,
    changes: list[HoldingChange],
) -> None:
    """Print quarter-over-quarter comparison table."""
    summary = (
        f"[bold]{current.name}[/bold]\n"
        f"Comparing: [cyan]{previous.report_period}[/cyan] -> "
        f"[cyan]{current.report_period}[/cyan]\n"
        f"Previous Value: ${previous.total_value:,.0f}  |  "
        f"Current Value: ${current.total_value:,.0f}"
    )
    console.print(Panel(summary, title="Quarter Comparison", border_style="blue"))

    table = Table()
    table.add_column("#", style="dim", width=4)
    table.add_column("Issuer", style="bold", max_width=30)
    table.add_column("Status", width=10)
    table.add_column("Prev Shares", justify="right")
    table.add_column("Curr Shares", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Curr Value ($)", justify="right")

    status_styles = {
        "NEW": "bold green",
        "CLOSED": "bold red",
        "INCREASED": "green",
        "DECREASED": "red",
        "UNCHANGED": "dim",
    }

    for i, c in enumerate(changes, 1):
        style = status_styles.get(c.status, "")
        change_str = f"{c.share_change:+,}" if c.share_change != 0 else "-"
        prev_str = f"{c.previous_shares:,}" if c.previous_shares else "-"
        curr_str = f"{c.current_shares:,}" if c.current_shares else "-"
        value_str = f"${c.current_value:,.0f}" if c.current_value else "-"

        table.add_row(
            str(i),
            c.issuer_name,
            f"[{style}]{c.status}[/{style}]",
            prev_str,
            curr_str,
            f"[{style}]{change_str}[/{style}]",
            value_str,
        )

    console.print(table)
