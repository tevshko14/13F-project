import sys

from rich.console import Console

from filings import client, display

console = Console()

USAGE = """\
[bold]13F Filing Tool[/bold] — View SEC 13F institutional holdings

[bold cyan]Usage:[/bold cyan]
  filings search <name>      Search for fund managers by name
  filings holdings <cik>     View latest 13F holdings for a fund
  filings compare <cik>      Compare last two quarters of holdings
  filings --help             Show this help message

[bold cyan]Examples:[/bold cyan]
  filings search "Berkshire Hathaway"
  filings holdings 1067983
  filings compare 1067983
"""


def main():
    args = sys.argv[1:]

    if not args or args[0] in ("--help", "-h", "help"):
        console.print(USAGE)
        return

    command = args[0]

    try:
        if command == "search":
            if len(args) < 2:
                console.print("[red]Please provide a search term.[/red]")
                console.print(
                    'Example: [bold]filings search "Berkshire Hathaway"[/bold]'
                )
                return
            query = " ".join(args[1:])
            results = client.search_managers(query)
            if not results:
                console.print(
                    f"[yellow]No fund managers found matching '{query}'[/yellow]"
                )
                return
            display.display_search_results(results)

        elif command == "holdings":
            if len(args) < 2:
                console.print("[red]Please provide a CIK number.[/red]")
                console.print(
                    "Tip: Use [bold]filings search <name>[/bold] to find a CIK first."
                )
                return
            cik = args[1]
            top_n = 25
            fund, holdings = client.get_holdings(cik, top_n=top_n)
            display.display_holdings(fund, holdings, showing=top_n)

        elif command == "compare":
            if len(args) < 2:
                console.print("[red]Please provide a CIK number.[/red]")
                console.print(
                    "Tip: Use [bold]filings search <name>[/bold] to find a CIK first."
                )
                return
            cik = args[1]
            current, previous, changes = client.compare_quarters(cik)
            display.display_comparison(current, previous, changes)

        else:
            console.print(f"[red]Unknown command: {command}[/red]\n")
            console.print(USAGE)

    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("[dim]Check your internet connection and try again.[/dim]")
