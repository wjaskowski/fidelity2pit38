from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

SUPPORTED_PIT38_FORM_YEARS = (2024, 2025, 2026)

# Years whose official PIT-38 layout has not been published yet and which
# provisionally reuse a previous year's field mapping. Once the official forms
# for these years are released (typically early in the following year), give the
# year its own branch in report._pit38_summary_sections and drop it from here.
PROVISIONAL_PIT38_FORM_YEARS = {2026: 2025}


def ensure_supported_pit38_form_year(year: Optional[int]) -> None:
    """Validate that PIT-38 layout mapping exists for the given tax year."""
    if year not in SUPPORTED_PIT38_FORM_YEARS:
        supported = ", ".join(str(y) for y in SUPPORTED_PIT38_FORM_YEARS)
        raise ValueError(
            f"PIT-38 layout mapping for year {year} is not implemented. "
            f"Supported years: {supported}."
        )


def warn_if_provisional_form_year(year: Optional[int]) -> None:
    """Warn when the given year reuses an earlier year's PIT-38 layout."""
    reused_from = PROVISIONAL_PIT38_FORM_YEARS.get(year)
    if reused_from is not None:
        import logging

        logging.warning(
            "PIT-38 form layout for %s is not available yet; reusing the %s "
            "fields. Verify against the official %s form and update the mapping "
            "once it is published (usually early in %s).",
            year, reused_from, year, year + 1,
        )


@dataclass(frozen=True)
class PIT38Fields:
    """Computed PIT-38/PIT-ZG fields represented as Decimal values."""
    poz22: Decimal
    poz23: Decimal
    poz26: Decimal
    poz29: Decimal
    poz30_rate: Decimal
    poz31: Decimal
    poz32: Decimal
    tax_final: Decimal
    poz45: Decimal
    poz46: Decimal
    poz47: Decimal
    pitzg_poz29: Decimal
    pitzg_poz30: Decimal
    section_g_uncollected_tax: Decimal = Decimal("0.00")
    section_g_total_income: Decimal = Decimal("0.00")
    section_g_equity_dividends: Decimal = Decimal("0.00")
    section_g_fund_distributions: Decimal = Decimal("0.00")
    year: Optional[int] = None

    def __getitem__(self, key: str):
        return getattr(self, key)

    def print(self, method: str = "") -> None:
        """Print PIT-38/PIT-ZG fields in CLI-friendly, coloured format."""
        from rich.console import Console
        from rich.text import Text
        from .report import _CONSOLE_SECTION_TITLES, _pit38_summary_sections

        console = Console()
        console.print()

        title = Text()
        title.append("PIT-38 for year ", style="bold cyan")
        title.append(str(self.year), style="bold cyan underline")
        if method:
            title.append("  |  Method: ", style="bold cyan")
            title.append(method.upper(), style="bold cyan")
        console.print(title)

        legend = Text()
        legend.append("(", style="dim")
        legend.append("<-- enter", style="bright_green bold")
        legend.append(
            " = fill in the tax form; remaining fields are typically auto-calculated)",
            style="dim",
        )
        console.print(legend)

        for (_, rows), section_title in zip(_pit38_summary_sections(self), _CONSOLE_SECTION_TITLES):
            console.print()
            console.print(Text(f"{section_title}:", style="bold blue"))
            for desc, val, is_raw in rows:
                unit = "" if val.endswith("%") else " PLN"
                row = Text()
                if is_raw:
                    row.append(f"  {desc}: ", style="bright_green")
                    row.append(f"{val}{unit}", style="bright_green bold")
                    row.append("  <-- enter", style="bright_green bold")
                else:
                    row.append(f"  {desc}: ", style="grey62")
                    row.append(f"{val}{unit}", style="grey62 bold")
                console.print(row)
        console.print()
