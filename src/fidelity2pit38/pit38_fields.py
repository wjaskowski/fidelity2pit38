from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from rich.console import Console
from rich.text import Text

SUPPORTED_PIT38_FORM_YEARS = (2024, 2025)

_console = Console()


def ensure_supported_pit38_form_year(year: Optional[int]) -> None:
    """Validate that PIT-38 layout mapping exists for the given tax year."""
    if year not in SUPPORTED_PIT38_FORM_YEARS:
        supported = ", ".join(str(y) for y in SUPPORTED_PIT38_FORM_YEARS)
        raise ValueError(
            f"PIT-38 layout mapping for year {year} is not implemented. "
            f"Supported years: {supported}."
        )


def _row(label: str, value: str, enter: bool) -> Text:
    """Build a colored row for one PIT-38 field.

    Enter rows (fields the user must manually type into the form) are rendered
    in bright yellow so they stand out.  Auto-calculated rows use the default
    terminal colour.
    """
    if enter:
        text = Text()
        text.append(f"  {label}: ", style="bright_yellow")
        text.append(value, style="bright_yellow bold")
        text.append("  <-- enter", style="bright_green bold")
        return text
    else:
        text = Text()
        text.append(f"  {label}: ", style="")
        text.append(value, style="bold")
        return text


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

    def print(self) -> None:
        """Print PIT-38/PIT-ZG fields in CLI-friendly, coloured format."""
        ensure_supported_pit38_form_year(self.year)
        capital_income = max(self.poz26, Decimal("0.00")).quantize(Decimal("0.01"))
        capital_loss = max(-self.poz26, Decimal("0.00")).quantize(Decimal("0.01"))

        _console.print()
        _console.print()

        title = Text()
        title.append("PIT-38 for year ", style="bold cyan")
        title.append(str(self.year), style="bold cyan underline")
        _console.print(title)

        legend = Text()
        legend.append("(", style="dim")
        legend.append("<-- enter", style="bright_green bold")
        legend.append(" = fill in the tax form; remaining fields are typically auto-calculated)", style="dim")
        _console.print(legend)

        _console.print()
        _console.print(Text("Czesc C/D - Dochody ze zbycia papierow wartosciowych (art. 30b):", style="bold blue"))

        if self.year == 2024:
            _console.print(_row("Poz. 22 (Inne przychody)", f"{self.poz22:.2f} PLN", enter=True))
            _console.print(_row("Poz. 23 (Koszty uzyskania przychodow)", f"{self.poz23:.2f} PLN", enter=True))
            _console.print(_row("Poz. 26 (Dochod)", f"{capital_income:.2f} PLN", enter=False))
            _console.print(_row("Poz. 27 (Strata)", f"{capital_loss:.2f} PLN", enter=False))
            _console.print(_row("Poz. 28 (Straty z lat ubieglych)", "0.00 PLN", enter=True))
            _console.print(_row("Poz. 29 (Podstawa opodatkowania)", f"{self.poz29}.00 PLN", enter=False))
            _console.print(_row("Poz. 30 (Stawka podatku)", f"{int(self.poz30_rate * 100)}%", enter=False))
            _console.print(_row("Poz. 31 (Podatek)", f"{self.poz31:.2f} PLN", enter=False))
            _console.print(_row("Poz. 32 (Podatek zaplacony za granica)", f"{self.poz32:.2f} PLN", enter=True))
            _console.print(_row("Poz. 33 (Podatek nalezny)", f"{self.tax_final:.2f} PLN", enter=False))
        else:
            _console.print(_row("Poz. 22 (Inne przychody)", f"{self.poz22:.2f} PLN", enter=True))
            _console.print(_row("Poz. 23 (Koszty uzyskania przychodow)", f"{self.poz23:.2f} PLN", enter=True))
            _console.print(_row("Poz. 26 (Przychod - razem)", f"{self.poz22:.2f} PLN", enter=False))
            _console.print(_row("Poz. 27 (Koszty uzyskania - razem)", f"{self.poz23:.2f} PLN", enter=False))
            _console.print(_row("Poz. 28 (Dochod)", f"{capital_income:.2f} PLN", enter=False))
            _console.print(_row("Poz. 29 (Strata)", f"{capital_loss:.2f} PLN", enter=False))
            _console.print(_row("Poz. 30 (Straty z lat ubieglych)", "0.00 PLN", enter=True))
            _console.print(_row("Poz. 31 (Podstawa opodatkowania)", f"{self.poz29}.00 PLN", enter=False))
            _console.print(_row("Poz. 32 (Stawka podatku)", f"{int(self.poz30_rate * 100)}%", enter=False))
            _console.print(_row("Poz. 33 (Podatek)", f"{self.poz31:.2f} PLN", enter=False))
            _console.print(_row("Poz. 34 (Podatek zaplacony za granica)", f"{self.poz32:.2f} PLN", enter=True))
            _console.print(_row("Poz. 35 (Podatek nalezny)", f"{self.tax_final:.2f} PLN", enter=False))

        _console.print()
        _console.print(Text("Czesc G - Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5):", style="bold blue"))
        if self.year == 2024:
            _console.print(_row("Poz. 45 (Podatek 19% od przychodow czesci G)", f"{self.poz45:.2f} PLN", enter=True))
            _console.print(_row("Poz. 46 (Podatek zaplacony za granica)", f"{self.poz46:.2f} PLN", enter=True))
            _console.print(_row("Poz. 47 (Do zaplaty)", f"{self.poz47:.2f} PLN", enter=False))
        else:
            _console.print(_row("Poz. 46 (Podatek niepobrany przez platnika)", f"{self.section_g_uncollected_tax:.2f} PLN", enter=True))
            _console.print(_row("Poz. 47 (Podatek 19% od przychodow czesci G)", f"{self.poz45:.2f} PLN", enter=True))
            _console.print(_row("Poz. 48 (Podatek zaplacony za granica)", f"{self.poz46:.2f} PLN", enter=True))
            _console.print(_row("Poz. 49 (Do zaplaty)", f"{self.poz47:.2f} PLN", enter=False))

        _console.print()
        _console.print(Text("PIT-ZG (dochody zagraniczne):", style="bold blue"))
        _console.print(_row("Poz. 29 (Dochod z art. 30b ust.5 i 5b)", f"{self.pitzg_poz29:.2f} PLN", enter=True))
        _console.print(_row("Poz. 30 (Podatek zaplacony za granica)", f"{self.pitzg_poz30:.2f} PLN", enter=True))
        _console.print()
