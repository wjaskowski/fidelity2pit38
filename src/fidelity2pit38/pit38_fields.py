import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

SUPPORTED_PIT38_FORM_YEARS = (2024, 2025)


def ensure_supported_pit38_form_year(year: Optional[int]) -> None:
    """Validate that PIT-38 layout mapping exists for the given tax year."""
    if year not in SUPPORTED_PIT38_FORM_YEARS:
        supported = ", ".join(str(y) for y in SUPPORTED_PIT38_FORM_YEARS)
        raise ValueError(
            f"PIT-38 layout mapping for year {year} is not implemented. "
            f"Supported years: {supported}."
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

    # Suffix for fields the user enters in the tax form (raw/independent).
    # Fields without this suffix are typically auto-calculated.
    _ENTER = "  <-- enter"

    def print(self) -> None:
        """Print PIT-38/PIT-ZG fields in CLI-friendly format."""
        ensure_supported_pit38_form_year(self.year)
        capital_income = max(self.poz26, Decimal("0.00")).quantize(Decimal("0.01"))
        capital_loss = max(-self.poz26, Decimal("0.00")).quantize(Decimal("0.01"))
        e = self._ENTER

        print(f"\n\nPIT-38 for year {self.year}:")
        print("(<-- enter = fill in the tax form;"
              " remaining fields are typically auto-calculated)")
        print("\nCzesc C/D - Dochody ze zbycia papierow wartosciowych (art. 30b):")
        if self.year == 2024:
            print(f"  Poz. 22 (Inne przychody): {self.poz22:.2f} PLN{e}")
            print(f"  Poz. 23 (Koszty uzyskania przychodow): {self.poz23:.2f} PLN{e}")
            print(f"  Poz. 26 (Dochod): {capital_income:.2f} PLN")
            print(f"  Poz. 27 (Strata): {capital_loss:.2f} PLN")
            print(f"  Poz. 28 (Straty z lat ubieglych): 0.00 PLN{e}")
            print(f"  Poz. 29 (Podstawa opodatkowania): {self.poz29}.00 PLN")
            print(f"  Poz. 30 (Stawka podatku): {int(self.poz30_rate * 100)}%")
            print(f"  Poz. 31 (Podatek): {self.poz31:.2f} PLN")
            print(f"  Poz. 32 (Podatek zaplacony za granica): {self.poz32:.2f} PLN{e}")
            print(f"  Poz. 33 (Podatek nalezny): {self.tax_final:.2f} PLN")
        else:
            print(f"  Poz. 22 (Inne przychody): {self.poz22:.2f} PLN{e}")
            print(f"  Poz. 23 (Koszty uzyskania przychodow): {self.poz23:.2f} PLN{e}")
            print(f"  Poz. 26 (Przychod - razem): {self.poz22:.2f} PLN")
            print(f"  Poz. 27 (Koszty uzyskania - razem): {self.poz23:.2f} PLN")
            print(f"  Poz. 28 (Dochod): {capital_income:.2f} PLN")
            print(f"  Poz. 29 (Strata): {capital_loss:.2f} PLN")
            print(f"  Poz. 30 (Straty z lat ubieglych): 0.00 PLN{e}")
            print(f"  Poz. 31 (Podstawa opodatkowania): {self.poz29}.00 PLN")
            print(f"  Poz. 32 (Stawka podatku): {int(self.poz30_rate * 100)}%")
            print(f"  Poz. 33 (Podatek): {self.poz31:.2f} PLN")
            print(f"  Poz. 34 (Podatek zaplacony za granica): {self.poz32:.2f} PLN{e}")
            print(f"  Poz. 35 (Podatek nalezny): {self.tax_final:.2f} PLN")

        print("\nCzesc G - Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5):")
        if self.year == 2024:
            print(f"  Poz. 45 (Podatek 19% od przychodow czesci G): {self.poz45:.2f} PLN{e}")
            print(f"  Poz. 46 (Podatek zaplacony za granica): {self.poz46:.2f} PLN{e}")
            print(f"  Poz. 47 (Do zaplaty): {self.poz47:.2f} PLN")
        else:
            print(f"  Poz. 46 (Podatek niepobrany przez platnika): {self.section_g_uncollected_tax:.2f} PLN{e}")
            print(f"  Poz. 47 (Podatek 19% od przychodow czesci G): {self.poz45:.2f} PLN{e}")
            print(f"  Poz. 48 (Podatek zaplacony za granica): {self.poz46:.2f} PLN{e}")
            print(f"  Poz. 49 (Do zaplaty): {self.poz47:.2f} PLN")

        print("\nPIT-ZG (dochody zagraniczne):")
        print(f"  Poz. 29 (Dochod z art. 30b ust.5 i 5b): {self.pitzg_poz29:.2f} PLN{e}")
        print(f"  Poz. 30 (Podatek zaplacony za granica): {self.pitzg_poz30:.2f} PLN{e}")
