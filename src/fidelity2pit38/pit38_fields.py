from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


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
    section_g_total_income: Decimal = Decimal("0.00")
    section_g_equity_dividends: Decimal = Decimal("0.00")
    section_g_fund_distributions: Decimal = Decimal("0.00")
    year: Optional[int] = None

    def __getitem__(self, key: str):
        return getattr(self, key)

    def print(self) -> None:
        """Print PIT-38/PIT-ZG fields in CLI-friendly format."""
        print(f"\n\nPIT-38 for year {self.year}:")
        print("\nCzesc C/D - Dochody ze zbycia papierow wartosciowych (art. 30b):")
        print(f"  Poz. 22 (Przychod): {self.poz22:.2f} PLN")
        print(f"  Poz. 23 (Koszty uzyskania): {self.poz23:.2f} PLN")
        print(f"  Poz. 26 (Dochod): {self.poz26:.2f} PLN")
        print(f"  Poz. 29 (Podstawa opodatkowania): {self.poz29}.00 PLN")
        print(f"  Poz. 30 (Stawka podatku): {int(self.poz30_rate * 100)}%")
        print(f"  Poz. 31 (Podatek): {self.poz31:.2f} PLN")
        print(f"  Poz. 32 (Podatek zaplacony za granica): {self.poz32:.2f} PLN")
        print(f"  Poz. 33 (Podatek nalezny): {self.tax_final:.2f} PLN")

        print("\nCzesc G - Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5):")
        print(f"  Poz. 45 (Podatek 19% od przychodow czesci G): {self.poz45:.2f} PLN")
        print(f"  Poz. 46 (Podatek zaplacony za granica): {self.poz46:.2f} PLN")
        print(f"  Poz. 47 (Do zaplaty): {self.poz47:.2f} PLN")

        print("\nPIT-ZG (dochody zagraniczne):")
        print(f"  Poz. 29 (Dochod z art. 30b ust.5 i 5b): {self.pitzg_poz29:.2f} PLN")
        print(f"  Poz. 30 (Podatek zaplacony za granica): {self.pitzg_poz30:.2f} PLN")
