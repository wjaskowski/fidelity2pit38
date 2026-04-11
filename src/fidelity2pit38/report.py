"""Report generation: data structures and CSV renderer for PIT-38 audit trail."""

import csv
import io
import logging
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

from .pit38_fields import PIT38Fields


@dataclass
class CapitalGainAlloc:
    """One matched buy-lot contributing to a single sale."""
    sale_settlement_date: date
    buy_settlement_date: Optional[date]  # None for RSU lots (no monetary buy)
    security: str
    quantity: float
    proceeds_usd_per_share: float
    proceeds_usd: float
    sale_nbp_rate_date: Optional[date]
    sale_nbp_rate: float
    proceeds_pln: float
    cost_usd_per_share: float
    cost_usd: float
    buy_nbp_rate_date: Optional[date]    # None for RSU lots
    buy_nbp_rate: Optional[float]        # None for RSU lots
    cost_pln: float
    gain_pln: float
    source: str                          # 'RSU', 'ESPP', or 'MARKET'


@dataclass
class DividendRow:
    """One DIVIDEND RECEIVED transaction with its matching foreign-tax row."""
    date: date
    security: str
    kind: str                            # 'equity' or 'fund'
    amount_usd: float
    nbp_rate_date: Optional[date]
    nbp_rate: float
    amount_pln: float
    foreign_tax_usd: float               # 0.0 when no NON-RESIDENT TAX row matches
    foreign_tax_pln: float


@dataclass
class ReportData:
    """All data needed to render a full PIT-38 audit report."""
    year: int
    capital_gains: List[CapitalGainAlloc]
    dividends: List[DividendRow]
    pit38: PIT38Fields


# ── Rendering ──────────────────────────────────────────────────────────────────

def render_csv(data: ReportData) -> str:
    """Render ReportData as a multi-section CSV string.

    Returns a string suitable for writing directly to a .csv file.
    Three sections are separated by blank lines; each has its own header row.
    This non-standard but opens cleanly in Excel/LibreOffice.
    """
    buf = io.StringIO()
    w = csv.writer(buf)

    # ── Section 1: Capital Gains ───────────────────────────────────────
    w.writerow(["CAPITAL GAINS (Section C/D - art. 30b)"])
    w.writerow([
        "Sale Date", "Buy Date", "Security", "Qty",
        "Proceeds USD/share", "Proceeds USD", "Sale NBP Rate Date", "Sale NBP Rate", "Proceeds PLN",
        "Cost USD/share", "Cost USD", "Buy NBP Rate Date", "Buy NBP Rate", "Cost PLN",
        "Gain/Loss PLN", "Source",
    ])
    for a in data.capital_gains:
        w.writerow([
            a.sale_settlement_date.isoformat(),
            a.buy_settlement_date.isoformat() if a.buy_settlement_date else "",
            a.security,
            a.quantity,
            f"{a.proceeds_usd_per_share:.4f}",
            f"{a.proceeds_usd:.2f}",
            a.sale_nbp_rate_date.isoformat() if a.sale_nbp_rate_date else "",
            f"{a.sale_nbp_rate:.4f}",
            f"{a.proceeds_pln:.2f}",
            f"{a.cost_usd_per_share:.4f}",
            f"{a.cost_usd:.2f}",
            a.buy_nbp_rate_date.isoformat() if a.buy_nbp_rate_date else "",
            f"{a.buy_nbp_rate:.4f}" if a.buy_nbp_rate is not None else "",
            f"{a.cost_pln:.2f}",
            f"{a.gain_pln:.2f}",
            a.source,
        ])
    w.writerow([
        "TOTAL", "", "",
        sum(a.quantity for a in data.capital_gains),
        "",
        f"{sum(a.proceeds_usd for a in data.capital_gains):.2f}", "", "",
        f"{sum(a.proceeds_pln for a in data.capital_gains):.2f}",
        "",
        f"{sum(a.cost_usd for a in data.capital_gains):.2f}", "", "",
        f"{sum(a.cost_pln for a in data.capital_gains):.2f}",
        f"{sum(a.gain_pln for a in data.capital_gains):.2f}",
        "",
    ])
    w.writerow([])

    # ── Section 2: Dividends & Foreign Tax ────────────────────────────
    w.writerow(["DIVIDENDS & FOREIGN TAX (Section G - art. 30a)"])
    w.writerow([
        "Date", "Security", "Type",
        "Amount USD", "NBP Rate Date", "NBP Rate", "Amount PLN",
        "Foreign Tax USD", "Foreign Tax PLN",
    ])
    for d in data.dividends:
        w.writerow([
            d.date.isoformat(),
            d.security,
            d.kind,
            f"{d.amount_usd:.2f}",
            d.nbp_rate_date.isoformat() if d.nbp_rate_date else "",
            f"{d.nbp_rate:.4f}",
            f"{d.amount_pln:.2f}",
            f"{d.foreign_tax_usd:.2f}",
            f"{d.foreign_tax_pln:.2f}",
        ])
    w.writerow([
        "TOTAL", "", "",
        f"{sum(d.amount_usd for d in data.dividends):.2f}", "", "",
        f"{sum(d.amount_pln for d in data.dividends):.2f}",
        f"{sum(d.foreign_tax_usd for d in data.dividends):.2f}",
        f"{sum(d.foreign_tax_pln for d in data.dividends):.2f}",
    ])
    w.writerow([])

    # ── Section 3: PIT-38 Summary ─────────────────────────────────────
    w.writerow([f"PIT-38 SUMMARY (year {data.year})"])
    w.writerow(["Field", "Description", "Value PLN"])
    for field_id, description, value in _pit38_summary_rows(data.pit38):
        w.writerow([field_id, description, value])

    return buf.getvalue()


def write_reports(data: ReportData, output_dir: str) -> None:
    """Write pit38_report_{year}.csv to output_dir, creating the directory if needed."""
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"pit38_report_{data.year}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write(render_csv(data))
    logging.info("Report written to %s", csv_path)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pit38_summary_rows(pit38: PIT38Fields) -> List[Tuple[str, str, str]]:
    """Return (field_id, description, value_str) for the PIT-38 summary section."""
    year = pit38.year
    capital_income = max(pit38.poz26, Decimal("0.00"))
    capital_loss = max(-pit38.poz26, Decimal("0.00"))

    if year == 2024:
        return [
            ("poz22",    "Poz. 22 - Inne przychody (przychody ze zbycia)",       f"{pit38.poz22:.2f}"),
            ("poz23",    "Poz. 23 - Koszty uzyskania przychodow",                f"{pit38.poz23:.2f}"),
            ("poz26",    "Poz. 26 - Dochod",                                     f"{capital_income:.2f}"),
            ("poz27",    "Poz. 27 - Strata",                                     f"{capital_loss:.2f}"),
            ("poz28",    "Poz. 28 - Straty z lat ubieglych",                     "0.00"),
            ("poz29",    "Poz. 29 - Podstawa opodatkowania",                     f"{pit38.poz29}"),
            ("poz30",    "Poz. 30 - Stawka podatku",                             f"{int(pit38.poz30_rate * 100)}%"),
            ("poz31",    "Poz. 31 - Podatek",                                    f"{pit38.poz31:.2f}"),
            ("poz32",    "Poz. 32 - Podatek zaplacony za granica (art. 30b)",    f"{pit38.poz32:.2f}"),
            ("poz33",    "Poz. 33 - Podatek nalezny",                            f"{pit38.tax_final:.2f}"),
            ("poz45",    "Poz. 45 - Podatek 19% od przychodow czesci G",         f"{pit38.poz45:.2f}"),
            ("poz46",    "Poz. 46 - Podatek zaplacony za granica (Section G)",   f"{pit38.poz46:.2f}"),
            ("poz47",    "Poz. 47 - Do zaplaty (Section G)",                     f"{pit38.poz47:.2f}"),
            ("pitzg29",  "PIT-ZG Poz. 29 - Dochod z art. 30b ust.5 i 5b",      f"{pit38.pitzg_poz29:.2f}"),
            ("pitzg30",  "PIT-ZG Poz. 30 - Podatek zaplacony za granica",       f"{pit38.pitzg_poz30:.2f}"),
            ("diag_g",   "Section G - Total income (diagnostics)",               f"{pit38.section_g_total_income:.2f}"),
            ("diag_eq",  "Section G - Equity dividends (diagnostics)",           f"{pit38.section_g_equity_dividends:.2f}"),
            ("diag_fd",  "Section G - Fund distributions (diagnostics)",         f"{pit38.section_g_fund_distributions:.2f}"),
        ]
    else:  # 2025
        return [
            ("poz22",    "Poz. 22 - Inne przychody (przychody ze zbycia)",       f"{pit38.poz22:.2f}"),
            ("poz23",    "Poz. 23 - Koszty uzyskania przychodow",                f"{pit38.poz23:.2f}"),
            ("poz26",    "Poz. 26 - Przychod - razem",                           f"{pit38.poz22:.2f}"),
            ("poz27",    "Poz. 27 - Koszty uzyskania - razem",                   f"{pit38.poz23:.2f}"),
            ("poz28",    "Poz. 28 - Dochod",                                     f"{capital_income:.2f}"),
            ("poz29",    "Poz. 29 - Strata",                                     f"{capital_loss:.2f}"),
            ("poz30",    "Poz. 30 - Straty z lat ubieglych",                     "0.00"),
            ("poz31",    "Poz. 31 - Podstawa opodatkowania",                     f"{pit38.poz29}"),
            ("poz32",    "Poz. 32 - Stawka podatku",                             f"{int(pit38.poz30_rate * 100)}%"),
            ("poz33",    "Poz. 33 - Podatek",                                    f"{pit38.poz31:.2f}"),
            ("poz34",    "Poz. 34 - Podatek zaplacony za granica (art. 30b)",    f"{pit38.poz32:.2f}"),
            ("poz35",    "Poz. 35 - Podatek nalezny",                            f"{pit38.tax_final:.2f}"),
            ("poz46",    "Poz. 46 - Podatek niepobrany przez platnika",          f"{pit38.section_g_uncollected_tax:.2f}"),
            ("poz47",    "Poz. 47 - Podatek 19% od przychodow czesci G",         f"{pit38.poz45:.2f}"),
            ("poz48",    "Poz. 48 - Podatek zaplacony za granica (Section G)",   f"{pit38.poz46:.2f}"),
            ("poz49",    "Poz. 49 - Do zaplaty (Section G)",                     f"{pit38.poz47:.2f}"),
            ("pitzg29",  "PIT-ZG Poz. 29 - Dochod z art. 30b ust.5 i 5b",      f"{pit38.pitzg_poz29:.2f}"),
            ("pitzg30",  "PIT-ZG Poz. 30 - Podatek zaplacony za granica",       f"{pit38.pitzg_poz30:.2f}"),
            ("diag_g",   "Section G - Total income (diagnostics)",               f"{pit38.section_g_total_income:.2f}"),
            ("diag_eq",  "Section G - Equity dividends (diagnostics)",           f"{pit38.section_g_equity_dividends:.2f}"),
            ("diag_fd",  "Section G - Fund distributions (diagnostics)",         f"{pit38.section_g_fund_distributions:.2f}"),
        ]
