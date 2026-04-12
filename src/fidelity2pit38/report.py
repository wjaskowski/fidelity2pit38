"""Report generation: data structures and CSV renderer for PIT-38 audit trail."""

import csv
import io
import logging
import os
import webbrowser
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
    for section_title, rows in _pit38_summary_sections(data.pit38):
        w.writerow([])
        w.writerow([section_title])
        w.writerow(["Description", "Value PLN"])
        for desc, val, _ in rows:
            w.writerow([desc, val])

    return buf.getvalue()


_CSS = """\
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 13px; background: #f1f5f9; color: #1e293b; padding: 2rem;
}
.wrap { max-width: 1600px; margin: 0 auto; }
h1 {
    font-size: 1.5rem; font-weight: 700; margin-bottom: 2rem;
    padding-bottom: .75rem; border-bottom: 3px solid #3b82f6;
}
.card {
    background: #fff; border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 2rem; overflow: hidden;
}
.card-title {
    font-size: .7rem; font-weight: 700; letter-spacing: .1em;
    text-transform: uppercase; color: #f8fafc; background: #1e293b;
    padding: .65rem 1rem;
}
.scroll { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; white-space: nowrap; }
th {
    background: #f8fafc; color: #64748b; font-size: .68rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: .06em;
    padding: .55rem .75rem; border-bottom: 2px solid #e2e8f0; text-align: right;
}
th.l { text-align: left; }
td {
    padding: .45rem .75rem; border-bottom: 1px solid #f1f5f9;
    text-align: right; font-variant-numeric: tabular-nums;
}
td.l { text-align: left; }
td.c { text-align: center; }
td.mono { font-family: ui-monospace, monospace; font-size: .8rem; color: #94a3b8; text-align: left; }
tbody tr:hover { background: #f0f9ff; }
tr.tot td {
    font-weight: 700; background: #f1f5f9 !important;
    border-top: 2px solid #cbd5e0; border-bottom: none;
}
.pos { color: #16a34a; }
.neg { color: #dc2626; }
.badge {
    display: inline-block; padding: .15rem .45rem; border-radius: 9999px;
    font-size: .65rem; font-weight: 700; text-transform: uppercase; letter-spacing: .05em;
}
.badge-rsu    { background: #dbeafe; color: #1d4ed8; }
.badge-espp   { background: #dcfce7; color: #15803d; }
.badge-market { background: #f1f5f9; color: #475569; }
.badge-equity { background: #fef3c7; color: #b45309; }
.badge-fund   { background: #f3e8ff; color: #7c3aed; }
tr.section-hdr td {
    text-align: left; background: #f8fafc !important; font-weight: 600; font-size: .78rem;
    color: #475569; border-top: 2px solid #e2e8f0; padding-top: .6rem;
}
tr.derived td { color: #94a3b8; }
.table-note { font-size: .75rem; color: #64748b; padding: .5rem 1rem .75rem; }
footer { margin-top: 1rem; font-size: .75rem; color: #94a3b8; text-align: right; }
"""


def render_html(data: ReportData) -> str:
    """Render ReportData as a self-contained HTML page."""

    def e(s: object) -> str:
        return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def n(v: float, d: int = 2) -> str:
        return f"{v:.{d}f}"

    def opt_date(d: Optional[date]) -> str:
        return d.isoformat() if d else ''

    def gain_cls(v: float) -> str:
        return ' class="pos"' if v > 0 else (' class="neg"' if v < 0 else '')

    def badge(text: str, kind: str) -> str:
        return f'<span class="badge badge-{kind.lower()}">{e(text)}</span>'

    # ── Capital gains ──────────────────────────────────────────────────
    cg_rows = '\n'.join(
        f'<tr>'
        f'<td class="l">{a.sale_settlement_date.isoformat()}</td>'
        f'<td class="l">{opt_date(a.buy_settlement_date)}</td>'
        f'<td class="l">{e(a.security)}</td>'
        f'<td>{a.quantity}</td>'
        f'<td>{n(a.proceeds_usd_per_share, 4)}</td>'
        f'<td>{n(a.proceeds_usd)}</td>'
        f'<td class="l">{opt_date(a.sale_nbp_rate_date)}</td>'
        f'<td>{n(a.sale_nbp_rate, 4)}</td>'
        f'<td>{n(a.proceeds_pln)}</td>'
        f'<td>{n(a.cost_usd_per_share, 4)}</td>'
        f'<td>{n(a.cost_usd)}</td>'
        f'<td class="l">{opt_date(a.buy_nbp_rate_date)}</td>'
        f'<td>{n(a.buy_nbp_rate, 4) if a.buy_nbp_rate is not None else ""}</td>'
        f'<td>{n(a.cost_pln)}</td>'
        f'<td{gain_cls(a.gain_pln)}>{n(a.gain_pln)}</td>'
        f'<td class="c">{badge(a.source, a.source)}</td>'
        f'</tr>'
        for a in data.capital_gains
    )
    cg_gain_total = sum(a.gain_pln for a in data.capital_gains)
    cg_total_row = (
        f'<tr class="tot">'
        f'<td class="l" colspan="3">Total</td>'
        f'<td>{sum(a.quantity for a in data.capital_gains)}</td>'
        f'<td></td>'
        f'<td>{n(sum(a.proceeds_usd for a in data.capital_gains))}</td>'
        f'<td colspan="2"></td>'
        f'<td>{n(sum(a.proceeds_pln for a in data.capital_gains))}</td>'
        f'<td></td>'
        f'<td>{n(sum(a.cost_usd for a in data.capital_gains))}</td>'
        f'<td colspan="2"></td>'
        f'<td>{n(sum(a.cost_pln for a in data.capital_gains))}</td>'
        f'<td{gain_cls(cg_gain_total)}>{n(cg_gain_total)}</td>'
        f'<td></td>'
        f'</tr>'
    )

    # ── Dividends ──────────────────────────────────────────────────────
    div_rows = '\n'.join(
        f'<tr>'
        f'<td class="l">{d.date.isoformat()}</td>'
        f'<td class="l">{e(d.security)}</td>'
        f'<td class="c">{badge(d.kind, d.kind)}</td>'
        f'<td>{n(d.amount_usd)}</td>'
        f'<td class="l">{opt_date(d.nbp_rate_date)}</td>'
        f'<td>{n(d.nbp_rate, 4)}</td>'
        f'<td>{n(d.amount_pln)}</td>'
        f'<td>{n(d.foreign_tax_usd)}</td>'
        f'<td>{n(d.foreign_tax_pln)}</td>'
        f'</tr>'
        for d in data.dividends
    )
    div_total_row = (
        f'<tr class="tot">'
        f'<td class="l" colspan="3">Total</td>'
        f'<td>{n(sum(d.amount_usd for d in data.dividends))}</td>'
        f'<td colspan="2"></td>'
        f'<td>{n(sum(d.amount_pln for d in data.dividends))}</td>'
        f'<td>{n(sum(d.foreign_tax_usd for d in data.dividends))}</td>'
        f'<td>{n(sum(d.foreign_tax_pln for d in data.dividends))}</td>'
        f'</tr>'
    )

    # ── PIT-38 summary ─────────────────────────────────────────────────
    pit_sections_html = []
    for section_title, rows in _pit38_summary_sections(data.pit38):
        section_rows_parts = []
        for desc, val, is_raw in rows:
            cls = '' if is_raw else ' class="derived"'
            section_rows_parts.append(
                f'<tr{cls}><td class="l">{e(desc)}</td><td>{e(val)}</td></tr>'
            )
        pit_sections_html.append(
            f'<tr class="section-hdr"><td colspan="2">{e(section_title)}</td></tr>\n'
            + '\n'.join(section_rows_parts)
        )
    pit_html = '\n'.join(pit_sections_html)

    generated = date.today().isoformat()
    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PIT-38 Report {data.year}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="wrap">
<h1>PIT-38 Report {data.year}</h1>

<div class="card">
<div class="card-title">Capital Gains &mdash; Section C/D (art. 30b)</div>
<div class="scroll"><table>
<thead><tr>
<th class="l">Sale Date</th><th class="l">Buy Date</th><th class="l">Security</th>
<th>Qty</th>
<th>Proceeds USD/share</th><th>Proceeds USD</th>
<th class="l">Sale NBP Rate Date</th><th>Sale NBP Rate</th><th>Proceeds PLN</th>
<th>Cost USD/share</th><th>Cost USD</th>
<th class="l">Buy NBP Rate Date</th><th>Buy NBP Rate</th><th>Cost PLN</th>
<th>Gain/Loss PLN</th><th>Source</th>
</tr></thead>
<tbody>
{cg_rows}
{cg_total_row}
</tbody>
</table></div>
</div>

<div class="card">
<div class="card-title">Dividends &amp; Foreign Tax &mdash; Section G (art. 30a)</div>
<div class="scroll"><table>
<thead><tr>
<th class="l">Date</th><th class="l">Security</th><th>Type</th>
<th>Amount USD</th><th class="l">NBP Rate Date</th><th>NBP Rate</th><th>Amount PLN</th>
<th>Foreign Tax USD</th><th>Foreign Tax PLN</th>
</tr></thead>
<tbody>
{div_rows}
{div_total_row}
</tbody>
</table></div>
</div>

<div class="card">
<div class="card-title">PIT-38 Summary &mdash; year {data.year}</div>
<div class="scroll"><table>
<thead><tr><th class="l">Description</th><th>Value</th></tr></thead>
<tbody>
{pit_html}
</tbody>
</table></div>
<p class="table-note">Values shown in grey are auto-calculated by the tax form and do not need to be entered manually. Values in normal text must be entered into the form.</p>
</div>

<footer>Generated {generated}</footer>
</div>
</body>
</html>"""


def write_reports(data: ReportData, output_dir: str, open_browser: bool = False) -> None:
    """Write pit38_report_{year}.csv and .html to output_dir, creating it if needed."""
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, f"pit38_report_{data.year}")
    html_path = None
    for ext, content in ((".csv", render_csv(data)), (".html", render_html(data))):
        path = base + ext
        with open(path, "w", newline="" if ext == ".csv" else None, encoding="utf-8") as f:
            f.write(content)
        logging.info("Report written to %s", path)
        if ext == ".html":
            html_path = path
    if open_browser and html_path:
        webbrowser.open(f"file://{os.path.abspath(html_path)}")


# ── Helpers ────────────────────────────────────────────────────────────────────

# (description, value_str, is_raw)  — is_raw=True means the user must enter it manually
_PitRow = Tuple[str, str, bool]


def _pit38_summary_sections(pit38: PIT38Fields) -> List[Tuple[str, List[_PitRow]]]:
    """Return PIT-38 summary rows grouped by form section, with enter/auto annotations."""
    capital_income = max(pit38.poz26, Decimal("0.00"))
    capital_loss   = max(-pit38.poz26, Decimal("0.00"))

    if pit38.year == 2024:
        cd: List[_PitRow] = [
            ("Poz. 22 (Inne przychody)",                                f"{pit38.poz22:.2f}",                      True),
            ("Poz. 23 (Koszty uzyskania przychodow)",                   f"{pit38.poz23:.2f}",                      True),
            ("Poz. 26 (Dochod)",                                        f"{capital_income:.2f}",                   False),
            ("Poz. 27 (Strata)",                                        f"{capital_loss:.2f}",                     False),
            ("Poz. 28 (Straty z lat ubieglych)",                        "0.00",                                    True),
            ("Poz. 29 (Podstawa opodatkowania)",                        f"{pit38.poz29}.00",                       False),
            ("Poz. 30 (Stawka podatku)",                                f"{int(pit38.poz30_rate * 100)}%",         False),
            ("Poz. 31 (Podatek)",                                       f"{pit38.poz31:.2f}",                      False),
            ("Poz. 32 (Podatek zaplacony za granica)",                  f"{pit38.poz32:.2f}",                      True),
            ("Poz. 33 (Podatek nalezny)",                               f"{pit38.tax_final:.2f}",                  False),
        ]
        g: List[_PitRow] = [
            ("Poz. 45 (Podatek 19% od przychodow czesci G)",            f"{pit38.poz45:.2f}",                      True),
            ("Poz. 46 (Podatek zaplacony za granica)",                  f"{pit38.poz46:.2f}",                      True),
            ("Poz. 47 (Do zaplaty)",                                    f"{pit38.poz47:.2f}",                      False),
        ]
    else:  # 2025
        cd = [
            ("Poz. 22 (Inne przychody)",                                f"{pit38.poz22:.2f}",                      True),
            ("Poz. 23 (Koszty uzyskania przychodow)",                   f"{pit38.poz23:.2f}",                      True),
            ("Poz. 26 (Przychod - razem)",                              f"{pit38.poz22:.2f}",                      False),
            ("Poz. 27 (Koszty uzyskania - razem)",                      f"{pit38.poz23:.2f}",                      False),
            ("Poz. 28 (Dochod)",                                        f"{capital_income:.2f}",                   False),
            ("Poz. 29 (Strata)",                                        f"{capital_loss:.2f}",                     False),
            ("Poz. 30 (Straty z lat ubieglych)",                        "0.00",                                    True),
            ("Poz. 31 (Podstawa opodatkowania)",                        f"{pit38.poz29}.00",                       False),
            ("Poz. 32 (Stawka podatku)",                                f"{int(pit38.poz30_rate * 100)}%",         False),
            ("Poz. 33 (Podatek)",                                       f"{pit38.poz31:.2f}",                      False),
            ("Poz. 34 (Podatek zaplacony za granica)",                  f"{pit38.poz32:.2f}",                      True),
            ("Poz. 35 (Podatek nalezny)",                               f"{pit38.tax_final:.2f}",                  False),
        ]
        g = [
            ("Poz. 46 (Podatek niepobrany przez platnika)",             f"{pit38.section_g_uncollected_tax:.2f}",  True),
            ("Poz. 47 (Podatek 19% od przychodow czesci G)",            f"{pit38.poz45:.2f}",                      True),
            ("Poz. 48 (Podatek zaplacony za granica)",                  f"{pit38.poz46:.2f}",                      True),
            ("Poz. 49 (Do zaplaty)",                                    f"{pit38.poz47:.2f}",                      False),
        ]

    pitzg: List[_PitRow] = [
        ("Poz. 29 (Dochod z art. 30b ust.5 i 5b)",                     f"{pit38.pitzg_poz29:.2f}",                True),
        ("Poz. 30 (Podatek zaplacony za granica)",                      f"{pit38.pitzg_poz30:.2f}",                True),
    ]

    return [
        ("Section C/D – Dochody ze zbycia papierow wartosciowych (art. 30b)", cd),
        ("Section G – Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5)",       g),
        ("PIT-ZG – Dochody zagraniczne",                                       pitzg),
    ]
