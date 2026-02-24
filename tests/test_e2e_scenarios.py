"""End-to-end scenario tests for fidelity2pit38.

Each test builds a self-contained CSV (and optionally a custom-summary TXT),
runs the full pipeline, and asserts the computed PIT-38 / PIT-ZG fields.

All NBP rates are deterministic fixtures — no network access is needed.
Expected values are hand-calculated from the pipeline logic:
  1. parse CSV → trade_date, shares, amount_usd
  2. settlement_date (T+1 post-SWITCH_DATE, T+2 before, T+0 for dividends)
  3. rate_date = previous Polish business day
  4. merge_asof with NBP rates → amount_pln = amount_usd * rate
  5. FIFO or custom lot matching → proceeds, costs, gain
  6. Section G dividends + foreign tax
  7. PIT-38 field rounding per Ordynacja Podatkowa
"""

from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fidelity2pit38 import (
    calculate_pit38,
    calculate_pit38_fields,
    calculate_rate_dates,
    calculate_settlement_dates,
    compute_dividends_and_tax,
    compute_foreign_tax_capital_gains,
    compute_section_g_income_components,
    merge_with_rates,
    process_custom,
    process_fifo,
)

TWO_PLACES = Decimal("0.01")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_tx(rows):
    """Build a transaction DataFrame from (date, type, investment, shares, amount) tuples."""
    tx = pd.DataFrame(
        rows,
        columns=["Transaction date", "Transaction type", "Investment name", "Shares", "Amount"],
    )
    tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
    tx["trade_date"] = pd.to_datetime(tx["Transaction date"], format="%b-%d-%Y", errors="coerce")
    tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
    tx["amount_usd"] = pd.to_numeric(
        tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
    )
    tx["settlement_date"] = calculate_settlement_dates(tx["trade_date"], tx["Transaction type"])
    tx["rate_date"] = calculate_rate_dates(tx["settlement_date"])
    return tx


def _write_csv(tmp_path, filename, rows):
    """Write a Fidelity-style transaction CSV to tmp_path, return path string."""
    header = "Transaction date,Transaction type,Investment name,Shares,Amount"
    lines = [header] + [",".join(r) for r in rows]
    p = tmp_path / filename
    p.write_text("\n".join(lines) + "\n")
    return str(p)


def _write_custom_summary(tmp_path, filename, rows):
    """Write a tab-separated custom summary TXT, return path string."""
    header = "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source"
    lines = [header] + ["\t".join(r) for r in rows]
    p = tmp_path / filename
    p.write_text("\n".join(lines) + "\n")
    return str(p)


def _mock_urlopen_for(rates_df):
    """Return a context-manager patch that mocks urlopen with the given rates."""
    def _to_archive_csv(df):
        lines = ["data;1USD"]
        for row in df.sort_values("date").itertuples(index=False):
            lines.append(f"{row.date.strftime('%Y%m%d')};{str(f'{row.rate:.4f}').replace('.', ',')}")
        return ("\n".join(lines) + "\n").encode("cp1250")

    def _mock_urlopen(url, **kwargs):
        resp = MagicMock()
        resp.read.return_value = _to_archive_csv(rates_df)
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    return patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen)


def _round_tax(value):
    """Ordynacja art. 63 §1: round to full PLN (ROUND_HALF_UP)."""
    d = Decimal(str(value))
    if d < 0:
        return Decimal(0)
    return d.quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def _round_up_grosz(value):
    """Ordynacja art. 63 §1a: round up to grosze (ROUND_CEILING)."""
    return Decimal(str(value)).quantize(TWO_PLACES, rounding=ROUND_CEILING)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rates_2024():
    """Broad NBP rates for 2024 covering common rate-date needs."""
    return pd.DataFrame({
        "date": pd.to_datetime([
            "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            "2024-03-14", "2024-03-18", "2024-03-19", "2024-03-28",
            "2024-06-13", "2024-06-14", "2024-06-17",
            "2024-09-10", "2024-09-11", "2024-09-12", "2024-09-13", "2024-09-16",
            "2024-12-13", "2024-12-16", "2024-12-17", "2024-12-18",
            "2024-12-19", "2024-12-20", "2024-12-27", "2024-12-30", "2024-12-31",
        ]),
        "rate": [
            3.9200, 3.9250, 3.9300, 3.9350,
            3.9400, 3.9450, 3.9500, 3.9600,
            3.9800, 3.9900, 4.0000,
            3.8700, 3.8816, 3.8900, 3.8950, 3.9000,
            4.0500, 4.0571, 4.0600, 4.0621,
            4.0650, 4.0700, 4.0800, 4.0960, 4.1000,
        ],
    })


@pytest.fixture
def rates_2024_2025():
    """NBP rates spanning 2024–2025 for multi-year / cross-year tests."""
    return pd.DataFrame({
        "date": pd.to_datetime([
            "2024-09-10", "2024-09-11", "2024-09-12", "2024-09-13",
            "2024-12-13", "2024-12-16", "2024-12-17", "2024-12-18",
            "2024-12-19", "2024-12-20", "2024-12-30", "2024-12-31",
            "2025-01-02", "2025-01-03",
            "2025-02-17", "2025-02-18", "2025-02-19",
        ]),
        "rate": [
            3.8700, 3.8816, 3.8900, 3.8950,
            4.0500, 4.0571, 4.0600, 4.0621,
            4.0650, 4.0700, 4.0960, 4.1000,
            4.1100, 4.1200,
            4.0000, 4.0100, 4.0200,
        ],
    })


# ===========================================================================
# Test 1: Single RSU vest + sell — FIFO (zero cost basis)
# ===========================================================================

class TestSingleRsuVestAndSellFifo:
    """RSU vest (YOU BOUGHT RSU####) has $0 cost. Selling yields 100% gain."""

    def test_rsu_zero_cost_full_gain(self, rates_2024):
        tx = _build_tx([
            # RSU vest on Dec-13-2024 (T+0 settlement) → settles Dec-13
            # rate_date = prev Polish biz day = Dec-12 (Thu) → rate: 2024-09-13=3.8950 (asof)
            # Wait — we need a rate for Dec-12 → asof will pick 2024-09-16=3.9000? No.
            # Let me trace carefully:
            # RSU vest: trade_date = Dec-13-2024, type = "YOU BOUGHT RSU####"
            # Not a market trade → T+0 → settlement = Dec-13-2024
            # rate_date = prev Polish biz day before Dec-13 = Dec-12-2024 (Thu)
            # rates_2024 has no Dec-12. asof backward picks the most recent ≤ Dec-12:
            #   Sep-16 = 3.9000  (that's the latest before Dec-13 in our fixture)
            #   Wait, we have "2024-09-16" = 3.9000. Next is "2024-12-13" = 4.0500
            #   So for rate_date Dec-12, asof picks Sep-16 = 3.9000? That seems wrong.
            #   Let me add Dec-12 to our rates... Actually let me just use dates that work.
            # Simpler: use Dec-16-2024 for vest and Dec-17-2024 for sell.
            ("Dec-16-2024", "YOU BOUGHT RSU####", "ACME INC.", "30.00", "$0.00"),
            # Sell on Dec-17-2024 (T+1 post-SWITCH) → settles Dec-18
            # rate_date = prev Polish biz day before Dec-18 = Dec-17
            # rate on Dec-17 = 4.0600
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-30.00", "$4500.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)

        # Sell: 4500 * 4.0600 = 18270.00 proceeds
        # Buy (RSU): 0 * rate = 0 cost
        # gain = 18270.00
        assert proceeds == pytest.approx(18270.00, abs=0.01)
        assert costs == pytest.approx(0.0, abs=0.01)
        assert gain == pytest.approx(18270.00, abs=0.01)

        # PIT-38 fields
        dividends, foreign_tax_div = compute_dividends_and_tax(merged, year=2024)
        foreign_tax_cg = compute_foreign_tax_capital_gains(merged, year=2024)
        result = calculate_pit38_fields(
            proceeds, costs, gain, dividends, foreign_tax_div,
            foreign_tax_capital_gains=foreign_tax_cg,
        )

        assert result["poz22"] == Decimal("18270.00")
        assert result["poz23"] == Decimal("0.00")
        assert result["poz26"] == Decimal("18270.00")
        assert result["poz29"] == Decimal("18270")
        # 18270 * 0.19 = 3471.30
        assert result["poz31"] == Decimal("3471.30")
        assert result["poz32"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("3471")  # _round_tax(3471.30) → 3471


# ===========================================================================
# Test 2: ESPP buy + sell — FIFO (actual cost basis)
# ===========================================================================

class TestEsppBuyAndSellFifo:
    """ESPP purchase has a real cost basis. FIFO matches it to the sale."""

    def test_espp_cost_reduces_gain(self, rates_2024):
        # ESPP buy: Sep-13-2024, T+1 → settles Sep-16
        # rate_date = prev PL biz day before Sep-16 = Sep-13 → rate 3.8950
        # amount_pln = -4160 * 3.8950 = -16203.20
        tx = _build_tx([
            ("Sep-13-2024", "YOU BOUGHT ESPP### AS OF 09-13-24", "ACME INC.", "80.00", "-$4160.00"),
            # Sell Dec-17-2024, T+1 → settles Dec-18
            # rate_date = Dec-17 → rate 4.0600
            # amount_pln = 5340 * 4.0600 = 21680.40
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-30.00", "$5340.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)

        # sale: 30 shares at 5340/30 = $178/share
        # proceeds = 30 * (5340 * 4.0600 / 30) = 5340 * 4.0600 = 21680.40
        # buy cost_per = (-(-4160 * 3.8950)) / 80 = 16203.20 / 80 = 202.54
        # cost = 30 * 202.54 = 6076.20
        expected_proceeds = round(5340.0 * 4.0600, 2)  # 21680.40
        buy_amount_pln = -4160.0 * 3.8950  # -16203.20
        cost_per_share = (-buy_amount_pln) / 80  # 202.54
        expected_costs = round(30 * cost_per_share, 2)  # 6076.20
        expected_gain = round(expected_proceeds - expected_costs, 2)

        assert proceeds == pytest.approx(expected_proceeds, abs=0.01)
        assert costs == pytest.approx(expected_costs, abs=0.01)
        assert gain == pytest.approx(expected_gain, abs=0.01)


# ===========================================================================
# Test 3: Multiple buys consumed by single sell — FIFO order
# ===========================================================================

class TestFifoMultipleBuysOneSell:
    """FIFO should consume the earliest buy first, then the next."""

    def test_fifo_order_matters(self, rates_2024):
        tx = _build_tx([
            # Buy #1: Mar-18-2024 (before SWITCH → T+2) → settles Mar-20
            # rate_date = prev PL biz day before Mar-20 = Mar-19 → rate 3.9500
            # amount_pln = -1000 * 3.9500 = -3950.00
            ("Mar-18-2024", "YOU BOUGHT", "ACME INC.", "10.00", "-$1000.00"),
            # Buy #2: Sep-13-2024 (after SWITCH → T+1) → settles Sep-16
            # rate_date = Sep-13 → rate 3.8950
            # amount_pln = -1500 * 3.8950 = -5842.50
            ("Sep-13-2024", "YOU BOUGHT", "ACME INC.", "10.00", "-$1500.00"),
            # Sell: Dec-17-2024 (T+1) → settles Dec-18
            # rate_date = Dec-17 → rate 4.0600
            # amount_pln = 3000 * 4.0600 = 12180.00
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-15.00", "$3000.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)

        # sale: price_per = 12180.00 / 15 = 812.00
        # FIFO: 10 shares from buy#1, 5 shares from buy#2
        # lot1: proceeds = 10 * 812 = 8120.00; cost = 10 * (3950/10) = 3950.00
        # lot2: proceeds = 5 * 812 = 4060.00;  cost = 5 * (5842.50/10) = 2921.25
        # total_proceeds = 12180.00; total_costs = 3950.00 + 2921.25 = 6871.25
        expected_proceeds = round(3000.0 * 4.0600, 2)
        price_per = expected_proceeds / 15.0
        cost1 = round(10 * (1000.0 * 3.9500 / 10), 2)
        cost2 = round(5 * (1500.0 * 3.8950 / 10), 2)
        expected_costs = cost1 + cost2
        expected_gain = round(expected_proceeds - expected_costs, 2)

        assert proceeds == pytest.approx(expected_proceeds, abs=0.01)
        assert costs == pytest.approx(expected_costs, abs=0.01)
        assert gain == pytest.approx(expected_gain, abs=0.01)


# ===========================================================================
# Test 4: Dividends only (no sales) — Section G populated, Section C/D zero
# ===========================================================================

class TestDividendsOnlyNoSales:
    """When there are only dividends, capital gains fields are all zero."""

    def test_section_g_only(self, rates_2024):
        tx = _build_tx([
            # Dividend: Dec-17-2024 (T+0) → settles Dec-17
            # rate_date = Dec-16 → rate 4.0571
            # amount_pln = 100 * 4.0571 = 405.71
            ("Dec-17-2024", "DIVIDEND RECEIVED", "ACME INC.", "-", "$100.00"),
            # Tax: Dec-17-2024 (T+0) → same settlement/rate
            # amount_pln = -15 * 4.0571 = -60.8565
            ("Dec-17-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "ACME INC.", "-", "-$15.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)
        assert proceeds == 0
        assert costs == 0
        assert gain == 0

        dividends, foreign_tax_div = compute_dividends_and_tax(merged, year=2024)
        # dividend: abs(100 * 4.0571) = 405.71
        assert dividends == pytest.approx(405.71, abs=0.01)
        # tax: -(-15 * 4.0571) = 60.86 (rounded)
        assert foreign_tax_div == pytest.approx(60.86, abs=0.01)

        foreign_tax_cg = compute_foreign_tax_capital_gains(merged, year=2024)
        assert foreign_tax_cg == 0.0

        result = calculate_pit38_fields(
            proceeds, costs, gain, dividends, foreign_tax_div,
            foreign_tax_capital_gains=foreign_tax_cg,
        )
        # Section C/D all zero
        assert result["poz22"] == Decimal("0.00")
        assert result["poz23"] == Decimal("0.00")
        assert result["poz26"] == Decimal("0.00")
        assert result["poz29"] == Decimal("0")
        assert result["poz31"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("0")

        # Section G
        # poz45 = ceil(405.71 * 0.19) = ceil(77.0849) = 77.09
        poz45_expected = _round_up_grosz(Decimal("405.71") * Decimal("0.19"))
        assert result["poz45"] == poz45_expected
        # poz46 = min(60.86, 77.09) = 60.86
        assert result["poz46"] == Decimal("60.86")
        # poz47 = _round_tax(77.09 - 60.86) = _round_tax(16.23) = 16
        assert result["poz47"] == Decimal("16")


# ===========================================================================
# Test 5: Mixed equity dividends + fund distributions — Section G breakdown
# ===========================================================================

class TestSectionGBreakdown:
    """Section G should separately track equity dividends and fund distributions."""

    def test_equity_vs_fund_classification(self, rates_2024):
        tx = _build_tx([
            # Equity dividend
            ("Dec-17-2024", "DIVIDEND RECEIVED", "ACME INC.", "-", "$200.00"),
            # Fund distribution (name contains "FUND")
            ("Dec-17-2024", "DIVIDEND RECEIVED", "FIDELITY GOVERNMENT CASH RESERVES", "-", "$50.00"),
            # Withholding on equity
            ("Dec-17-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "ACME INC.", "-", "-$30.00"),
            # Withholding on fund
            ("Dec-17-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "FIDELITY GOVERNMENT CASH RESERVES", "-", "-$7.50"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        components = compute_section_g_income_components(merged, year=2024)

        # All settle Dec-17, rate_date = Dec-16, rate = 4.0571
        rate = 4.0571
        expected_equity = round(abs(200.0 * rate), 2)   # 811.42
        expected_fund = round(abs(50.0 * rate), 2)       # 202.86 (approx)
        expected_total = round(expected_equity + expected_fund, 2)
        expected_tax = round(30.0 * rate + 7.5 * rate, 2)  # 152.14 (approx)

        assert components["section_g_equity_dividends"] == pytest.approx(expected_equity, abs=0.01)
        assert components["section_g_fund_distributions"] == pytest.approx(expected_fund, abs=0.01)
        assert components["section_g_total_income"] == pytest.approx(expected_total, abs=0.01)
        assert components["section_g_foreign_tax"] == pytest.approx(expected_tax, abs=0.01)

        # Check that the fund distribution is classified because of "RESERVES" in name
        # (the _is_fund_like_investment heuristic checks for "FUND", "CASH RESERVES", etc.)
        assert components["section_g_fund_distributions"] > 0


# ===========================================================================
# Test 6: Full pipeline via calculate_pit38() — FIFO mode with mocked NBP
# ===========================================================================

class TestCalculatePit38FullPipelineFifo:
    """Test the top-level calculate_pit38() function end-to-end."""

    def test_full_pipeline_fifo(self, tmp_path, rates_2024):
        csv_path = _write_csv(tmp_path, "Transaction history 2024.csv", [
            ("Dec-16-2024", "YOU BOUGHT RSU####", "ACME INC.", "20.00", "$0.00"),
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-20.00", "$3000.00"),
            ("Dec-17-2024", "DIVIDEND RECEIVED", "ACME INC.", "-", "$50.00"),
            ("Dec-17-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "ACME INC.", "-", "-$7.50"),
        ])

        with _mock_urlopen_for(rates_2024):
            result = calculate_pit38(tx_csv=csv_path, year=2024, method="fifo")

        assert result.year == 2024

        # Sell settles Dec-18, rate_date Dec-17 → rate 4.0600
        # proceeds = 3000 * 4.0600 = 12180.00
        assert result["poz22"] == Decimal("12180.00")
        # RSU cost = 0
        assert result["poz23"] == Decimal("0.00")
        assert result["poz26"] == Decimal("12180.00")
        assert result["poz29"] == Decimal("12180")
        # 12180 * 0.19 = 2314.20
        assert result["poz31"] == Decimal("2314.20")
        assert result["tax_final"] == Decimal("2314")

        # Dividends: rate_date Dec-16 → rate 4.0571
        # poz45 = ceil(50 * 4.0571 * 0.19) = ceil(202.855 * 0.19) = ceil(38.54245) = 38.55
        div_income = Decimal(str(round(50.0 * 4.0571, 2)))
        poz45_expected = _round_up_grosz(div_income * Decimal("0.19"))
        assert result["poz45"] == poz45_expected


# ===========================================================================
# Test 7: Full pipeline via calculate_pit38() — custom mode
# ===========================================================================

class TestCalculatePit38FullPipelineCustom:
    """Test calculate_pit38() with custom lot matching."""

    def test_full_pipeline_custom(self, tmp_path, rates_2024):
        csv_path = _write_csv(tmp_path, "Transaction history 2024.csv", [
            # ESPP buy
            ("Sep-13-2024", "YOU BOUGHT ESPP### AS OF 09-13-24", "ACME INC.", "40.00", "-$2000.00"),
            # RSU vest
            ("Dec-16-2024", "YOU BOUGHT RSU####", "ACME INC.", "20.00", "$0.00"),
            # Sell 30 shares
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-30.00", "$4500.00"),
        ])
        # Custom summary: 20 from RSU + 10 from ESPP
        summary_path = _write_custom_summary(tmp_path, "stock-sales-2024.txt", [
            ("Dec-17-2024", "Dec-16-2024", "20.0000", "$0.00", "$3000.00", "$3000.00", "RS"),
            ("Dec-17-2024", "Sep-13-2024", "10.0000", "$500.00", "$1500.00", "$1000.00", "SP"),
        ])

        with _mock_urlopen_for(rates_2024):
            result = calculate_pit38(
                tx_csv=csv_path, year=2024, method="custom",
                custom_summary=[summary_path],
            )

        # Sell settles Dec-18, rate_date Dec-17 → rate 4.0600
        # price_per_share = (4500 * 4.0600) / 30 = 18270.00 / 30 = 609.00
        # RS lot: 20 shares, proceeds = 20 * 609 = 12180.00, cost = 0
        # SP lot: 10 shares, proceeds = 10 * 609 = 6090.00
        #   cost = Cost basis USD $500 * buy_rate
        #   ESPP buy settles Sep-16, rate_date = Sep-13 → rate 3.8950
        #   cost = 500 * 3.8950 = 1947.50
        expected_proceeds = round(20 * 609.0, 2) + round(10 * 609.0, 2)  # 18270.00
        expected_costs = 0.0 + round(500.0 * 3.8950, 2)  # 1947.50
        expected_gain = round(expected_proceeds - expected_costs, 2)

        assert result["poz22"] == Decimal(str(expected_proceeds))
        assert result["poz23"] == Decimal(str(expected_costs))
        assert result["poz26"] == Decimal(str(expected_gain))


# ===========================================================================
# Test 8: Cross-year FIFO — buy in 2024, sell in 2025
# ===========================================================================

class TestCrossYearFifoE2E:
    """Buys from 2024 should be available for FIFO matching in 2025."""

    def test_cross_year_buy_sell(self, rates_2024_2025):
        tx = _build_tx([
            # Buy in 2024: Dec-17-2024 (T+1) → settles Dec-18
            # rate_date = Dec-17 → rate 4.0600
            ("Dec-17-2024", "YOU BOUGHT", "ACME INC.", "10.00", "-$1000.00"),
            # Sell in 2025: Feb-18-2025 (T+1) → settles Feb-19
            # rate_date = prev PL biz day before Feb-19 = Feb-18 → rate 4.0100
            ("Feb-18-2025", "YOU SOLD", "ACME INC.", "-10.00", "$1500.00"),
        ])
        merged = merge_with_rates(tx, rates_2024_2025)

        # Year 2025: the 2024 buy should be available
        proceeds, costs, gain = process_fifo(merged, year=2025)

        # sell amount_pln = 1500 * 4.0100 = 6015.00
        # buy cost_per = (1000 * 4.0600) / 10 = 406.00
        # cost = 10 * 406.00 = 4060.00
        # gain = 6015.00 - 4060.00 = 1955.00
        expected_proceeds = round(1500.0 * 4.0100, 2)
        expected_costs = round(10 * (1000.0 * 4.0600 / 10), 2)
        expected_gain = round(expected_proceeds - expected_costs, 2)

        assert proceeds == pytest.approx(expected_proceeds, abs=0.01)
        assert costs == pytest.approx(expected_costs, abs=0.01)
        assert gain == pytest.approx(expected_gain, abs=0.01)

        # Year 2024: no sell in 2024 → all zeros
        p24, c24, g24 = process_fifo(merged, year=2024)
        assert p24 == 0
        assert c24 == 0
        assert g24 == 0


# ===========================================================================
# Test 9: Pre-SWITCH_DATE T+2 settlement vs post-SWITCH T+1
# ===========================================================================

class TestSettlementDateRegimeChange:
    """Verify T+2 before 2024-05-28 and T+1 on/after 2024-05-28."""

    def test_t2_vs_t1_affects_rate_and_pln(self, rates_2024):
        tx = _build_tx([
            # Pre-SWITCH: Mar-18-2024 (Mon) T+2 → settles Mar-20 (Wed)
            # rate_date = prev PL biz day before Mar-20 = Mar-19 → rate 3.9500
            ("Mar-18-2024", "YOU BOUGHT", "ACME INC.", "10.00", "-$1000.00"),
            # Post-SWITCH: Sep-13-2024 (Fri) T+1 → settles Sep-16 (Mon)
            # rate_date = prev PL biz day before Sep-16 = Sep-13 → rate 3.8950
            ("Sep-13-2024", "YOU BOUGHT", "ACME INC.", "10.00", "-$1000.00"),
            # Sell post-SWITCH: Dec-17-2024 (Tue) T+1 → settles Dec-18 (Wed)
            # rate_date = Dec-17 → rate 4.0600
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-20.00", "$4000.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        # Verify settlement dates
        buy1 = tx[tx["trade_date"] == pd.Timestamp("2024-03-18")]
        buy2 = tx[tx["trade_date"] == pd.Timestamp("2024-09-13")]
        sell = tx[tx["trade_date"] == pd.Timestamp("2024-12-17")]

        assert buy1["settlement_date"].iloc[0] == pd.Timestamp("2024-03-20")  # T+2
        assert buy2["settlement_date"].iloc[0] == pd.Timestamp("2024-09-16")  # T+1
        assert sell["settlement_date"].iloc[0] == pd.Timestamp("2024-12-18")  # T+1

        # FIFO should use different rates for each buy
        proceeds, costs, gain = process_fifo(merged, year=2024)

        # proceeds = 4000 * 4.0600 = 16240.00
        # FIFO: 10 from buy#1 (rate 3.9500) + 10 from buy#2 (rate 3.8950)
        # cost1 = 10 * (1000*3.9500/10) = 3950.00
        # cost2 = 10 * (1000*3.8950/10) = 3895.00
        # total_costs = 7845.00
        expected_costs = round(1000.0 * 3.9500, 2) + round(1000.0 * 3.8950, 2)
        assert costs == pytest.approx(expected_costs, abs=0.01)
        assert proceeds == pytest.approx(4000.0 * 4.0600, abs=0.01)


# ===========================================================================
# Test 10: PIT-38 tax rounding edge cases
# ===========================================================================

class TestPit38TaxRounding:
    """Verify Ordynacja Podatkowa rounding rules for edge cases."""

    def test_rounding_boundary_49_vs_50_groszy(self):
        """Art. 63 §1: < 50 groszy rounds down, >= 50 groszy rounds up."""
        # gain that produces tax_base.49 → rounds down
        # poz26 = 100.49 → poz29 = _round_tax(100.49) = 100
        # poz31 = 100 * 0.19 = 19.00
        # tax_final = _round_tax(19.00) = 19
        result_low = calculate_pit38_fields(
            total_proceeds=100.49, total_costs=0, total_gain=100.49,
            total_dividends=0, foreign_tax_dividends=0,
        )
        assert result_low["poz29"] == Decimal("100")  # .49 rounds down

        # poz26 = 100.50 → poz29 = _round_tax(100.50) = 101
        result_high = calculate_pit38_fields(
            total_proceeds=100.50, total_costs=0, total_gain=100.50,
            total_dividends=0, foreign_tax_dividends=0,
        )
        assert result_high["poz29"] == Decimal("101")  # .50 rounds up

    def test_section_g_ceiling_rounding(self):
        """Art. 63 §1a: Poz. 45 rounds UP to the nearest grosz."""
        # dividends = 100.00
        # poz45 = ceil(100.00 * 0.19) = ceil(19.00) = 19.00 (exact, no rounding needed)
        result_exact = calculate_pit38_fields(
            total_proceeds=0, total_costs=0, total_gain=0,
            total_dividends=100, foreign_tax_dividends=0,
        )
        assert result_exact["poz45"] == Decimal("19.00")

        # dividends = 100.01
        # poz45 = ceil(100.01 * 0.19) = ceil(19.0019) = 19.01
        result_ceil = calculate_pit38_fields(
            total_proceeds=0, total_costs=0, total_gain=0,
            total_dividends=100.01, foreign_tax_dividends=0,
        )
        assert result_ceil["poz45"] == Decimal("19.01")

    def test_foreign_tax_credit_capped_at_polish_tax(self):
        """Poz. 32 (foreign tax credit) is capped at Poz. 31 (Polish tax)."""
        # gain = 100, tax = 100 * 0.19 = 19.00
        # foreign_tax_capital_gains = 50 (exceeds 19.00)
        # poz32 should be capped at 19.00
        result = calculate_pit38_fields(
            total_proceeds=100, total_costs=0, total_gain=100,
            total_dividends=0, foreign_tax_dividends=0,
            foreign_tax_capital_gains=50,
        )
        assert result["poz32"] == Decimal("19.00")
        assert result["tax_final"] == Decimal("0")

    def test_negative_gain_results_in_zero_tax(self):
        """Negative gain (loss) should result in zero tax."""
        result = calculate_pit38_fields(
            total_proceeds=500, total_costs=800, total_gain=-300,
            total_dividends=0, foreign_tax_dividends=0,
        )
        assert result["poz26"] == Decimal("-300.00")
        assert result["poz29"] == Decimal("0")  # _round_tax(-300) → 0
        assert result["poz31"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("0")


# ===========================================================================
# Test 11: Multi-stock FIFO isolation — each stock matched independently
# ===========================================================================

class TestMultiStockFifoIsolation:
    """FIFO matches buys to sells per investment name, not across stocks."""

    def test_two_stocks_matched_independently(self, rates_2024):
        tx = _build_tx([
            # Stock AAA: buy cheap
            # Dec-16-2024 RSU vest → T+0 settles Dec-16, rate_date Dec-13 → rate 4.0500
            ("Dec-16-2024", "YOU BOUGHT RSU####", "AAA INC.", "10.00", "$0.00"),
            # Stock BBB: buy expensive
            # Sep-13-2024 ESPP → T+1 settles Sep-16, rate_date Sep-13 → rate 3.8950
            ("Sep-13-2024", "YOU BOUGHT ESPP### AS OF 09-13-24", "BBB INC.", "10.00", "-$2000.00"),
            # Sell AAA
            # Dec-17-2024 → T+1 settles Dec-18, rate_date Dec-17 → rate 4.0600
            ("Dec-17-2024", "YOU SOLD", "AAA INC.", "-10.00", "$1500.00"),
            # Sell BBB
            # Dec-18-2024 → T+1 settles Dec-19, rate_date Dec-18 → rate 4.0621
            ("Dec-18-2024", "YOU SOLD", "BBB INC.", "-10.00", "$2500.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)

        # AAA: proceeds = 1500 * 4.0600 = 6090.00, cost = 0 (RSU)
        # BBB: proceeds = 2500 * 4.0621 = 10155.25, cost = 10*(2000*3.8950/10) = 7790.00
        aaa_proceeds = round(1500.0 * 4.0600, 2)
        bbb_proceeds = round(2500.0 * 4.0621, 2)
        aaa_cost = 0.0
        bbb_cost = round(2000.0 * 3.8950, 2)

        assert proceeds == pytest.approx(aaa_proceeds + bbb_proceeds, abs=0.01)
        assert costs == pytest.approx(aaa_cost + bbb_cost, abs=0.01)
        assert gain == pytest.approx(
            (aaa_proceeds - aaa_cost) + (bbb_proceeds - bbb_cost), abs=0.01
        )

    def test_sell_stock_a_does_not_consume_stock_b_lots(self, rates_2024):
        """Selling stock A must not eat into stock B buy lots."""
        tx = _build_tx([
            # Only buy stock BBB
            ("Dec-16-2024", "YOU BOUGHT RSU####", "BBB INC.", "10.00", "$0.00"),
            # Sell stock AAA — no AAA buys exist
            ("Dec-17-2024", "YOU SOLD", "AAA INC.", "-5.00", "$750.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        # FIFO should find no matching buy lots for AAA (BBB lots should not be used)
        # The code logs an error and breaks the loop, resulting in 0 proceeds for the
        # unmatched sale.
        proceeds, costs, gain = process_fifo(merged, year=2024)
        assert proceeds == 0
        assert costs == 0


# ===========================================================================
# Test 12: Sample data full pipeline — FIFO via calculate_pit38()
# ===========================================================================

class TestSampleDataFullPipeline:
    """Run the full pipeline on data-sample/ files with mocked NBP."""

    def test_sample_data_fifo(self, example_tx_csv_path, mock_nbp_read_csv):
        with mock_nbp_read_csv:
            result = calculate_pit38(
                tx_csv=example_tx_csv_path, year=2024, method="fifo",
            )

        assert result.year == 2024
        # Section C/D
        assert result["poz22"] == Decimal("34033.91")
        assert result["poz23"] == Decimal("8865.00")
        assert result["poz26"] == Decimal("25168.91")
        assert result["poz29"] == Decimal("25169")
        assert result["poz31"] == Decimal("4782.11")
        assert result["poz32"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("4782")
        # PIT-ZG
        assert result["pitzg_poz29"] == Decimal("25168.91")
        assert result["pitzg_poz30"] == Decimal("0.00")
        # Section G: sample data has only fund dividends (FIDELITY GOVERNMENT CASH RESERVES)
        assert result["section_g_equity_dividends"] == Decimal("0.00")
        assert result["section_g_fund_distributions"] > Decimal("0")

    def test_sample_data_custom(
        self, example_tx_csv_path, example_custom_summary_path, mock_nbp_read_csv,
    ):
        with mock_nbp_read_csv:
            result = calculate_pit38(
                tx_csv=example_tx_csv_path, year=2024, method="custom",
                custom_summary=[example_custom_summary_path],
            )

        assert result.year == 2024
        assert result["poz22"] == Decimal("34033.92")
        assert result["poz23"] == Decimal("5571.40")
        assert result["poz26"] == Decimal("28462.52")
        assert result["poz29"] == Decimal("28463")
        assert result["poz31"] == Decimal("5407.97")
        assert result["tax_final"] == Decimal("5408")


# ===========================================================================
# Test 13: Partial sell — remaining shares left in FIFO pool
# ===========================================================================

class TestPartialSellLeavesRemainder:
    """Selling fewer shares than bought leaves remainder for future sells."""

    def test_two_sells_from_one_lot(self, rates_2024):
        tx = _build_tx([
            # Buy 50 shares via ESPP
            # Sep-13-2024 → T+1 settles Sep-16, rate_date Sep-13 → rate 3.8950
            ("Sep-13-2024", "YOU BOUGHT ESPP### AS OF 09-13-24", "ACME INC.", "50.00", "-$5000.00"),
            # Sell #1: 20 shares on Dec-17
            # Dec-17-2024 → T+1 settles Dec-18, rate_date Dec-17 → rate 4.0600
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-20.00", "$3000.00"),
            # Sell #2: 10 shares on Dec-18
            # Dec-18-2024 → T+1 settles Dec-19, rate_date Dec-18 → rate 4.0621
            ("Dec-18-2024", "YOU SOLD", "ACME INC.", "-10.00", "$1600.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)

        # Buy: amount_pln = -5000 * 3.8950 = -19475.00
        # cost_per_share = 19475.00 / 50 = 389.50
        cost_per = 5000.0 * 3.8950 / 50  # 389.50

        # Sell #1: proceeds = 3000 * 4.0600 = 12180.00, cost = 20 * 389.50 = 7790.00
        sell1_proceeds = round(3000.0 * 4.0600, 2)
        sell1_cost = round(20 * cost_per, 2)

        # Sell #2: proceeds = 1600 * 4.0621 = 6499.36, cost = 10 * 389.50 = 3895.00
        sell2_proceeds = round(1600.0 * 4.0621, 2)
        sell2_cost = round(10 * cost_per, 2)

        expected_proceeds = sell1_proceeds + sell2_proceeds
        expected_costs = sell1_cost + sell2_cost
        expected_gain = round(expected_proceeds - expected_costs, 2)

        assert proceeds == pytest.approx(expected_proceeds, abs=0.01)
        assert costs == pytest.approx(expected_costs, abs=0.01)
        assert gain == pytest.approx(expected_gain, abs=0.01)

        # 20 shares still remain in the FIFO pool (50 - 20 - 10 = 20)
        # We can verify this indirectly: total sold = 30 < 50 bought


# ===========================================================================
# Test 14: Foreign tax on capital gains flows into Poz. 32 and PIT-ZG
# ===========================================================================

class TestForeignTaxCapitalGainsCredit:
    """NON-RESIDENT TAX (non-dividend) should reduce capital gains tax via Poz. 32."""

    def test_capital_gains_tax_credit(self, rates_2024):
        tx = _build_tx([
            ("Dec-16-2024", "YOU BOUGHT RSU####", "ACME INC.", "10.00", "$0.00"),
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-10.00", "$2000.00"),
            # Foreign tax on capital gains (not dividend-related)
            # Dec-17-2024 → T+0, settles Dec-17, rate_date Dec-16 → rate 4.0571
            ("Dec-17-2024", "NON-RESIDENT TAX ON CAPITAL GAIN", "ACME INC.", "-", "-$50.00"),
        ])
        merged = merge_with_rates(tx, rates_2024)

        proceeds, costs, gain = process_fifo(merged, year=2024)
        # Sell settles Dec-18, rate_date Dec-17 → rate 4.0600
        # proceeds = 2000 * 4.0600 = 8120.00
        assert proceeds == pytest.approx(2000.0 * 4.0600, abs=0.01)
        assert costs == pytest.approx(0.0, abs=0.01)

        dividends, foreign_tax_div = compute_dividends_and_tax(merged, year=2024)
        foreign_tax_cg = compute_foreign_tax_capital_gains(merged, year=2024)

        # Foreign tax on CG: -(-50 * 4.0571) = 202.86
        assert foreign_tax_cg == pytest.approx(50.0 * 4.0571, abs=0.01)
        assert dividends == 0.0
        assert foreign_tax_div == 0.0

        result = calculate_pit38_fields(
            proceeds, costs, gain, dividends, foreign_tax_div,
            foreign_tax_capital_gains=foreign_tax_cg,
        )

        # poz22 = 8120.00, poz23 = 0, poz26 = 8120.00
        # poz29 = 8120, poz31 = 8120 * 0.19 = 1542.80
        # poz32 = min(202.86, 1542.80) = 202.86
        # tax_final = _round_tax(1542.80 - 202.86) = _round_tax(1339.94) = 1340
        expected_foreign_tax = Decimal(str(round(50.0 * 4.0571, 2)))
        assert result["poz32"] == expected_foreign_tax
        assert result["poz31"] == Decimal("1542.80")
        assert result["tax_final"] == Decimal("1340")

        # PIT-ZG should reflect the same foreign tax
        assert result["pitzg_poz30"] == expected_foreign_tax
        assert result["pitzg_poz29"] == Decimal("8120.00")


# ===========================================================================
# Test 15: Multi-CSV loading through full pipeline
# ===========================================================================

class TestMultiCsvFullPipeline:
    """Two separate CSV files combined into a single pipeline run."""

    def test_two_csvs_combined(self, tmp_path, rates_2024):
        # File 1: buy in September
        csv1 = _write_csv(tmp_path, "Transaction history 2024a.csv", [
            ("Sep-13-2024", "YOU BOUGHT ESPP### AS OF 09-13-24", "ACME INC.", "20.00", "-$2000.00"),
        ])
        # File 2: sell in December + dividend
        csv2 = _write_csv(tmp_path, "Transaction history 2024b.csv", [
            ("Dec-17-2024", "YOU SOLD", "ACME INC.", "-20.00", "$3600.00"),
            ("Dec-17-2024", "DIVIDEND RECEIVED", "ACME INC.", "-", "$25.00"),
            ("Dec-17-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "ACME INC.", "-", "-$3.75"),
        ])

        with _mock_urlopen_for(rates_2024):
            result = calculate_pit38(
                tx_csv=[csv1, csv2], year=2024, method="fifo",
            )

        # Buy: Sep-13 → T+1 settles Sep-16, rate_date Sep-13 → rate 3.8950
        # cost = 2000 * 3.8950 = 7790.00
        # Sell: Dec-17 → T+1 settles Dec-18, rate_date Dec-17 → rate 4.0600
        # proceeds = 3600 * 4.0600 = 14616.00
        expected_proceeds = Decimal(str(round(3600.0 * 4.0600, 2)))
        expected_costs = Decimal(str(round(2000.0 * 3.8950, 2)))
        expected_gain = expected_proceeds - expected_costs

        assert result["poz22"] == expected_proceeds
        assert result["poz23"] == expected_costs
        assert result["poz26"] == expected_gain

        # Dividends should also be present
        # Div: Dec-17 → T+0, rate_date Dec-16 → rate 4.0571
        # income = 25 * 4.0571 = 101.43
        assert result["section_g_total_income"] > Decimal("0")
        assert result["section_g_equity_dividends"] > Decimal("0")

        # Tax: poz45 = ceil(101.43 * 0.19) = ceil(19.2717) = 19.28
        div_income = Decimal(str(round(25.0 * 4.0571, 2)))
        poz45_expected = _round_up_grosz(div_income * Decimal("0.19"))
        assert result["poz45"] == poz45_expected
