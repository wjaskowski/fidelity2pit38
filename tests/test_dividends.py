import pandas as pd
import pytest

from fidelity2pit38 import compute_dividends_and_tax, compute_section_g_income_components


def test_example_data(merged_example):
    dividends, foreign_tax = compute_dividends_and_tax(merged_example)
    assert dividends == pytest.approx(52.47, abs=0.01)
    assert foreign_tax == pytest.approx(7.86, abs=0.01)


def test_gross_dividend_only():
    merged = pd.DataFrame(
        {
            "Transaction type": ["DIVIDEND RECEIVED"],
            "amount_pln": [100.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    assert dividends == pytest.approx(100.0)
    assert foreign_tax == pytest.approx(0.0)


def test_reinvestment_excluded_from_taxable_dividends():
    merged = pd.DataFrame(
        {
            "Transaction type": ["DIVIDEND RECEIVED", "REINVESTMENT REINVEST @ $1.000"],
            "amount_pln": [50.0, -30.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    assert dividends == pytest.approx(50.0)


def test_no_dividends():
    merged = pd.DataFrame(
        {
            "Transaction type": ["YOU SOLD", "YOU BOUGHT"],
            "amount_pln": [1000.0, -800.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    assert dividends == pytest.approx(0.0)
    assert foreign_tax == pytest.approx(0.0)


def test_foreign_tax_non_resident():
    """NON-RESIDENT TAX DIVIDEND RECEIVED counted once via contains('NON-RESIDENT TAX')."""
    merged = pd.DataFrame(
        {
            "Transaction type": ["NON-RESIDENT TAX DIVIDEND RECEIVED"],
            "amount_pln": [-10.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    # Single filter: contains "NON-RESIDENT TAX" -> -(-10) = 10
    assert foreign_tax == pytest.approx(10.0)


def test_multiple_non_resident_tax_rows():
    """Multiple withholding tax rows are summed correctly."""
    merged = pd.DataFrame(
        {
            "Transaction type": [
                "NON-RESIDENT TAX DIVIDEND RECEIVED",
                "NON-RESIDENT TAX DIVIDEND RECEIVED",
            ],
            "amount_pln": [-10.0, -5.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    assert foreign_tax == pytest.approx(15.0)


def test_dividends_not_in_foreign_tax():
    """DIVIDEND RECEIVED should not be picked up by foreign tax filter."""
    merged = pd.DataFrame(
        {
            "Transaction type": ["DIVIDEND RECEIVED"],
            "amount_pln": [100.0],
        }
    )
    dividends, foreign_tax = compute_dividends_and_tax(merged)
    assert dividends == pytest.approx(100.0)
    assert foreign_tax == pytest.approx(0.0)


def test_section_g_breakdown_fund_vs_equity():
    merged = pd.DataFrame(
        {
            "Transaction type": ["DIVIDEND RECEIVED", "DIVIDEND RECEIVED"],
            "Investment name": ["ACME TECHNOLOGY INC. COMMON STOCK", "FID TREASURY ONLY MMKT FUND CL OUS"],
            "amount_pln": [12.0, 34.0],
            "settlement_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        }
    )
    comp = compute_section_g_income_components(merged)
    assert comp["section_g_equity_dividends"] == pytest.approx(12.0)
    assert comp["section_g_fund_distributions"] == pytest.approx(34.0)
    assert comp["section_g_total_income"] == pytest.approx(46.0)


def test_section_g_breakdown_in_example_is_fund_only(merged_example):
    comp = compute_section_g_income_components(merged_example)
    assert comp["section_g_total_income"] == pytest.approx(52.47, abs=0.01)
    assert comp["section_g_equity_dividends"] == pytest.approx(0.0)
    assert comp["section_g_fund_distributions"] == pytest.approx(52.47, abs=0.01)
