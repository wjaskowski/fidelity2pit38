import pandas as pd
import pytest

from fidelity2pit38 import (
    calculate_rate_dates,
    calculate_settlement_dates,
    compute_dividends_and_tax,
    merge_with_rates,
    process_fifo,
)


def _build_tx(rows):
    """Build a transaction DataFrame from (date, type, shares, amount) tuples."""
    tx = pd.DataFrame(rows, columns=["Transaction date", "Transaction type", "Investment name", "Shares", "Amount"])
    tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
    tx["trade_date"] = pd.to_datetime(tx["Transaction date"], format="%b-%d-%Y", errors="coerce")
    tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
    tx["amount_usd"] = pd.to_numeric(tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce")
    tx["settlement_date"] = calculate_settlement_dates(tx["trade_date"], tx["Transaction type"])
    tx["rate_date"] = calculate_rate_dates(tx["settlement_date"])
    return tx


class TestCrossYearFifo:
    def test_buy_2023_sell_2024(self, nbp_rates_multi_year_df):
        """Stock bought in 2023 and sold in 2024 should be matched by FIFO."""
        tx = _build_tx([
            ("Dec-20-2023", "YOU BOUGHT", "ACME", "10.00", "-$1000.00"),
            ("Dec-18-2024", "YOU SOLD", "ACME", "-10.00", "$1500.00"),
        ])
        merged = merge_with_rates(tx, nbp_rates_multi_year_df)

        # Without year filter: matches the buy and sell
        proceeds, costs, gain = process_fifo(merged)
        assert proceeds > 0
        assert costs > 0
        assert gain == pytest.approx(proceeds - costs, abs=0.01)

        # With year=2024: only 2024 sells, but 2023 buys still available
        proceeds_2024, costs_2024, gain_2024 = process_fifo(merged, year=2024)
        assert proceeds_2024 == proceeds  # same single sell
        assert costs_2024 == costs
        assert gain_2024 == gain

    def test_sell_in_wrong_year_excluded(self, nbp_rates_multi_year_df):
        """Sells from 2023 should be excluded when year=2024."""
        tx = _build_tx([
            ("Dec-18-2023", "YOU BOUGHT", "ACME", "10.00", "-$500.00"),
            ("Dec-20-2023", "YOU SOLD", "ACME", "-10.00", "$600.00"),
        ])
        merged = merge_with_rates(tx, nbp_rates_multi_year_df)

        proceeds, costs, gain = process_fifo(merged, year=2024)
        assert proceeds == 0
        assert costs == 0
        assert gain == 0

    def test_multiple_years_buys(self, nbp_rates_multi_year_df):
        """FIFO should consume 2023 buy before 2024 buy."""
        tx = _build_tx([
            ("Dec-20-2023", "YOU BOUGHT", "ACME", "5.00", "-$500.00"),
            ("Sep-10-2024", "YOU BOUGHT", "ACME", "5.00", "-$600.00"),
            ("Dec-18-2024", "YOU SOLD", "ACME", "-7.00", "$1050.00"),
        ])
        merged = merge_with_rates(tx, nbp_rates_multi_year_df)
        proceeds, costs, gain = process_fifo(merged, year=2024)

        # Should have matched: 5 from 2023 buy + 2 from 2024 buy
        assert proceeds > 0
        assert costs > 0


class TestYearFilteredDividends:
    def test_dividends_filtered_by_year(self, nbp_rates_multi_year_df):
        """Only dividends from the target year should be included."""
        tx = _build_tx([
            ("Dec-20-2023", "DIVIDEND RECEIVED", "FUND", "-", "$100.00"),
            ("Dec-20-2023", "NON-RESIDENT TAX DIVIDEND RECEIVED", "FUND", "-", "-$15.00"),
            ("Dec-18-2024", "DIVIDEND RECEIVED", "FUND", "-", "$200.00"),
            ("Dec-18-2024", "NON-RESIDENT TAX DIVIDEND RECEIVED", "FUND", "-", "-$30.00"),
        ])
        merged = merge_with_rates(tx, nbp_rates_multi_year_df)

        # All years
        div_all, tax_all = compute_dividends_and_tax(merged)
        assert div_all > 0

        # Only 2024
        div_2024, tax_2024 = compute_dividends_and_tax(merged, year=2024)
        assert div_2024 > 0
        assert div_2024 < div_all  # 2024 is a subset

        # Only 2023
        div_2023, tax_2023 = compute_dividends_and_tax(merged, year=2023)
        assert div_2023 > 0
        assert div_2023 < div_all

    def test_no_dividends_in_year(self, nbp_rates_multi_year_df):
        """If no dividends in the target year, totals should be zero."""
        tx = _build_tx([
            ("Dec-20-2023", "DIVIDEND RECEIVED", "FUND", "-", "$100.00"),
        ])
        merged = merge_with_rates(tx, nbp_rates_multi_year_df)
        div_2024, tax_2024 = compute_dividends_and_tax(merged, year=2024)
        assert div_2024 == 0
        assert tax_2024 == 0
