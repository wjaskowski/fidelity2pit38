import sys
from unittest.mock import patch

import pandas as pd
import pytest

import fidelity2pit38
from fidelity2pit38 import (
    calculate_pit38_fields,
    calculate_rate_dates,
    calculate_settlement_dates,
    compute_dividends_and_tax,
    main,
    merge_with_rates,
    process_custom,
    process_fifo,
)


def _run_pipeline_fifo(tx_csv_path, nbp_rates_df, year=2024):
    """Run the full pipeline via function composition (no argparse)."""
    tx_raw = pd.read_csv(tx_csv_path)
    tx = tx_raw.copy()
    tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
    tx["trade_date"] = pd.to_datetime(
        tx["Transaction date"], format="%b-%d-%Y", errors="coerce"
    )
    tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
    tx["amount_usd"] = pd.to_numeric(
        tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
    )
    tx["settlement_date"] = calculate_settlement_dates(
        tx["trade_date"], tx["Transaction type"]
    )
    tx = tx[tx["settlement_date"].dt.year == year]
    tx["rate_date"] = calculate_rate_dates(tx["settlement_date"])
    merged = merge_with_rates(tx, nbp_rates_df)

    total_proceeds, total_costs, total_gain = process_fifo(merged)
    total_dividends, foreign_tax = compute_dividends_and_tax(merged)

    return calculate_pit38_fields(
        total_proceeds, total_costs, total_gain, total_dividends, foreign_tax,
    )


# --- E2E via function composition ---


class TestE2EFifo:
    def test_full_pipeline(self, example_tx_csv_path, nbp_rates_df):
        result = _run_pipeline_fifo(example_tx_csv_path, nbp_rates_df)
        # Section C/D: capital gains only (no dividends in poz22)
        assert result["poz22"] == pytest.approx(11860.43, abs=0.01)  # proceeds only
        assert result["poz23"] == pytest.approx(5944.98, abs=0.01)
        assert result["poz26"] == pytest.approx(5915.45, abs=0.01)
        assert result["poz29"] == 5915  # _round_tax(5915.45)
        assert result["poz31"] == pytest.approx(1123.85, abs=0.01)  # 5915 * 0.19
        assert result["poz32"] == pytest.approx(0.0)  # US doesn't withhold on stock sales
        assert result["tax_final"] == 1124  # _round_tax(1123.85)
        # PIT-ZG
        assert result["pitzg_poz29"] == pytest.approx(5915.45, abs=0.01)
        assert result["pitzg_poz30"] == pytest.approx(0.0)  # no foreign tax on capital gains


class TestE2ECustom:
    def test_full_pipeline(
        self, example_tx_csv_path, example_custom_summary_path, nbp_rates_df
    ):
        tx_raw = pd.read_csv(example_tx_csv_path)
        tx = tx_raw.copy()
        tx["Transaction type"] = (
            tx["Transaction type"].astype(str).str.split(";").str[0]
        )
        tx["trade_date"] = pd.to_datetime(
            tx["Transaction date"], format="%b-%d-%Y", errors="coerce"
        )
        tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
        tx["amount_usd"] = pd.to_numeric(
            tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
        )
        tx["settlement_date"] = calculate_settlement_dates(
            tx["trade_date"], tx["Transaction type"]
        )
        tx = tx[tx["settlement_date"].dt.year == 2024]
        tx["rate_date"] = calculate_rate_dates(tx["settlement_date"])
        merged = merge_with_rates(tx, nbp_rates_df)

        total_proceeds, total_costs, total_gain = process_custom(
            merged, example_custom_summary_path
        )
        total_dividends, foreign_tax = compute_dividends_and_tax(merged)

        result = calculate_pit38_fields(
            total_proceeds, total_costs, total_gain, total_dividends, foreign_tax,
        )

        # Section C/D: capital gains (custom method — all RSU, so costs=0)
        assert result["poz22"] == pytest.approx(11860.44, abs=0.01)  # proceeds only
        assert result["poz23"] == pytest.approx(0.0)
        assert result["poz26"] == pytest.approx(11860.44, abs=0.01)
        assert result["poz29"] == 11860  # _round_tax(11860.44)
        assert result["poz31"] == pytest.approx(2253.40, abs=0.01)  # 11860 * 0.19
        assert result["poz32"] == pytest.approx(0.0)  # US doesn't withhold on stock sales
        assert result["tax_final"] == 2253  # _round_tax(2253.40)
        assert total_gain == pytest.approx(11860.44, abs=0.01)


# --- E2E via main() with stdout capture ---


class TestMainCLI:
    def test_main_fifo(
        self, capsys, monkeypatch, example_data_dir, mock_nbp_read_csv
    ):
        monkeypatch.setattr(
            sys, "argv", ["fidelity2pit38", "--data-dir", example_data_dir, "--method", "fifo", "--year", "2024"]
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out
        assert "Czesc C/D" in out
        assert "Poz. 22" in out
        assert "Poz. 33" in out
        assert "Czesc G" in out
        assert "Poz. 45" in out
        assert "Poz. 47" in out
        assert "PIT-ZG" in out

    def test_main_custom(
        self,
        capsys,
        monkeypatch,
        example_data_dir,
        example_custom_summary_path,
        mock_nbp_read_csv,
    ):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "fidelity2pit38",
                "--data-dir",
                example_data_dir,
                "--method",
                "custom",
                "--custom-summary",
                example_custom_summary_path,
                "--year",
                "2024",
            ],
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out
        assert "PIT-ZG" in out

    def test_main_year_flag(
        self, capsys, monkeypatch, example_data_dir, mock_nbp_read_csv
    ):
        monkeypatch.setattr(
            sys,
            "argv",
            ["fidelity2pit38", "--data-dir", example_data_dir, "--year", "2024"],
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out

    def test_main_custom_without_summary_errors(
        self, tmp_path, monkeypatch, mock_nbp_read_csv
    ):
        """custom method without --custom-summary and no discoverable TXTs should error."""
        # tmp_path has no stock-sales*.txt files
        (tmp_path / "Transaction history.csv").write_text("Transaction date,Transaction type,Investment name,Shares,Amount\n")
        monkeypatch.setattr(
            sys,
            "argv",
            ["fidelity2pit38", "--data-dir", str(tmp_path), "--method", "custom", "--year", "2024"],
        )
        with mock_nbp_read_csv, pytest.raises(SystemExit):
            main()


class TestE2EYearFiltering:
    def test_cross_year_settlement_excluded(self, nbp_rates_df):
        """A sell on Dec 31 settles in Jan 2025 -> excluded from 2024."""
        tx = pd.DataFrame(
            {
                "Transaction date": ["Dec-31-2024"],
                "Transaction type": ["YOU SOLD"],
                "Investment name": ["XXX"],
                "Shares": ["-10.00"],
                "Amount": ["$1000.00"],
            }
        )
        tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
        tx["trade_date"] = pd.to_datetime(
            tx["Transaction date"], format="%b-%d-%Y", errors="coerce"
        )
        tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
        tx["amount_usd"] = pd.to_numeric(
            tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
        )
        tx["settlement_date"] = calculate_settlement_dates(
            tx["trade_date"], tx["Transaction type"]
        )
        # Settlement should be 2025-01-02 (Jan 1 holiday)
        assert tx["settlement_date"].iloc[0] == pd.Timestamp("2025-01-02")
        # Filtering for 2024 excludes it
        tx_2024 = tx[tx["settlement_date"].dt.year == 2024]
        assert len(tx_2024) == 0

    def test_dividends_only_no_sells(self, nbp_rates_df):
        """Only dividends, no stock trades -> proceeds/costs/gain all zero."""
        tx = pd.DataFrame(
            {
                "Transaction date": ["Dec-31-2024", "Dec-31-2024"],
                "Transaction type": [
                    "DIVIDEND RECEIVED",
                    "NON-RESIDENT TAX DIVIDEND RECEIVED",
                ],
                "Investment name": ["FUND", "FUND"],
                "Shares": ["-", "-"],
                "Amount": ["$50.00", "-$10.00"],
            }
        )
        tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
        tx["trade_date"] = pd.to_datetime(
            tx["Transaction date"], format="%b-%d-%Y", errors="coerce"
        )
        tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
        tx["amount_usd"] = pd.to_numeric(
            tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
        )
        tx["settlement_date"] = calculate_settlement_dates(
            tx["trade_date"], tx["Transaction type"]
        )
        tx = tx[tx["settlement_date"].dt.year == 2024]
        tx["rate_date"] = calculate_rate_dates(tx["settlement_date"])
        merged = merge_with_rates(tx, nbp_rates_df)

        proceeds, costs, gain = process_fifo(merged)
        assert proceeds == 0
        assert costs == 0
        assert gain == 0

        dividends, foreign_tax = compute_dividends_and_tax(merged)
        assert dividends > 0
        assert foreign_tax > 0
