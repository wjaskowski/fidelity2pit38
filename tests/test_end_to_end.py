import sys
from unittest.mock import patch

import pandas as pd
import pytest

import fidelity2pit38
from fidelity2pit38 import (
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

    poz22 = round(total_proceeds + total_dividends, 2)
    poz23 = round(total_costs, 2)
    poz26 = round(poz22 - poz23, 2)
    poz29 = int(round(poz26))
    poz31 = round(poz29 * 0.19, 2)
    poz32 = foreign_tax
    raw_tax_due = poz31 - poz32
    tax_final = int(max(raw_tax_due, 0) + 0.5)
    pitzg_poz29 = total_gain
    pitzg_poz30 = foreign_tax

    return {
        "poz22": poz22,
        "poz23": poz23,
        "poz26": poz26,
        "poz29": poz29,
        "poz31": poz31,
        "poz32": poz32,
        "tax_final": tax_final,
        "pitzg_poz29": pitzg_poz29,
        "pitzg_poz30": pitzg_poz30,
    }


# --- E2E via function composition ---


class TestE2EFifo:
    def test_full_pipeline(self, example_tx_csv_path, nbp_rates_df):
        result = _run_pipeline_fifo(example_tx_csv_path, nbp_rates_df)
        assert result["poz22"] == pytest.approx(11834.99, abs=0.01)
        assert result["poz23"] == pytest.approx(5944.98, abs=0.01)
        assert result["poz26"] == pytest.approx(5890.01, abs=0.01)
        assert result["poz29"] == 5890
        assert result["poz31"] == pytest.approx(1119.10, abs=0.01)
        assert result["poz32"] == pytest.approx(14.66, abs=0.01)
        assert result["tax_final"] == 1104
        assert result["pitzg_poz29"] == pytest.approx(5915.45, abs=0.01)
        assert result["pitzg_poz30"] == pytest.approx(14.66, abs=0.01)


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

        poz22 = round(total_proceeds + total_dividends, 2)
        poz23 = round(total_costs, 2)
        poz26 = round(poz22 - poz23, 2)
        poz29 = int(round(poz26))
        poz31 = round(poz29 * 0.19, 2)
        poz32 = foreign_tax
        raw_tax_due = poz31 - poz32
        tax_final = int(max(raw_tax_due, 0) + 0.5)

        assert poz22 == pytest.approx(11835.00, abs=0.01)
        assert poz23 == pytest.approx(0.0)
        assert poz26 == pytest.approx(11835.00, abs=0.01)
        assert poz29 == 11835
        assert poz31 == pytest.approx(2248.65, abs=0.01)
        assert poz32 == pytest.approx(14.66, abs=0.01)
        assert tax_final == 2234
        assert total_gain == pytest.approx(11860.44, abs=0.01)


# --- E2E via main() with stdout capture ---


class TestMainCLI:
    def test_main_fifo(
        self, capsys, monkeypatch, example_tx_csv_path, mock_nbp_read_csv
    ):
        monkeypatch.setattr(
            sys, "argv", ["fidelity2pit38.py", example_tx_csv_path, "--method", "fifo"]
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out
        assert "Poz. 22" in out
        assert "Poz. 33" in out
        assert "PIT-ZG:" in out

    def test_main_custom(
        self,
        capsys,
        monkeypatch,
        example_tx_csv_path,
        example_custom_summary_path,
        mock_nbp_read_csv,
    ):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "fidelity2pit38.py",
                example_tx_csv_path,
                "--method",
                "custom",
                "--custom_summary",
                example_custom_summary_path,
            ],
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out
        assert "PIT-ZG:" in out

    def test_main_year_flag(
        self, capsys, monkeypatch, example_tx_csv_path, mock_nbp_read_csv
    ):
        monkeypatch.setattr(
            sys,
            "argv",
            ["fidelity2pit38.py", example_tx_csv_path, "--year", "2024"],
        )
        with mock_nbp_read_csv:
            main()
        out = capsys.readouterr().out
        assert "PIT-38 for year 2024:" in out

    def test_main_custom_without_summary_errors(
        self, monkeypatch, example_tx_csv_path, mock_nbp_read_csv
    ):
        monkeypatch.setattr(
            sys,
            "argv",
            ["fidelity2pit38.py", example_tx_csv_path, "--method", "custom"],
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
