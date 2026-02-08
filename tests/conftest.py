import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

PROJECT_DIR = Path(__file__).parent.parent
FIXTURE_DIR = Path(__file__).parent / "fixtures"

sys.path.insert(0, str(PROJECT_DIR))

import fidelity2pit38  # noqa: E402


@pytest.fixture
def nbp_fixture_csv_path():
    return str(FIXTURE_DIR / "nbp_rates_2024.csv")


@pytest.fixture
def example_tx_csv_path():
    return str(PROJECT_DIR / "Transaction history.csv")


@pytest.fixture
def example_custom_summary_path():
    return str(PROJECT_DIR / "stock-sales.txt")


@pytest.fixture
def nbp_rates_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-09-10",
                    "2024-09-11",
                    "2024-09-12",
                    "2024-09-13",
                    "2024-12-13",
                    "2024-12-16",
                    "2024-12-17",
                    "2024-12-18",
                    "2024-12-19",
                    "2024-12-20",
                    "2024-12-27",
                    "2024-12-30",
                    "2024-12-31",
                ]
            ),
            "rate": [
                3.8700,
                3.8816,
                3.8900,
                3.8950,
                4.0500,
                4.0571,
                4.0600,
                4.0621,
                4.0650,
                4.0700,
                4.0800,
                4.0960,
                4.1000,
            ],
        }
    )


@pytest.fixture
def mock_nbp_read_csv(nbp_fixture_csv_path):
    """Patches pd.read_csv: redirects HTTP URLs to the fixture file, passes through local paths."""
    original_read_csv = pd.read_csv

    def _mock(path_or_url, **kwargs):
        if isinstance(path_or_url, str) and path_or_url.startswith("http"):
            return original_read_csv(nbp_fixture_csv_path, **kwargs)
        return original_read_csv(path_or_url, **kwargs)

    return patch("fidelity2pit38.pd.read_csv", side_effect=_mock)


@pytest.fixture
def load_and_clean_tx(example_tx_csv_path):
    """Load and clean the example transaction CSV, matching main() logic."""
    tx_raw = pd.read_csv(example_tx_csv_path)
    tx = tx_raw.copy()
    tx["Transaction type"] = tx["Transaction type"].astype(str).str.split(";").str[0]
    tx["trade_date"] = pd.to_datetime(
        tx["Transaction date"], format="%b-%d-%Y", errors="coerce"
    )
    tx["shares"] = pd.to_numeric(tx["Shares"], errors="coerce")
    tx["amount_usd"] = pd.to_numeric(
        tx["Amount"].str.replace(r"[$,]", "", regex=True), errors="coerce"
    )
    return tx


@pytest.fixture
def merged_example(load_and_clean_tx, nbp_rates_df):
    """Full merged DataFrame from example CSV + fixture rates, filtered to 2024."""
    tx = load_and_clean_tx
    tx["settlement_date"] = fidelity2pit38.calculate_settlement_dates(
        tx["trade_date"], tx["Transaction type"]
    )
    tx = tx[tx["settlement_date"].dt.year == 2024]
    tx["rate_date"] = fidelity2pit38.calculate_rate_dates(tx["settlement_date"])
    return fidelity2pit38.merge_with_rates(tx, nbp_rates_df)
