from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import fidelity2pit38

PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "data-sample"
FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def nbp_fixture_csv_path():
    return str(FIXTURE_DIR / "nbp_rates_2024.csv")


@pytest.fixture
def nbp_fixture_csv_path_2023():
    return str(FIXTURE_DIR / "nbp_rates_2023.csv")


@pytest.fixture
def example_data_dir():
    return str(DATA_DIR)


@pytest.fixture
def example_tx_csv_path():
    return str(DATA_DIR / "Transaction history 2024.csv")


@pytest.fixture
def example_custom_summary_path():
    return str(DATA_DIR / "stock-sales-2024.txt")


@pytest.fixture
def nbp_rates_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-03-18",
                    "2024-03-28",
                    "2024-06-13",
                    "2024-06-14",
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
                3.9400,
                3.9600,
                3.9800,
                3.9900,
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
def nbp_rates_multi_year_df():
    """NBP rates spanning 2023-2024 for multi-year tests."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2023-12-18",
                    "2023-12-19",
                    "2023-12-20",
                    "2023-12-27",
                    "2023-12-28",
                    "2023-12-29",
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
                3.9500,
                3.9550,
                3.9600,
                3.9700,
                3.9750,
                3.9800,
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
def mock_nbp_read_csv(nbp_rates_df, nbp_rates_multi_year_df):
    """Patch urllib.request.urlopen with deterministic synthetic NBP archive CSVs."""
    from unittest.mock import MagicMock

    def _to_archive_csv(df):
        lines = ["data;1USD"]
        for row in df.sort_values("date").itertuples(index=False):
            lines.append(f"{row.date.strftime('%Y%m%d')};{str(f'{row.rate:.4f}').replace('.', ',')}")
        return ("\n".join(lines) + "\n").encode("cp1250")

    def _mock_urlopen(url, **kwargs):
        if "2023" in url:
            data = _to_archive_csv(nbp_rates_multi_year_df[nbp_rates_multi_year_df["date"].dt.year == 2023])
        else:
            data = _to_archive_csv(nbp_rates_df)
        resp = MagicMock()
        resp.read.return_value = data
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    return patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen)


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
