from unittest.mock import patch

import pandas as pd

from fidelity2pit38 import load_nbp_rates


def test_loads_from_fixture(nbp_fixture_csv_path):
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert len(rates) == 13
    assert list(rates.columns) == ["date", "rate"]


def test_parses_comma_decimal(nbp_fixture_csv_path):
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    row = rates[rates["date"] == pd.Timestamp("2024-09-11")]
    assert row["rate"].iloc[0] == 3.8816


def test_filters_non_date_rows(nbp_fixture_csv_path):
    """The header description row (no YYYYMMDD in data column) is excluded."""
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    # Fixture has 13 data rows + 1 description row; only 13 should remain
    assert len(rates) == 13


def test_date_format(nbp_fixture_csv_path):
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert rates.iloc[0]["date"] == pd.Timestamp("2024-09-10")
    assert rates.iloc[-1]["date"] == pd.Timestamp("2024-12-31")


def test_sorted_by_date(nbp_fixture_csv_path):
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert rates["date"].is_monotonic_increasing


def test_deduplicates_dates(nbp_fixture_csv_path):
    """Loading same fixture URL twice should deduplicate."""
    original = pd.read_csv

    def mock(url, **kw):
        return original(nbp_fixture_csv_path, **kw)

    with patch("fidelity2pit38.core.pd.read_csv", side_effect=mock):
        rates = load_nbp_rates(
            ["https://fake.url/rates1.csv", "https://fake.url/rates2.csv"]
        )

    assert len(rates) == 13  # not 26
    assert rates["date"].is_monotonic_increasing
