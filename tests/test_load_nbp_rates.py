from unittest.mock import MagicMock, patch

import pandas as pd

from fidelity2pit38 import load_nbp_rates


def _mock_urlopen_factory(fixture_path):
    """Create a urlopen mock that returns fixture file contents."""
    def _mock(url, **kwargs):
        with open(fixture_path, "rb") as f:
            data = f.read()
        resp = MagicMock()
        resp.read.return_value = data
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp
    return _mock


def test_loads_from_fixture(nbp_fixture_csv_path):
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert len(rates) == 13
    assert list(rates.columns) == ["date", "rate"]


def test_parses_comma_decimal(nbp_fixture_csv_path):
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    row = rates[rates["date"] == pd.Timestamp("2024-09-11")]
    assert row["rate"].iloc[0] == 3.8816


def test_filters_non_date_rows(nbp_fixture_csv_path):
    """The header description row (no YYYYMMDD in data column) is excluded."""
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    # Fixture has 13 data rows + 1 description row; only 13 should remain
    assert len(rates) == 13


def test_date_format(nbp_fixture_csv_path):
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert rates.iloc[0]["date"] == pd.Timestamp("2024-09-10")
    assert rates.iloc[-1]["date"] == pd.Timestamp("2024-12-31")


def test_sorted_by_date(nbp_fixture_csv_path):
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(["https://fake.url/rates.csv"])

    assert rates["date"].is_monotonic_increasing


def test_deduplicates_dates(nbp_fixture_csv_path):
    """Loading same fixture URL twice should deduplicate."""
    with patch("fidelity2pit38.core.urllib.request.urlopen", side_effect=_mock_urlopen_factory(nbp_fixture_csv_path)):
        rates = load_nbp_rates(
            ["https://fake.url/rates1.csv", "https://fake.url/rates2.csv"]
        )

    assert len(rates) == 13  # not 26
    assert rates["date"].is_monotonic_increasing
