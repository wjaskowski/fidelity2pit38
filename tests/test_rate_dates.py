import pandas as pd

from fidelity2pit38 import calculate_rate_dates


def _calc(date_str):
    dates = pd.Series([pd.Timestamp(date_str)])
    return calculate_rate_dates(dates).iloc[0]


def test_weekday_thursday():
    """2024-12-19 (Thu) - 1 PL BD = 2024-12-18 (Wed)."""
    assert _calc("2024-12-19") == pd.Timestamp("2024-12-18")


def test_weekday_wednesday():
    """2024-12-18 (Wed) - 1 PL BD = 2024-12-17 (Tue)."""
    assert _calc("2024-12-18") == pd.Timestamp("2024-12-17")


def test_monday_goes_to_friday():
    """2024-12-16 (Mon) - 1 PL BD = 2024-12-13 (Fri)."""
    assert _calc("2024-12-16") == pd.Timestamp("2024-12-13")


def test_tuesday():
    """2024-12-17 (Tue) - 1 PL BD = 2024-12-16 (Mon)."""
    assert _calc("2024-12-17") == pd.Timestamp("2024-12-16")


def test_settlement_dec_31():
    """2024-12-31 (Tue) - 1 PL BD = 2024-12-30 (Mon)."""
    assert _calc("2024-12-31") == pd.Timestamp("2024-12-30")


def test_settlement_sep_12():
    """2024-09-12 (Thu) - 1 PL BD = 2024-09-11 (Wed)."""
    assert _calc("2024-09-12") == pd.Timestamp("2024-09-11")


def test_vectorized():
    dates = pd.Series(
        [pd.Timestamp("2024-12-19"), pd.Timestamp("2024-12-31")]
    )
    results = calculate_rate_dates(dates)
    assert results.iloc[0] == pd.Timestamp("2024-12-18")
    assert results.iloc[1] == pd.Timestamp("2024-12-30")
