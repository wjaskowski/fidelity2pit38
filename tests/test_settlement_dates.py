import pandas as pd

from fidelity2pit38 import calculate_settlement_dates


def _calc(date_str, tx_type):
    dates = pd.Series([pd.Timestamp(date_str)])
    types = pd.Series([tx_type])
    return calculate_settlement_dates(dates, types).iloc[0]


# --- T+1 after switch date (2024-05-28) ---

def test_you_sold_after_switch():
    result = _calc("2024-12-18", "YOU SOLD")
    assert result == pd.Timestamp("2024-12-19")


def test_you_bought_rsu_after_switch():
    """RSU with 'YOU BOUGHT' prefix is treated as market trade."""
    result = _calc("2024-12-16", "YOU BOUGHT RSU####")
    assert result == pd.Timestamp("2024-12-17")


def test_espp_after_switch():
    result = _calc("2024-09-11", "YOU BOUGHT ESPP### AS OF 09-11-24")
    assert result == pd.Timestamp("2024-09-12")


def test_sell_on_friday_after_switch():
    """Friday 2024-12-13 + T+1 = Monday 2024-12-16."""
    result = _calc("2024-12-13", "YOU SOLD")
    assert result == pd.Timestamp("2024-12-16")


def test_sell_before_us_holiday_july4():
    """2024-07-03 (Wed) + T+1 = 2024-07-05 (Fri, skipping July 4th)."""
    result = _calc("2024-07-03", "YOU SOLD")
    assert result == pd.Timestamp("2024-07-05")


# --- T+2 before switch date ---

def test_you_sold_before_switch():
    """2024-05-01 (Wed) + T+2 = 2024-05-03 (Fri)."""
    result = _calc("2024-05-01", "YOU SOLD")
    assert result == pd.Timestamp("2024-05-03")


def test_you_bought_before_switch():
    """2024-01-02 (Tue) + T+2 = 2024-01-04 (Thu)."""
    result = _calc("2024-01-02", "YOU BOUGHT")
    assert result == pd.Timestamp("2024-01-04")


def test_exact_switch_date_uses_t_plus_1():
    """2024-05-28 is NOT < SWITCH_DATE, so T+1."""
    result = _calc("2024-05-28", "YOU SOLD")
    assert result == pd.Timestamp("2024-05-29")


def test_day_before_switch_uses_t_plus_2():
    """2024-05-24 (Fri) + T+2 = 2024-05-28 (Tue, Mon is Memorial Day)."""
    result = _calc("2024-05-24", "YOU SOLD")
    assert result == pd.Timestamp("2024-05-29")


# --- Same-day settlement for non-market ---

def test_dividend_same_day():
    result = _calc("2024-12-31", "DIVIDEND RECEIVED")
    assert result == pd.Timestamp("2024-12-31")


def test_reinvestment_same_day():
    result = _calc("2024-12-31", "REINVESTMENT REINVEST @ $1.000")
    assert result == pd.Timestamp("2024-12-31")


def test_non_resident_tax_same_day():
    result = _calc("2024-12-31", "NON-RESIDENT TAX DIVIDEND RECEIVED")
    assert result == pd.Timestamp("2024-12-31")


def test_journaled_same_day():
    result = _calc("2024-12-19", "JOURNALED WIRE/CHECK FEE")
    assert result == pd.Timestamp("2024-12-19")


# --- Edge cases ---

def test_nat_trade_date():
    result = _calc(pd.NaT, "YOU SOLD")
    assert pd.isna(result)


def test_cross_year_boundary():
    """2024-12-31 (Tue) + T+1 = 2025-01-02 (Thu, skipping Jan 1 holiday)."""
    result = _calc("2024-12-31", "YOU SOLD")
    assert result == pd.Timestamp("2025-01-02")


def test_vectorized_multiple_transactions():
    dates = pd.Series(
        [pd.Timestamp("2024-12-18"), pd.Timestamp("2024-12-31")]
    )
    types = pd.Series(["YOU SOLD", "DIVIDEND RECEIVED"])
    results = calculate_settlement_dates(dates, types)
    assert results.iloc[0] == pd.Timestamp("2024-12-19")
    assert results.iloc[1] == pd.Timestamp("2024-12-31")
