import pandas as pd
import pytest

from fidelity2pit38 import merge_with_rates


def test_exact_date_match(nbp_rates_df):
    tx = pd.DataFrame(
        {
            "rate_date": [pd.Timestamp("2024-09-11")],
            "amount_usd": [100.0],
        }
    )
    merged = merge_with_rates(tx, nbp_rates_df)
    assert merged["rate"].iloc[0] == 3.8816
    assert merged["amount_pln"].iloc[0] == pytest.approx(388.16)


def test_backward_fill(nbp_rates_df):
    """rate_date on a weekend uses most recent prior rate."""
    tx = pd.DataFrame(
        {
            "rate_date": [pd.Timestamp("2024-09-14")],  # Saturday
            "amount_usd": [100.0],
        }
    )
    merged = merge_with_rates(tx, nbp_rates_df)
    # Should use 2024-09-13 rate (3.8950)
    assert merged["rate"].iloc[0] == 3.8950
    assert merged["amount_pln"].iloc[0] == pytest.approx(389.50)


def test_amount_pln_calculation(nbp_rates_df):
    tx = pd.DataFrame(
        {
            "rate_date": [pd.Timestamp("2024-12-18")],
            "amount_usd": [2919.78],
        }
    )
    merged = merge_with_rates(tx, nbp_rates_df)
    expected = 2919.78 * 4.0621
    assert merged["amount_pln"].iloc[0] == pytest.approx(expected)


def test_preserves_original_columns(nbp_rates_df):
    tx = pd.DataFrame(
        {
            "rate_date": [pd.Timestamp("2024-09-11")],
            "amount_usd": [100.0],
            "Transaction type": ["YOU SOLD"],
            "shares": [-10.0],
        }
    )
    merged = merge_with_rates(tx, nbp_rates_df)
    assert "Transaction type" in merged.columns
    assert "shares" in merged.columns
    assert "rate" in merged.columns
    assert "amount_pln" in merged.columns


def test_multiple_transactions_sorted(nbp_rates_df):
    tx = pd.DataFrame(
        {
            "rate_date": [
                pd.Timestamp("2024-12-30"),
                pd.Timestamp("2024-09-11"),
                pd.Timestamp("2024-12-18"),
            ],
            "amount_usd": [5.29, -1531.58, 2919.78],
        }
    )
    merged = merge_with_rates(tx, nbp_rates_df)
    assert len(merged) == 3
    # Should be sorted by rate_date
    assert merged["rate_date"].is_monotonic_increasing
