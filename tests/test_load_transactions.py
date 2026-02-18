import pandas as pd
import pytest

from fidelity2pit38 import load_transactions


def test_parses_columns(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert "trade_date" in tx.columns
    assert "shares" in tx.columns
    assert "amount_usd" in tx.columns


def test_trade_date_is_datetime(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert pd.api.types.is_datetime64_any_dtype(tx["trade_date"])


def test_shares_is_numeric(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert pd.api.types.is_numeric_dtype(tx["shares"])


def test_amount_usd_is_numeric(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert pd.api.types.is_numeric_dtype(tx["amount_usd"])


def test_strips_semicolons_from_transaction_type(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert not tx["Transaction type"].str.contains(";", na=False).any()


def test_preserves_original_columns(example_tx_csv_path):
    tx = load_transactions(example_tx_csv_path)
    assert "Transaction date" in tx.columns
    assert "Transaction type" in tx.columns
    assert "Amount" in tx.columns


def test_dollar_signs_stripped(example_tx_csv_path):
    """amount_usd should not contain dollar sign artifacts."""
    tx = load_transactions(example_tx_csv_path)
    assert tx["amount_usd"].notna().any()
    # If there were dollar signs left, conversion to numeric would have produced NaN
    assert tx["amount_usd"].dropna().apply(lambda x: isinstance(x, float)).all()


def test_multi_csv_loading(example_tx_csv_path):
    """Loading the same CSV twice should deduplicate."""
    single = load_transactions(example_tx_csv_path)
    double = load_transactions([example_tx_csv_path, example_tx_csv_path])
    assert len(double) == len(single)


def test_multi_csv_list_single(example_tx_csv_path):
    """A list with one path should work identically to a string."""
    from_str = load_transactions(example_tx_csv_path)
    from_list = load_transactions([example_tx_csv_path])
    assert len(from_str) == len(from_list)
