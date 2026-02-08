import pandas as pd
import pytest

from fidelity2pit38 import process_fifo


def _make_merged(buys, sells):
    """Helper: create a merged DataFrame with buy and sell transactions."""
    rows = []
    for shares, amount_pln, date in buys:
        rows.append(
            {
                "Transaction type": "YOU BOUGHT",
                "shares": shares,
                "amount_pln": amount_pln,
                "settlement_date": pd.Timestamp(date),
            }
        )
    for shares, amount_pln, date in sells:
        rows.append(
            {
                "Transaction type": "YOU SOLD",
                "shares": shares,
                "amount_pln": amount_pln,
                "settlement_date": pd.Timestamp(date),
            }
        )
    return pd.DataFrame(rows)


def test_single_buy_single_sell():
    merged = _make_merged(
        buys=[(10, -1000.0, "2024-01-05")],
        sells=[(-10, 1500.0, "2024-06-05")],
    )
    proceeds, costs, gain = process_fifo(merged)
    assert proceeds == pytest.approx(1500.0)
    assert costs == pytest.approx(1000.0)
    assert gain == pytest.approx(500.0)


def test_partial_lot_matching():
    """Buy 10 + Buy 20, Sell 15 -> FIFO: 10 from first lot, 5 from second."""
    merged = _make_merged(
        buys=[
            (10, -1000.0, "2024-01-05"),
            (20, -2400.0, "2024-02-05"),
        ],
        sells=[(-15, 1800.0, "2024-06-05")],
    )
    proceeds, costs, gain = process_fifo(merged)
    # 10 shares @ 100 cost + 5 shares @ 120 cost = 1600
    assert proceeds == pytest.approx(1800.0)
    assert costs == pytest.approx(1600.0)
    assert gain == pytest.approx(200.0)


def test_rsu_zero_cost():
    """RSU buys have amount_usd=0, so cost_per=0."""
    merged = _make_merged(
        buys=[(100, 0.0, "2024-01-05")],
        sells=[(-50, 5000.0, "2024-06-05")],
    )
    proceeds, costs, gain = process_fifo(merged)
    assert proceeds == pytest.approx(5000.0)
    assert costs == pytest.approx(0.0)
    assert gain == pytest.approx(5000.0)


def test_fifo_order_by_settlement_date():
    """ESPP bought in Sep consumed before RSU bought in Dec."""
    merged = _make_merged(
        buys=[
            (19, -5944.98, "2024-09-12"),  # ESPP
            (103, 0.0, "2024-12-17"),  # RSU
        ],
        sells=[(-23, 11860.44, "2024-12-19")],
    )
    proceeds, costs, gain = process_fifo(merged)
    # FIFO: 19 from ESPP (cost=5944.98), 4 from RSU (cost=0)
    espp_proceeds = 23 * (11860.44 / 23)  # same price per share
    espp_cost = 19 * (5944.98 / 19)
    rsu_cost = 4 * 0.0
    assert costs == pytest.approx(espp_cost + rsu_cost, rel=1e-2)


def test_multiple_sells():
    merged = _make_merged(
        buys=[
            (10, -1000.0, "2024-01-05"),
            (20, -2400.0, "2024-02-05"),
        ],
        sells=[
            (-10, 1200.0, "2024-06-05"),
            (-10, 1300.0, "2024-07-05"),
        ],
    )
    proceeds, costs, gain = process_fifo(merged)
    # Sell 1: 10 from lot 1 (cost 1000)
    # Sell 2: 10 from lot 2 (cost 1200)
    assert proceeds == pytest.approx(2500.0)
    assert costs == pytest.approx(2200.0)
    assert gain == pytest.approx(300.0)


def test_no_sells_returns_zeros():
    merged = _make_merged(
        buys=[(10, -1000.0, "2024-01-05")],
        sells=[],
    )
    proceeds, costs, gain = process_fifo(merged)
    assert proceeds == 0
    assert costs == 0
    assert gain == 0


def test_example_data_fifo(merged_example):
    proceeds, costs, gain = process_fifo(merged_example)
    assert proceeds == pytest.approx(11860.43, abs=0.01)
    assert costs == pytest.approx(5944.98, abs=0.01)
    assert gain == pytest.approx(5915.45, abs=0.01)
