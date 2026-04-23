import pytest
import pandas as pd

from fidelity2pit38 import process_custom


def test_rsu_source_zero_cost(merged_example, tmp_path):
    """Rows with source='RS' should have zero cost."""
    custom_file = tmp_path / "custom_rs.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t20.0000\twhatever\twhatever\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged_example, str(custom_file))
    assert costs == pytest.approx(0.0)
    assert gain == proceeds


def test_example_data_custom(merged_example, example_custom_summary_path):
    proceeds, costs, gain = process_custom(merged_example, example_custom_summary_path)
    assert proceeds == pytest.approx(34033.92, abs=0.01)
    assert costs == pytest.approx(5571.40, abs=0.01)
    assert gain == pytest.approx(28462.52, abs=0.01)


def test_espp_source_cost_from_buy(merged_example, tmp_path):
    """ESPP source ('SP') should derive cost from the ESPP buy transaction."""
    custom_file = tmp_path / "custom_sp.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tSep-13-2024\t10.0000\twhatever\twhatever\twhatever\tSP\n"
    )
    proceeds, costs, gain = process_custom(merged_example, str(custom_file))
    # 10 shares sold at sale price, cost from matching ESPP buy
    assert proceeds > 0
    assert costs > 0
    assert costs == pytest.approx(2025.40, abs=0.01)


def test_no_matching_sale_logs_error(merged_example, tmp_path, caplog):
    """If custom summary references a date with no sale, it logs an error."""
    custom_file = tmp_path / "custom_bad.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Jan-01-2024\tDec-16-2024\t10.0000\twhatever\twhatever\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged_example, str(custom_file))
    assert proceeds == 0.0
    assert costs == 0.0


def test_year_filter_uses_sale_settlement_year(tmp_path):
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-30-2024", "Dec-31-2024"]),
            "settlement_date": pd.to_datetime(["Dec-31-2024", "Jan-02-2025"]),
            "Transaction type": ["YOU BOUGHT ESPP###", "YOU SOLD"],
            "Investment name": ["ACME", "ACME"],
            "shares": [10.0, -10.0],
            "amount_pln": [-400.0, 1000.0],
            "rate": [4.0, 4.1],
        }
    )
    custom_file = tmp_path / "custom_cross_year.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-31-2024\tDec-30-2024\t10.0000\t$100.00\twhatever\twhatever\tSP\n"
    )

    proceeds_2024, costs_2024, gain_2024 = process_custom(merged.copy(), str(custom_file), year=2024)
    assert proceeds_2024 == 0.0
    assert costs_2024 == 0.0
    assert gain_2024 == 0.0

    proceeds_2025, costs_2025, gain_2025 = process_custom(merged.copy(), str(custom_file), year=2025)
    assert proceeds_2025 == pytest.approx(1000.0, abs=0.01)
    assert costs_2025 == pytest.approx(400.0, abs=0.01)
    assert gain_2025 == pytest.approx(600.0, abs=0.01)


def test_rs_lot_cost_is_always_zero(tmp_path):
    """RS (RSU) lots must always have cost=0 under Polish art. 30b.

    The 'Cost basis' column in Fidelity exports is the US FMV-at-vest amount
    (ordinary income recognised in the US) and is NOT a deductible cost for
    Polish capital-gains tax — it is ignored regardless of its value.
    """
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-16-2024"]),
            "Transaction type": ["YOU SOLD"],
            "Investment name": ["ACME"],
            "shares": [-5.0],
            "amount_pln": [500.0],
            "rate": [4.05],
        }
    )
    custom_file = tmp_path / "custom_rs_cost_basis.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t5.0000\t$250.00\twhatever\twhatever\tRS\n"
    )

    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    assert proceeds == pytest.approx(500.0, abs=0.01)
    assert costs == pytest.approx(0.0, abs=0.01)
    assert gain == pytest.approx(500.0, abs=0.01)


def test_reported_cost_basis_overrides_sp_buy_amount(tmp_path):
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Sep-13-2024", "Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Sep-16-2024", "Dec-16-2024"]),
            "Transaction type": ["YOU BOUGHT ESPP###", "YOU SOLD"],
            "Investment name": ["ACME", "ACME"],
            "shares": [10.0, -10.0],
            "amount_pln": [-200.0, 400.0],
            "rate": [4.0, 4.05],
        }
    )
    custom_file = tmp_path / "custom_sp_cost_basis.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tSep-13-2024\t10.0000\t$80.00\twhatever\twhatever\tSP\n"
    )

    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    assert proceeds == pytest.approx(400.0, abs=0.01)
    assert costs == pytest.approx(320.0, abs=0.01)
    assert gain == pytest.approx(80.0, abs=0.01)


def test_custom_symbol_column_disambiguates_same_day_sales(tmp_path):
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Sep-13-2024", "Sep-13-2024", "Dec-16-2024", "Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Sep-16-2024", "Sep-16-2024", "Dec-16-2024", "Dec-16-2024"]),
            "Transaction type": ["YOU BOUGHT ESPP###", "YOU BOUGHT ESPP###", "YOU SOLD", "YOU SOLD"],
            "Investment name": ["AAA INC COMMON STOCK", "BBB INC COMMON STOCK", "AAA INC COMMON STOCK", "BBB INC COMMON STOCK"],
            "shares": [10.0, 10.0, -10.0, -10.0],
            "amount_pln": [-400.0, -800.0, 1000.0, 2000.0],
            "rate": [4.0, 4.0, 4.1, 4.1],
        }
    )
    custom_file = tmp_path / "custom_with_symbol.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\tSymbol\n"
        "Dec-16-2024\tSep-13-2024\t10.0000\t$100.00\twhatever\twhatever\tSP\tAAA\n"
    )

    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    assert proceeds == pytest.approx(1000.0, abs=0.01)
    assert costs == pytest.approx(400.0, abs=0.01)
    assert gain == pytest.approx(600.0, abs=0.01)


def test_proceeds_column_used_over_sell_transaction(tmp_path):
    """When stock-sales has a valid Proceeds value, use it (× sell rate)
    instead of deriving from the sell transaction's amount_pln/shares."""
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-17-2024"]),
            "Transaction type": ["YOU SOLD"],
            "Investment name": ["ACME"],
            "shares": [-50.0],
            "amount_pln": [5000.0],
            "rate": [4.0],
        }
    )
    custom_file = tmp_path / "custom.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t30.0000\twhatever\t$400.00\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    # Proceeds = $400 × rate 4.0 = 1600 PLN
    # Old derivation would give 30 × (5000/50) = 3000 PLN
    assert proceeds == pytest.approx(1600.0, abs=0.01)
    assert costs == pytest.approx(0.0)
    assert gain == pytest.approx(1600.0, abs=0.01)


def test_multiple_sells_same_date_with_proceeds_column(tmp_path):
    """With Proceeds column, each lot gets its exact proceeds regardless of
    how many sell rows exist on the same date."""
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024", "Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-17-2024", "Dec-17-2024"]),
            "Transaction type": ["YOU SOLD", "YOU SOLD"],
            "Investment name": ["ACME", "ACME"],
            "shares": [-89.0, -100.0],
            "amount_pln": [71200.0, 80800.0],
            "rate": [4.0, 4.0],
        }
    )
    custom_file = tmp_path / "custom.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t100.0000\twhatever\t$20200.00\twhatever\tRS\n"
        "Dec-16-2024\tDec-01-2024\t89.0000\twhatever\t$17800.00\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    # Lot 1: $20,200 × 4.0 = 80,800
    # Lot 2: $17,800 × 4.0 = 71,200
    # Total: 152,000
    assert proceeds == pytest.approx(152000.0, abs=0.01)


def test_multiple_sells_same_date_no_proceeds_uses_weighted_average(tmp_path):
    """Without Proceeds column, multiple same-date sells use weighted-average
    price per share rather than the first sell's price."""
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024", "Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-17-2024", "Dec-17-2024"]),
            "Transaction type": ["YOU SOLD", "YOU SOLD"],
            "Investment name": ["ACME", "ACME"],
            "shares": [-60.0, -40.0],
            "amount_pln": [6000.0, 6000.0],
            "rate": [4.0, 4.0],
        }
    )
    custom_file = tmp_path / "custom.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t50.0000\twhatever\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    # Weighted avg: (6000 + 6000) / (60 + 40) = 120 PLN/share
    # Proceeds: 50 × 120 = 6000
    # First-match bug would give: 50 × (6000/60) = 50 × 100 = 5000
    assert proceeds == pytest.approx(6000.0, abs=0.01)


def test_identical_custom_rows_must_not_be_deduplicated(tmp_path):
    """Two legitimately-identical lot rows in the custom summary must be
    preserved, not collapsed by drop_duplicates().

    Scenario: two RSU grants of the same size vested on the same date and
    were sold on the same date. The custom stock-sales summary then contains
    two rows with identical Date sold / Date acquired / Quantity / Proceeds.
    These are real distinct lots — deduplicating them under-reports the sale.
    """
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-17-2024"]),
            "Transaction type": ["YOU SOLD"],
            "Investment name": ["ACME"],
            "shares": [-20.0],
            "amount_pln": [4000.0],
            "rate": [4.0],
        }
    )
    custom_file = tmp_path / "custom_duplicates.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t10.0000\twhatever\t$500.00\twhatever\tRS\n"
        "Dec-16-2024\tDec-13-2024\t10.0000\twhatever\t$500.00\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    # Two lots of 10 shares each @ $500 proceeds × rate 4.0 = 2000 PLN per lot
    # Correct total: 4000 PLN. Buggy (dedup) total: 2000 PLN.
    assert proceeds == pytest.approx(4000.0, abs=0.01)


def test_invalid_proceeds_falls_back_to_sell_transaction(tmp_path):
    """Non-numeric Proceeds value falls back to deriving from the sell transaction."""
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-17-2024"]),
            "Transaction type": ["YOU SOLD"],
            "Investment name": ["ACME"],
            "shares": [-50.0],
            "amount_pln": [5000.0],
            "rate": [4.0],
        }
    )
    custom_file = tmp_path / "custom.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t30.0000\twhatever\tN/A\twhatever\tRS\n"
    )
    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    # "N/A" → NaN, falls back: 30 × (5000/50) = 3000
    assert proceeds == pytest.approx(3000.0, abs=0.01)
