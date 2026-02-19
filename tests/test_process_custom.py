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


def test_reported_cost_basis_used_for_rs_lot(tmp_path):
    merged = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["Dec-13-2024", "Dec-16-2024"]),
            "settlement_date": pd.to_datetime(["Dec-13-2024", "Dec-16-2024"]),
            "Transaction type": ["YOU BOUGHT RSU####", "YOU SOLD"],
            "Investment name": ["ACME", "ACME"],
            "shares": [5.0, -5.0],
            "amount_pln": [0.0, 500.0],
            "rate": [4.0, 4.05],
        }
    )
    custom_file = tmp_path / "custom_rs_cost_basis.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-16-2024\tDec-13-2024\t5.0000\t$250.00\twhatever\twhatever\tRS\n"
    )

    proceeds, costs, gain = process_custom(merged, str(custom_file), year=2024)
    assert proceeds == pytest.approx(500.0, abs=0.01)
    assert costs == pytest.approx(1000.0, abs=0.01)
    assert gain == pytest.approx(-500.0, abs=0.01)


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
