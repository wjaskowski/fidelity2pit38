import pytest

from fidelity2pit38 import process_custom


def test_rsu_source_zero_cost(merged_example, example_custom_summary_path):
    """stock-sales.txt has source='RS', so cost should be 0."""
    proceeds, costs, gain = process_custom(merged_example, example_custom_summary_path)
    assert costs == pytest.approx(0.0)
    assert gain == proceeds


def test_example_data_custom(merged_example, example_custom_summary_path):
    proceeds, costs, gain = process_custom(merged_example, example_custom_summary_path)
    assert proceeds == pytest.approx(11860.44, abs=0.01)
    assert costs == pytest.approx(0.0)
    assert gain == pytest.approx(11860.44, abs=0.01)


def test_espp_source_cost_from_buy(merged_example, tmp_path):
    """ESPP source ('SP') should derive cost from the ESPP buy transaction."""
    custom_file = tmp_path / "custom_sp.txt"
    custom_file.write_text(
        "Date sold or transferred\tDate acquired\tQuantity\tCost basis\tProceeds\tGain/loss\tStock source\n"
        "Dec-18-2024\tSep-11-2024\t19.0000\twhatever\twhatever\twhatever\tSP\n"
    )
    proceeds, costs, gain = process_custom(merged_example, str(custom_file))
    # 19 shares sold at sale price, cost from ESPP buy
    assert proceeds > 0
    assert costs > 0
    assert costs == pytest.approx(5944.98, abs=0.01)


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
