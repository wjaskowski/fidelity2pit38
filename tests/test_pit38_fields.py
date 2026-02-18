import pytest

from fidelity2pit38 import calculate_pit38_fields


def test_basic_calculation():
    result = calculate_pit38_fields(
        total_proceeds=10000.0,
        total_costs=4000.0,
        total_gain=6000.0,
        total_dividends=500.0,
        foreign_tax=50.0,
    )
    assert result["poz22"] == 10500.0   # proceeds + dividends
    assert result["poz23"] == 4000.0    # costs
    assert result["poz26"] == 6500.0    # income
    assert result["poz29"] == 6500      # tax base (rounded to int)
    assert result["poz30_rate"] == 0.19
    assert result["poz31"] == 1235.0    # 6500 * 0.19
    assert result["poz32"] == 50.0      # foreign tax
    assert result["tax_final"] == 1185  # 1235 - 50


def test_pitzg_fields():
    result = calculate_pit38_fields(
        total_proceeds=10000.0,
        total_costs=4000.0,
        total_gain=6000.0,
        total_dividends=500.0,
        foreign_tax=50.0,
    )
    assert result["pitzg_poz29"] == 6000.0   # gain
    assert result["pitzg_poz30"] == 50.0     # foreign tax


def test_zero_values():
    result = calculate_pit38_fields(
        total_proceeds=0.0,
        total_costs=0.0,
        total_gain=0.0,
        total_dividends=0.0,
        foreign_tax=0.0,
    )
    assert result["poz22"] == 0.0
    assert result["poz23"] == 0.0
    assert result["poz26"] == 0.0
    assert result["poz29"] == 0
    assert result["poz31"] == 0.0
    assert result["tax_final"] == 0


def test_tax_cannot_be_negative():
    """When foreign tax exceeds computed tax, tax_final should be 0."""
    result = calculate_pit38_fields(
        total_proceeds=1000.0,
        total_costs=0.0,
        total_gain=1000.0,
        total_dividends=0.0,
        foreign_tax=500.0,
    )
    # 1000 * 0.19 = 190, 190 - 500 = -310 -> clamped to 0
    assert result["tax_final"] == 0


def test_rounding_tax_base():
    """poz29 should round poz26 to nearest integer."""
    result = calculate_pit38_fields(
        total_proceeds=1000.50,
        total_costs=0.0,
        total_gain=1000.50,
        total_dividends=0.0,
        foreign_tax=0.0,
    )
    assert result["poz26"] == 1000.50
    assert result["poz29"] == 1000  # rounds down .50 -> 1000 (banker's rounding)


def test_rounding_tax_base_up():
    result = calculate_pit38_fields(
        total_proceeds=1000.51,
        total_costs=0.0,
        total_gain=1000.51,
        total_dividends=0.0,
        foreign_tax=0.0,
    )
    assert result["poz29"] == 1001


def test_with_example_fifo_values():
    """Cross-check with known E2E FIFO results."""
    result = calculate_pit38_fields(
        total_proceeds=11860.44,
        total_costs=5944.98,
        total_gain=5915.45,
        total_dividends=-25.45,
        foreign_tax=14.66,
    )
    assert result["poz22"] == pytest.approx(11834.99, abs=0.01)
    assert result["poz23"] == pytest.approx(5944.98, abs=0.01)
    assert result["poz29"] == 5890
    assert result["tax_final"] == 1104
