from decimal import Decimal

import pytest

from fidelity2pit38 import PIT38Fields, calculate_pit38_fields
from fidelity2pit38.core import _round_tax


class TestRoundTax:
    """Test _round_tax per Ordynacja Podatkowa art. 63 §1."""

    def test_below_50_groszy_dropped(self):
        assert _round_tax(1234.49) == 1234

    def test_50_groszy_rounded_up(self):
        assert _round_tax(1234.50) == 1235

    def test_above_50_groszy_rounded_up(self):
        assert _round_tax(1234.99) == 1235

    def test_exact_zloty(self):
        assert _round_tax(1234.00) == 1234

    def test_zero(self):
        assert _round_tax(0.0) == 0

    def test_negative_returns_zero(self):
        assert _round_tax(-100.0) == 0

    def test_small_positive(self):
        assert _round_tax(0.49) == 0

    def test_one_grosz_above_half(self):
        assert _round_tax(0.50) == 1

    def test_float_artifact_just_below_half_stays_down(self):
        # Protect against float artifacts where floor(value + 0.5) can over-round.
        assert _round_tax(0.49999999999999994) == 0


class TestCapitalGainsSection:
    """Section C/D: capital gains from stock sales (art. 30b)."""

    def test_basic_calculation(self):
        result = calculate_pit38_fields(
            total_proceeds=10000.0,
            total_costs=4000.0,
            total_gain=6000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert isinstance(result, PIT38Fields)
        assert result["poz22"] == Decimal("10000.00")   # proceeds only (no dividends)
        assert result["poz23"] == Decimal("4000.00")    # costs
        assert result["poz26"] == Decimal("6000.00")    # income
        assert result["poz29"] == Decimal("6000")       # tax base
        assert result["poz30_rate"] == Decimal("0.19")
        assert result["poz31"] == Decimal("1140.00")    # 6000 * 0.19
        assert result["poz32"] == Decimal("0.00")       # US doesn't withhold on stock sales
        assert result["tax_final"] == Decimal("1140")   # 1140 - 0

    def test_dividends_excluded_from_poz22(self):
        """Dividends go to Section G, NOT Poz. 22."""
        result = calculate_pit38_fields(
            total_proceeds=10000.0,
            total_costs=4000.0,
            total_gain=6000.0,
            total_dividends=500.0,
            foreign_tax_dividends=50.0,
        )
        # Poz. 22 should NOT include dividends
        assert result["poz22"] == Decimal("10000.00")
        assert result["poz23"] == Decimal("4000.00")
        assert result["poz26"] == Decimal("6000.00")

    def test_zero_values(self):
        result = calculate_pit38_fields(
            total_proceeds=0.0,
            total_costs=0.0,
            total_gain=0.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz22"] == Decimal("0.00")
        assert result["poz23"] == Decimal("0.00")
        assert result["poz26"] == Decimal("0.00")
        assert result["poz29"] == Decimal("0")
        assert result["poz31"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("0")

    def test_tax_cannot_be_negative(self):
        """Poz. 33 should be 0 when foreign tax exceeds computed tax."""
        result = calculate_pit38_fields(
            total_proceeds=1000.0,
            total_costs=0.0,
            total_gain=1000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        # Poz. 32 is always 0 for US stocks (no withholding on stock sales)
        assert result["poz32"] == Decimal("0.00")
        assert result["tax_final"] == Decimal(str(_round_tax(1000 * 0.19)))

    def test_foreign_tax_credit_for_capital_gains_applied(self):
        result = calculate_pit38_fields(
            total_proceeds=10000.0,
            total_costs=0.0,
            total_gain=10000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
            foreign_tax_capital_gains=500.0,
        )
        assert result["poz31"] == Decimal("1900.00")
        assert result["poz32"] == Decimal("500.00")
        assert result["tax_final"] == Decimal("1400")

    def test_foreign_tax_credit_for_capital_gains_capped(self):
        result = calculate_pit38_fields(
            total_proceeds=1000.0,
            total_costs=0.0,
            total_gain=1000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
            foreign_tax_capital_gains=500.0,
        )
        # Poz.31 = 190, so Poz.32 cannot exceed 190
        assert result["poz31"] == Decimal("190.00")
        assert result["poz32"] == Decimal("190.00")
        assert result["tax_final"] == Decimal("0")

    def test_foreign_tax_credit_zero_not_negative_zero(self):
        result = calculate_pit38_fields(
            total_proceeds=1000.0,
            total_costs=0.0,
            total_gain=1000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
            foreign_tax_capital_gains=-0.0,
        )
        assert result["poz32"] == Decimal("0.00")
        assert not result["poz32"].is_signed()

    def test_rounding_tax_base_49_groszy(self):
        """poz29 rounds down when < 50 groszy."""
        result = calculate_pit38_fields(
            total_proceeds=1000.49,
            total_costs=0.0,
            total_gain=1000.49,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz29"] == Decimal("1000")

    def test_rounding_tax_base_50_groszy(self):
        """poz29 rounds up when >= 50 groszy."""
        result = calculate_pit38_fields(
            total_proceeds=1000.50,
            total_costs=0.0,
            total_gain=1000.50,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz29"] == Decimal("1001")

    def test_rounding_tax_base_51_groszy(self):
        result = calculate_pit38_fields(
            total_proceeds=1000.51,
            total_costs=0.0,
            total_gain=1000.51,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz29"] == Decimal("1001")

    def test_loss_gives_zero_tax_base(self):
        """When costs > proceeds, poz26 is negative, poz29 should be 0."""
        result = calculate_pit38_fields(
            total_proceeds=1000.0,
            total_costs=2000.0,
            total_gain=-1000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz26"] == Decimal("-1000.00")
        assert result["poz29"] == Decimal("0")  # _round_tax clamps negative to 0
        assert result["poz31"] == Decimal("0.00")
        assert result["tax_final"] == Decimal("0")


class TestDividendSection:
    """Section G: dividends (art. 30a)."""

    def test_dividend_tax_calculation(self):
        result = calculate_pit38_fields(
            total_proceeds=0.0,
            total_costs=0.0,
            total_gain=0.0,
            total_dividends=1000.0,
            foreign_tax_dividends=150.0,
        )
        # 19% of 1000 = 190
        assert result["poz45"] == Decimal("190.00")
        # Foreign tax capped at Polish tax
        assert result["poz46"] == Decimal("150.00")
        # 190 - 150 = 40
        assert result["poz47"] == Decimal("40")

    def test_foreign_tax_capped_at_polish_tax(self):
        """Foreign tax credit cannot exceed Polish 19% tax on dividends."""
        result = calculate_pit38_fields(
            total_proceeds=0.0,
            total_costs=0.0,
            total_gain=0.0,
            total_dividends=100.0,
            foreign_tax_dividends=50.0,  # 50% withholding > 19% Polish tax
        )
        # 19% of 100 = 19
        assert result["poz45"] == Decimal("19.00")
        # Credit capped at 19, not 50
        assert result["poz46"] == Decimal("19.00")
        # 19 - 19 = 0
        assert result["poz47"] == Decimal("0")

    def test_zero_dividends(self):
        result = calculate_pit38_fields(
            total_proceeds=5000.0,
            total_costs=2000.0,
            total_gain=3000.0,
            total_dividends=0.0,
            foreign_tax_dividends=0.0,
        )
        assert result["poz45"] == Decimal("0.00")
        assert result["poz46"] == Decimal("0.00")
        assert result["poz47"] == Decimal("0")

    def test_negative_dividends_net(self):
        """Reinvestment can make net dividends negative; tax should be 0."""
        result = calculate_pit38_fields(
            total_proceeds=0.0,
            total_costs=0.0,
            total_gain=0.0,
            total_dividends=-50.0,
            foreign_tax_dividends=5.0,
        )
        # 19% of -50 = -9.50 -> ceil to grosze -> -9.50 -> but poz47 clamped
        # Negative dividends shouldn't produce positive tax
        assert result["poz47"] == Decimal("0")


class TestPitZG:
    """PIT-ZG attachment fields."""

    def test_pitzg_reflects_capital_gains(self):
        result = calculate_pit38_fields(
            total_proceeds=10000.0,
            total_costs=4000.0,
            total_gain=6000.0,
            total_dividends=500.0,
            foreign_tax_dividends=50.0,
        )
        assert result["pitzg_poz29"] == Decimal("6000.00")  # capital gains only
        assert result["pitzg_poz30"] == Decimal("0.00")      # US doesn't withhold on stock sales

    def test_pitzg_with_zero_gain(self):
        result = calculate_pit38_fields(
            total_proceeds=1000.0,
            total_costs=1000.0,
            total_gain=0.0,
            total_dividends=100.0,
            foreign_tax_dividends=15.0,
        )
        assert result["pitzg_poz29"] == Decimal("0.00")
        assert result["pitzg_poz30"] == Decimal("0.00")


class TestPrintedFormLayout:
    def _sample_fields(self, year):
        return PIT38Fields(
            poz22=Decimal("100.00"),
            poz23=Decimal("40.00"),
            poz26=Decimal("60.00"),
            poz29=Decimal("60"),
            poz30_rate=Decimal("0.19"),
            poz31=Decimal("11.40"),
            poz32=Decimal("0.00"),
            tax_final=Decimal("11"),
            poz45=Decimal("19.00"),
            poz46=Decimal("9.92"),
            poz47=Decimal("9"),
            pitzg_poz29=Decimal("60.00"),
            pitzg_poz30=Decimal("0.00"),
            section_g_uncollected_tax=Decimal("0.00"),
            year=year,
        )

    def test_print_uses_2024_section_g_positions(self, capsys):
        self._sample_fields(year=2024).print()
        out = capsys.readouterr().out

        assert "Poz. 26 (Dochod)" in out
        assert "Poz. 27 (Strata)" in out
        assert "Poz. 28 (Straty z lat ubieglych)" in out
        assert "Poz. 29 (Podstawa opodatkowania)" in out
        assert "Poz. 33 (Podatek nalezny)" in out
        assert "Poz. 45 (Podatek 19% od przychodow czesci G)" in out
        assert "Poz. 46 (Podatek zaplacony za granica)" in out
        assert "Poz. 47 (Do zaplaty)" in out
        assert "Poz. 35 (Podatek nalezny)" not in out
        assert "Poz. 49 (Do zaplaty)" not in out

    def test_print_uses_2025_section_g_positions(self, capsys):
        self._sample_fields(year=2025).print()
        out = capsys.readouterr().out

        assert "Poz. 26 (Przychod - razem)" in out
        assert "Poz. 27 (Koszty uzyskania - razem)" in out
        assert "Poz. 28 (Dochod)" in out
        assert "Poz. 29 (Strata)" in out
        assert "Poz. 30 (Straty z lat ubieglych)" in out
        assert "Poz. 31 (Podatek)" not in out
        assert "Poz. 31 (Podstawa opodatkowania)" in out
        assert "Poz. 32 (Stawka podatku)" in out
        assert "Poz. 33 (Podatek)" in out
        assert "Poz. 34 (Podatek zaplacony za granica)" in out
        assert "Poz. 35 (Podatek nalezny)" in out
        assert "Poz. 30 (Stawka podatku)" not in out
        assert "Poz. 46 (Podatek niepobrany przez platnika)" in out
        assert "Poz. 47 (Podatek 19% od przychodow czesci G)" in out
        assert "Poz. 48 (Podatek zaplacony za granica)" in out
        assert "Poz. 49 (Do zaplaty)" in out
        assert "Poz. 45 (Podatek 19% od przychodow czesci G)" not in out

    def test_print_raises_for_unsupported_year(self):
        with pytest.raises(ValueError, match="Supported years: 2024, 2025"):
            self._sample_fields(year=2026).print()
