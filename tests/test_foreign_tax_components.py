import pandas as pd
import pytest

from fidelity2pit38 import compute_foreign_tax_capital_gains


def test_capital_gains_foreign_tax_excludes_dividend_withholding():
    merged = pd.DataFrame(
        {
            "Transaction type": [
                "NON-RESIDENT TAX DIVIDEND RECEIVED",
                "NON-RESIDENT TAX ON CAPITAL GAIN",
            ],
            "amount_pln": [-10.0, -5.0],
            "settlement_date": [pd.Timestamp("2024-12-31"), pd.Timestamp("2024-12-31")],
        }
    )
    tax = compute_foreign_tax_capital_gains(merged)
    assert tax == pytest.approx(5.0, abs=0.01)
