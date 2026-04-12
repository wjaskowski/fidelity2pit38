# fidelity2pit38

[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-wojciechjap-FFDD00?style=flat&logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/wojciechjap)

Calculates Polish PIT-38 and PIT-ZG tax forms from Fidelity Stock Plan transaction history.

If you have a Fidelity Stock Plan account (RSU, ESPP) and need to file Polish taxes, this tool:
- Parses your Fidelity transaction exports (buys, sells, dividends, tax withholdings)
- Converts USD amounts to PLN using official NBP exchange rates (rate from the last business day before the transaction)
- Matches sales to purchases using FIFO or your custom lot info
- Outputs the exact values for PIT-38 fields (capital gains, dividends, foreign tax credits) and PIT-ZG

## 1. Prepare your data

### Export from Fidelity

For each year you need, download a transaction history CSV:

> Fidelity (Your Landing Page) -> **STOCK PLAN ACCOUNT** -> **Activity** -> **Custom Date** -> Jan-01-20XX to Dec-31-20XX -> **Export** -> `Transaction history 20XX.csv`

### Place files in a data directory

Create a `data/` directory (or any name you like) and put your CSV files there:

```
data/
  Transaction history 2024.csv
  Transaction history 2025.csv
```

That's all you need for the default (FIFO) mode.

### (Optional) For custom lot matching

If you want to use Fidelity's specific lot information instead of FIFO, also export stock sales summaries:

> Fidelity -> **Statements / Records** -> **Custom Transaction Summary** -> select year -> **View Transactions** -> **Stock Sales** -> select & copy the whole table -> paste into a text file

Save as `stock-sales-YYYY.txt` in the same data directory:

```
data/
  Transaction history 2024.csv
  Transaction history 2025.csv
  stock-sales-2024.txt       <- only needed for --method custom
  stock-sales-2025.txt       <- only needed for --method custom
```

## 2. Run

```sh
uv run fidelity2pit38
```

The tool auto-discovers `Transaction history*.csv` files in `data/` and defaults to the previous calendar year.

Options:

```sh
uv run fidelity2pit38 --data-dir my-data/   # different data directory
uv run fidelity2pit38 --year 2025            # specific tax year
uv run fidelity2pit38 --method custom        # use custom lot matching (requires stock-sales*.txt)
```

Supported PIT-38 layout years right now: `2024`, `2025` (Section G line numbers differ by year).

### Reports

After each run the tool writes two files to `output/` (override with `--output DIR`):

| File | Contents |
|---|---|
| `pit38_report_YYYY.csv` | Per-lot capital gains, per-dividend details, PIT-38 field summary — machine-readable audit trail |
| `pit38_report_YYYY.html` | Same data as a styled HTML page, opens automatically in your browser |

Sample output (generated from `data-sample/`):
[pit38_report_2025.csv](output-sample/pit38_report_2025.csv) &nbsp;·&nbsp; [pit38_report_2025.html](output-sample/pit38_report_2025.html)

### Example output (year 2025 layout)

The output uses color: section headers in blue, fields you must enter manually highlighted in green with a `<-- enter` marker, auto-calculated fields in grey.

In the example below, green lines (`+`) are the fields you need to fill in:

```sh
uv run fidelity2pit38 --data-dir data-sample --year 2025
```

```diff
PIT-38 for year 2025  |  Method: FIFO:
(<-- enter = fill in the tax form; remaining fields are typically auto-calculated)

 Czesc C/D - Dochody ze zbycia papierow wartosciowych (art. 30b):
+  Poz. 22 (Inne przychody): 48392.00 PLN  <-- enter
+  Poz. 23 (Koszty uzyskania przychodow): 16082.14 PLN  <-- enter
   Poz. 26 (Przychod - razem): 48392.00 PLN
   Poz. 27 (Koszty uzyskania - razem): 16082.14 PLN
   Poz. 28 (Dochod): 32309.86 PLN
   Poz. 29 (Strata): 0.00 PLN
+  Poz. 30 (Straty z lat ubieglych): 0.00 PLN  <-- enter
   Poz. 31 (Podstawa opodatkowania): 32310.00 PLN
   Poz. 32 (Stawka podatku): 19%
   Poz. 33 (Podatek): 6138.90 PLN
+  Poz. 34 (Podatek zaplacony za granica): 0.00 PLN  <-- enter
   Poz. 35 (Podatek nalezny): 6139.00 PLN

 Czesc G - Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5):
+  Poz. 46 (Podatek niepobrany przez platnika): 0.00 PLN  <-- enter
+  Poz. 47 (Podatek 19% od przychodow czesci G): 10.52 PLN  <-- enter
+  Poz. 48 (Podatek zaplacony za granica): 8.32 PLN  <-- enter
   Poz. 49 (Do zaplaty): 2.20 PLN

 PIT-ZG (dochody zagraniczne):
+  Poz. 29 (Dochod z art. 30b ust.5 i 5b): 32309.86 PLN  <-- enter
+  Poz. 30 (Podatek zaplacony za granica): 0.00 PLN  <-- enter
```

## Running tests

```sh
uv run tests
```

## Contributing

CI runs on every push and pull request (lint + tests must pass before merging).

To get the same checks locally before you push, install the git pre-commit hook once after cloning:

```sh
sh scripts/install-hooks.sh
```

The hook runs `ruff`, `pytest`, regenerates `output-sample/` and updates the README example automatically on each commit.

## Documentation

- Tax/legal basis used in this project: [`podstawa-podatkowa.md`](podstawa-podatkowa.md)
- Glossary of transaction/sales fields and symbols: [`glosariusz-danych.md`](glosariusz-danych.md)

## DISCLAIMER

This script is provided "as is" for informational purposes only.
I am not a certified accountant or tax advisor, and this script does not constitute professional tax advice. Use at your own risk; always consult a qualified professional for personalized guidance.
