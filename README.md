# fidelity2pit38

Creates PIT-28 from your Fidelity history.

**News**: Updated to handle 2025, but I am still checking if it works correctly.

## Documentation

- Tax/legal basis used in this project: [`podstawa-podatkowa.md`](podstawa-podatkowa.md)
- Glossary of transaction/sales fields and symbols: [`glosariusz-danych.md`](glosariusz-danych.md)

## Usage

1. Go to your Fidelity -> Stock Plan Accounts -> Activity -> Custom Date -> Jan-01-2025-Dec-31-2025 -> Export -> `Transaction history 2025.csv`
 
2. Place your `Transaction history*.csv` files in the `data/` directory and run:
```sh
uv run fidelity2pit38
```

The tool auto-discovers files in `data/` and defaults to the previous calendar year.

You can also point to a different directory and select a different year:
```sh
uv run fidelity2pit38 --data-dir /path/to/my-data --year 2024
```

Examplary output
```sh
FINAL TAX SUMMARY:
Poz. 22 (Przychód): 11929.28 PLN
Poz. 23 (Koszty uzyskania): 5976.99 PLN
Poz. 26 (Dochód): 5952.29 PLN
Pos. 29 (Podstawa opodatkowania): 5952.00 PLN
Poz. 30 (Stawka podatku): 19%
Poz. 31 (Podatek od dochodów z poz. 29): 1130.88 PLN
Poz. 32 (Podatek zapłacony za granicą): 7.34 PLN
Poz. 33 (Podatek należny): 1124.00 PLN

PIT-ZG:
Poz. 29 (Dochód, o którym mowa w art. 30b ust.5 i 5b): 5977.76 PLN
Poz. 30 (Podatek zapłacony za granicą od dochodów z poz. 29): 7.34 PLN
```

### Non-FIFO

**Warning**: untested at all

If instead of FIFO, you prefer to use information about the specific stocks you sold use
```sh
uv run fidelity2pit38 --method custom
```
The `stock-sales*.txt` files are auto-discovered in the data directory. You can also specify them explicitly:
```sh
uv run fidelity2pit38 --method custom --custom-summary data/stock-sales.txt
```
The stock-sales.txt file can be created by copy&paste from Fidelity -> Statements / Records -> Custom Transaction Summary -> (Select year) View Transactions -> Stock Sales (select & copy the whole table)


## DISCLAIMER
This script is provided "as is" for informational purposes only.
I am not a certified accountant or tax advisor, and this script does not constitute professional tax advice. As I consulted AI to write this script, it might be worse than random. Thus, use at your own risk; always consult a qualified professional for personalized guidance.
