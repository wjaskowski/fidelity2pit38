# fidelity2pit38

Creates PIT-38 from your Fidelity history.

**News**: Updated to handle 2025. It might work correctly.

## Documentation

- Tax/legal basis used in this project: [`podstawa-podatkowa.md`](podstawa-podatkowa.md)
- Glossary of transaction/sales fields and symbols: [`glosariusz-danych.md`](glosariusz-danych.md)

## Usage

1. Go to your Fidelity. For each year, download `Transaction history 20XX`.csv for this year (-> Stock Plan Accounts -> Activity -> Custom Date -> Jan-01-2025-Dec-31-20XX -> Export -> `Transaction history 20XX.csv`)
 
2. Place your `Transaction history*.csv` files in the `data/` directory and run:
```sh
uv run fidelity2pit38
```

The tool auto-discovers files in `data/` and defaults to the previous calendar year.

You can also point to a different directory and select a different year:
```sh
uv run fidelity2pit38 --data-dir /path/to/my-data --year 2024
```

Example output
```sh
PIT-38 for year 2024:

Czesc C/D - Dochody ze zbycia papierow wartosciowych (art. 30b):
  Poz. 22 (Przychod): 34033.91 PLN
  Poz. 23 (Koszty uzyskania): 8865.00 PLN
  Poz. 26 (Dochod): 25168.91 PLN
  Poz. 29 (Podstawa opodatkowania): 25169.00 PLN
  Poz. 30 (Stawka podatku): 19%
  Poz. 31 (Podatek): 4782.11 PLN
  Poz. 32 (Podatek zaplacony za granica): 0.00 PLN
  Poz. 33 (Podatek nalezny): 4782.00 PLN

Czesc G - Zryczaltowany podatek (art. 30a ust. 1 pkt 1-5):
  Podstawa czesci G (lacznie): 52.47 PLN
  Poz. 45 (Podatek 19% od przychodow czesci G): 9.97 PLN
  Poz. 46 (Podatek zaplacony za granica): 7.86 PLN
  Poz. 47 (Do zaplaty): 2.00 PLN

PIT-ZG (dochody zagraniczne):
  Poz. 29 (Dochod z art. 30b ust.5 i 5b): 25168.91 PLN
  Poz. 30 (Podatek zaplacony za granica): 0.00 PLN
```

### Non-FIFO

**Warning**: Less tested then the default FIFO method

If instead of FIFO, you prefer to use information about the specific stocks you sold use
```sh
uv run fidelity2pit38 --method custom
```
The `stock-sales*.txt` files are auto-discovered in the data directory.
The stock-sales.txt file can be created by copy&paste from Fidelity -> Statements / Records -> Custom Transaction Summary -> (Select year) View Transactions -> Stock Sales (select & copy the whole table)


## DISCLAIMER
This script is provided "as is" for informational purposes only.
I am not a certified accountant or tax advisor, and this script does not constitute professional tax advice. As I consulted AI to write this script, it might be worse than random. Thus, use at your own risk; always consult a qualified professional for personalized guidance.
