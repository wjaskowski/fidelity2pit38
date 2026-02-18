# fidelity2pit38

Converts transaction history from Fidelity to PIT-38.

## Usage
```sh
uv run fidelity2pit38 "Transaction history.csv"
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

If instead of FIFO, you prefer to use information about the specific stocks you sold use
```sh
uv run fidelity2pit38 "Transaction history.csv" --method custom --custom_summary stock-sales.txt
```
The stock-sales.txt file can be created by copy&paste from Fidelity -> Statements / Records -> Custom Transaction Summary -> (Select year) View Transactions -> Stock Sales (select & copy the whole table)


## Limitations (TODO)
- Doesn't take into consideration previous years

## DISCLAIMER
This script is provided "as is" for informational purposes only. 
I am not a certified accountant or tax advisor, and this script does not constitute professional tax advice. As I consulted AI to write this script, it might be worse than random. Thus, use at your own risk; always consult a qualified professional for personalized guidance.
