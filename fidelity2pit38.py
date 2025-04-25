#!/usr/bin/env python3
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

import argparse

# 0. Parse command-line arguments
parser = argparse.ArgumentParser(description='Compute PIT-38 summary from transactions CSV')
parser.add_argument('tx_csv', help='Path to the transaction history CSV file')
args = parser.parse_args()
TX_CSV = args.tx_csv

# 1. Load and prepare multi-year NBP USD/PLN exchange rates from NBP archive URLs
#    Fetch directly from NBP static archive (semicolon-separated, cp1250 encoding)
NBP_RATE_URLS = [
    "https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_2024.csv",
    # add URLs for other years here, e.g. 2025
]
rates_list = []
for url in NBP_RATE_URLS:
    df_rates = pd.read_csv(
        url,
        sep=';',
        encoding='cp1250',
        header=0,
        dtype=str  # read as string to filter correctly
    )
    # Keep only rows where 'data' column is an 8-digit date
    df_rates = df_rates[df_rates['data'].str.match(r"\d{8}", na=False)]
    df_rates['date'] = pd.to_datetime(df_rates['data'], format='%Y%m%d', errors='coerce')
    df_rates['rate'] = pd.to_numeric(df_rates['1USD'].str.replace(',', '.'), errors='coerce')
    df_rates = df_rates.dropna(subset=['date', 'rate'])[['date', 'rate']]
    rates_list.append(df_rates)
nbp_rates = (
    pd.concat(rates_list)
      .drop_duplicates('date')
      .sort_values('date')
      .reset_index(drop=True)
)
logging.info(f"Loaded {len(nbp_rates)} exchange-rate entries from NBP URLs.")

# 2. Load transaction history and parse key fields# 2. Load transaction history and parse key fields
#    also clean 'Transaction type' by stripping any metadata after semicolon
TX_RAW = pd.read_csv(TX_CSV)
tx = TX_RAW.copy()
# Clean the transaction type: take only text before any semicolon
tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
# Parse dates, shares, amounts
tx['trade_date'] = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
tx['shares'] = pd.to_numeric(tx['Shares'], errors='coerce')
tx['amount_usd'] = pd.to_numeric(
    tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce'
)
logging.info(f"Loaded {len(tx)} transactions; {tx['trade_date'].isna().sum()} with invalid dates.")

# 3. Settlement dates: T+2 before 2024-05-28, T+1 thereafter, using Polish business days Settlement dates: T+2 before 2024-05-28, T+1 thereafter, using Polish business days
pol_calendar = Poland()
cbd2 = CustomBusinessDay(calendar=pol_calendar, n=2)
cbd1 = CustomBusinessDay(calendar=pol_calendar, n=1)
switch_date = pd.Timestamp('2024-05-28')
settlement_dates = []
for d in tx['trade_date']:
    if pd.isna(d):
        settlement_dates.append(pd.NaT)
    elif d < switch_date:
        settlement_dates.append(d + cbd2)
    else:
        settlement_dates.append(d + cbd1)
tx['settlement_date'] = settlement_dates

# 4. Merge NBP rates: last available on or before settlement_date
tx_sorted = tx.sort_values('settlement_date').reset_index(drop=True)
rates_sorted = nbp_rates.sort_values('date').reset_index(drop=True)
merged = pd.merge_asof(
    tx_sorted,
    rates_sorted.rename(columns={'date': 'rate_date'}),
    left_on='settlement_date',
    right_on='rate_date',
    direction='backward'
)
missing = merged['rate'].isna().sum()
if missing:
    logging.error(f"{missing} transactions missing exchange rate—please add missing NBP data.")
merged['amount_pln'] = merged['amount_usd'] * merged['rate']

# 5. Audit transaction types
types = merged['Transaction type'].unique()
logging.info(f"Transaction types found: {types}")

# 6. FIFO matching for capital gains
buys = merged[merged['Transaction type'].str.contains('YOU BOUGHT', na=False)].copy()
sells = merged[merged['Transaction type'].str.contains('YOU SOLD', na=False)].copy()
buys = buys.sort_values('settlement_date').reset_index(drop=True)
buys['remaining'] = buys['shares']
sells = sells.sort_values('settlement_date').reset_index(drop=True)
allocs = []
for _, sale in sells.iterrows():
    qty_to_match = abs(sale['shares'])
    sell_price_per_share = sale['amount_pln'] / qty_to_match if qty_to_match else 0
    while qty_to_match > 0:
        idx = buys[buys['remaining'] > 0].index.min()
        lot = buys.loc[idx]
        match_qty = min(qty_to_match, lot['remaining'])
        buy_cost_per_share = (-lot['amount_pln']) / lot['shares'] if lot['shares'] else 0
        proceeds = round(match_qty * sell_price_per_share, 2)
        cost = round(match_qty * buy_cost_per_share, 2)
        gain = round(proceeds - cost, 2)
        allocs.append({'proceeds': proceeds, 'cost': cost, 'gain': gain})
        buys.at[idx, 'remaining'] -= match_qty
        qty_to_match -= match_qty

# 7. Summarize capital gains
total_proceeds = sum(a['proceeds'] for a in allocs)
total_costs = sum(a['cost'] for a in allocs)
total_gain = sum(a['gain'] for a in allocs)
logging.info(f"Matched {len(allocs)} lots; Total gain PLN: {total_gain:.2f}")

# 8. Dividends, reinvestments, and foreign withholding
gross_div = merged[merged['Transaction type'] == 'DIVIDEND RECEIVED']['amount_pln'].sum()
reinvested_div = merged[merged['Transaction type'].str.contains('REINVESTMENT', na=False)]['amount_pln'].sum()
total_dividends = gross_div + reinvested_div
wd = -merged[merged['Transaction type'] == 'NON-RESIDENT TAX DIVIDEND RECEIVED']['amount_pln'].sum()
wk = -merged[merged['Transaction type'].str.contains('NON-RESIDENT TAX KKR WITH-HOLDING PROCESSING', na=False)]['amount_pln'].sum()
foreign_tax = round(wd + wk, 2)
logging.info(f"Total dividends PLN: {total_dividends:.2f}; Foreign tax credit PLN: {foreign_tax:.2f}")

# 9. Calculate and print PIT-38 and PIT-ZG fields
# PIT-38, section C (Art. 30b ust.1)
poz22 = round(total_proceeds + total_dividends, 2)      # Przychód
poz23 = round(total_costs, 2)                           # Koszty uzyskania
poz26 = round(poz22 - poz23, 2)                         # Dochód

# PIT-38, section D (Obliczenie zobowiązania)
poz29 = int(round(poz26))                               # Podstawa po zaokrągleniu do zł
poz30_rate = 0.19                                       # Stawka podatku
poz31 = round(poz29 * poz30_rate, 2)                    # Podatek od poz. 29 (w groszach)
poz32 = foreign_tax                                      # Podatek zapłacony za granicą
# Podatek należny przed zaokrągleniem (w zł i groszach)
raw_tax_due = poz31 - poz32
# Zgodnie z przepisami zaokrąglenie do pełnych złotych: grosze <50 obcinamy, >=50 zaokrąglamy w górę
tax_rounded = max(raw_tax_due, 0)
poz33 = int(tax_rounded + 0.5)                           # Finalny podatek należny w pełnych złotych

# PIT-ZG: załącznik o dochodach/przychodach i zapłaconym podatku o dochodach/przychodach i zapłaconym podatku
pitzg_poz29 = total_gain                                # Dochód z art. 30b ust.5 i 5b
pitzg_poz30 = foreign_tax                                # Podatek zapłacony za granicą od dochodów z poz. 29

print("FINAL TAX SUMMARY:")
print(f"Poz. 22 (Przychód): {poz22:.2f} PLN")
print(f"Poz. 23 (Koszty uzyskania): {poz23:.2f} PLN")
print(f"Poz. 26 (Dochód): {poz26:.2f} PLN")
print(f"Pos. 29 (Podstawa opodatkowania): {poz29}.00 PLN")
print(f"Poz. 30 (Stawka podatku): {poz30_rate*100:.0f}%")
print(f"Poz. 31 (Podatek od dochodów z poz. 29): {poz31:.2f} PLN")
print(f"Poz. 32 (Podatek zapłacony za granicą): {poz32:.2f} PLN")
print(f"Poz. 33 (Podatek należny): {poz33:.2f} PLN")
print("\nPIT-ZG:")
print(f"Poz. 29 (Dochód, o którym mowa w art. 30b ust.5 i 5b): {pitzg_poz29:.2f} PLN")
print(f"Poz. 30 (Podatek zapłacony za granicą od dochodów z poz. 29): {pitzg_poz30:.2f} PLN")
