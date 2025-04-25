#!/usr/bin/env python3
# DISCLAIMER: This script is provided "as is" for informational purposes only.
# I am not a certified accountant or tax advisor; consult a professional for personalized guidance.

import argparse
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland
import logging

# 0. Parse command-line arguments
parser = argparse.ArgumentParser(description='Compute PIT-38 summary from transactions or custom summary')
parser.add_argument('tx_csv', help='Path to the transaction history CSV file')
parser.add_argument('--method', choices=['fifo', 'custom'], default='fifo',
                    help='Use FIFO (default) or custom summary for matching')
parser.add_argument('--custom_summary',
                    help='Path to custom transaction summary (TXT) for method=custom')
args = parser.parse_args()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# 1. Load NBP USD/PLN exchange rates from NBP archive URLs
NBP_RATE_URLS = [
    "https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_2024.csv",
    # add more URLs for other years if needed
]
rates_list = []
for url in NBP_RATE_URLS:
    df = pd.read_csv(url, sep=';', encoding='cp1250', header=0, dtype=str)
    df = df[df['data'].str.match(r"\d{8}", na=False)]
    df['date'] = pd.to_datetime(df['data'], format='%Y%m%d', errors='coerce')
    df['rate'] = pd.to_numeric(df['1USD'].str.replace(',', '.'), errors='coerce')
    df = df.dropna(subset=['date', 'rate'])[['date', 'rate']]
    rates_list.append(df)
nbp_rates = pd.concat(rates_list).drop_duplicates('date').sort_values('date').reset_index(drop=True)
logging.info(f"Loaded {len(nbp_rates)} exchange-rate entries.")

# 2. Load and clean transaction history
tx_raw = pd.read_csv(args.tx_csv)
tx = tx_raw.copy()
# Clean transaction type metadata
tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
# Parse fields
tx['trade_date'] = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
tx['shares'] = pd.to_numeric(tx['Shares'], errors='coerce')
tx['amount_usd'] = pd.to_numeric(
    tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')
logging.info(f"Loaded {len(tx)} transactions; {tx['trade_date'].isna().sum()} invalid dates.")

# 3. Determine settlement dates with Polish business-day rules
pol_cal = Poland()
cbd2 = CustomBusinessDay(calendar=pol_cal, n=2)
cbd1 = CustomBusinessDay(calendar=pol_cal, n=1)
switch_date = pd.Timestamp('2024-05-28')
settlements = []
for d in tx['trade_date']:
    if pd.isna(d):
        settlements.append(pd.NaT)
    elif d < switch_date:
        settlements.append(d + cbd2)
    else:
        settlements.append(d + cbd1)
tx['settlement_date'] = settlements

# 4. Assign rate_date = last business day before settlement_date
#    to comply with art.11a PIT: use NBP rate from the business day preceding the revenue date
tx['rate_date'] = tx['settlement_date'] - cbd1

# 5. Merge rates: last available on or before rate_date
tx_sorted = tx.sort_values('rate_date').reset_index(drop=True)
rates_sorted = nbp_rates.sort_values('date').reset_index(drop=True)
merged = pd.merge_asof(
    tx_sorted,
    rates_sorted.rename(columns={'date': 'rate_date'}),
    left_on='rate_date', right_on='rate_date',
    direction='backward'
)
missing = merged['rate'].isna().sum()
if missing:
    logging.error(f"{missing} transactions missing exchange rate.")
merged['amount_pln'] = merged['amount_usd'] * merged['rate']

# 6. Process based on chosen method
total_proceeds = total_costs = total_gain = 0.0
if args.method == 'fifo':
    # FIFO matching
    buys = merged[merged['Transaction type'].str.contains('YOU BOUGHT', na=False)].sort_values('settlement_date').copy()
    sells = merged[merged['Transaction type'].str.contains('YOU SOLD', na=False)].sort_values('settlement_date').copy()
    buys['remaining'] = buys['shares']
    allocs = []
    for _, sale in sells.iterrows():
        qty = abs(sale['shares'])
        price_per = sale['amount_pln'] / qty if qty else 0
        while qty > 0:
            idx = buys[buys['remaining'] > 0].index.min()
            lot = buys.loc[idx]
            match = min(qty, lot['remaining'])
            cost_per = (-lot['amount_pln'])/lot['shares'] if lot['shares'] else 0
            proceeds = round(match * price_per, 2)
            cost = round(match * cost_per, 2)
            allocs.append({'proceeds': proceeds, 'cost': cost})
            buys.at[idx, 'remaining'] -= match
            qty -= match
    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs = sum(a['cost'] for a in allocs)
    total_gain = round(total_proceeds - total_costs, 2)
    logging.info(f"FIFO: matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
elif args.method == 'custom':
    if not args.custom_summary:
        parser.error("--custom_summary is required when method=custom")
    # Read custom summary TXT for sale and acquisition dates
    custom = pd.read_csv(args.custom_summary, sep='\t', engine='python')
    # Parse dates and quantity
    custom['Date sold'] = pd.to_datetime(
        custom['Date sold or transferred'], format='%b-%d-%Y', errors='coerce')
    custom['Date acquired'] = pd.to_datetime(
        custom['Date acquired'], format='%b-%d-%Y', errors='coerce')
    custom['Quantity'] = pd.to_numeric(custom['Quantity'], errors='coerce')
    # Prepare merged dataframe with trade and settlement data
    merged['trade_date_norm'] = merged['trade_date'].dt.normalize()
    merged['source'] = merged['Transaction type'].apply(
        lambda x: 'RS' if 'RSU' in x else ('SP' if 'ESPP' in x else None)
    )
    allocs = []
    for _, row in custom.iterrows():
        sale_date = row['Date sold'].normalize()
        acq_date = row['Date acquired'].normalize()
        qty = row['Quantity']
        source = row.get('Stock source') if 'Stock source' in row else None
        # Find the sale transaction
        sale_tx = merged[(merged['trade_date_norm'] == sale_date) &
                         merged['Transaction type'].str.contains('YOU SOLD', na=False)]
        if sale_tx.empty:
            logging.error(f"No sale record found for {sale_date}")
            continue
        sale = sale_tx.iloc[0]
        price_per = sale['amount_pln'] / abs(sale['shares'])
        proceeds = round(qty * price_per, 2)
        # Find the buy transaction matching acquisition date and source
        buy_tx = merged[(merged['trade_date_norm'] == acq_date) &
                        merged['Transaction type'].str.contains('YOU BOUGHT', na=False)]
        # Further filter by source if available
        if source:
            if source == 'RS':
                buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('RSU', na=False)]
            elif source == 'SP':
                buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('ESPP', na=False)]
        if buy_tx.empty:
            logging.error(f"No buy record found for {acq_date} (source={source})")
            continue
        buy = buy_tx.iloc[0]
        cost_per = (-buy['amount_pln']) / buy['shares']
        cost = round(qty * cost_per, 2)
        allocs.append({'proceeds': proceeds, 'cost': cost})
    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs = sum(a['cost'] for a in allocs)
    total_gain = round(total_proceeds - total_costs, 2)
    logging.info(f"Custom (by specific lots): matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")

# 7. Dividends & foreign withholding (unchanged)
gross_div = merged[merged['Transaction type']=='DIVIDEND RECEIVED']['amount_pln'].sum()
reinv_div = merged[merged['Transaction type'].str.contains('REINVESTMENT', na=False)]['amount_pln'].sum()
total_dividends = gross_div + reinv_div
wd = -merged[merged['Transaction type']=='NON-RESIDENT TAX DIVIDEND RECEIVED']['amount_pln'].sum()
wk = -merged[merged['Transaction type'].str.contains('NON-RESIDENT TAX KKR WITH-HOLDING PROCESSING', na=False)]['amount_pln'].sum()
foreign_tax = round(wd + wk, 2)
logging.info(f"Dividends PLN: {total_dividends:.2f}; Foreign tax PLN: {foreign_tax:.2f}")

# 8. PIT-38 and PIT-ZG fields
poz22 = round(total_proceeds + total_dividends, 2)
poz23 = round(total_costs, 2)
poz26 = round(poz22 - poz23, 2)
poz29 = int(round(poz26))
poz30_rate = 0.19
poz31 = round(poz29 * poz30_rate, 2)
poz32 = foreign_tax
raw_tax_due = poz31 - poz32
tax_final = int(max(raw_tax_due, 0) + 0.5)
pitzg_poz29 = total_gain
pitzg_poz30 = foreign_tax

# 9. Output
print("FINAL TAX SUMMARY:")
print(f"Poz. 22 (Przychód): {poz22:.2f} PLN")
print(f"Poz. 23 (Koszty uzyskania): {poz23:.2f} PLN")
print(f"Poz. 26 (Dochód): {poz26:.2f} PLN")
print(f"Poz. 29 (Podstawa opodatkowania): {poz29}.00 PLN")
print(f"Poz. 30 (Stawka podatku): {int(poz30_rate*100)}%")
print(f"Poz. 31 (Podatek od dochodów z poz. 29): {poz31:.2f} PLN")
print(f"Poz. 32 (Podatek zapłacony za granicą): {poz32:.2f} PLN")
print(f"Poz. 33 (Podatek należny): {tax_final:.2f} PLN")
print("\nPIT-ZG:")
print(f"Poz. 29 (Dochód, o którym mowa w art. 30b ust.5 i 5b): {pitzg_poz29:.2f} PLN")
print(f"Poz. 30 (Podatek zapłacony za granicą od dochodów z poz. 29): {pitzg_poz30:.2f} PLN")
