#!/usr/bin/env python3
# DISCLAIMER: This script is provided "as is" for informational purposes only.
# I am not a certified accountant or tax advisor; consult a professional for personalized guidance.

import argparse
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland
import logging
from typing import List, Tuple, Optional

# Switch to T+1 on and after this date
SWITCH_DATE: pd.Timestamp = pd.Timestamp('2024-05-28')


def load_nbp_rates(urls: List[str]) -> pd.DataFrame:
    """
    Load and concatenate NBP USD/PLN rates from given archive URLs.
    Returns DataFrame with 'date' and 'rate'.
    """
    rates: List[pd.DataFrame] = []
    for url in urls:
        df = pd.read_csv(url, sep=';', encoding='cp1250', header=0, dtype=str)
        df = df[df['data'].str.match(r"\d{8}", na=False)]
        df['date'] = pd.to_datetime(df['data'], format='%Y%m%d', errors='coerce')
        df['rate'] = pd.to_numeric(df['1USD'].str.replace(',', '.'), errors='coerce')
        df = df.dropna(subset=['date', 'rate'])[['date', 'rate']]
        rates.append(df)
    rates_df = pd.concat(rates).drop_duplicates('date').sort_values('date').reset_index(drop=True)
    logging.info(f"Loaded {len(rates_df)} exchange-rate entries.")
    return rates_df


def calculate_settlement_dates(trade_dates: pd.Series) -> List[Optional[pd.Timestamp]]:
    """
    Calculate settlement dates for US equity trades.
    Before SWITCH_DATE: T+2, on/after: T+1, using US Federal holidays.
    """
    us_bd1 = CustomBusinessDay(calendar=USFederalHolidayCalendar(), n=1)
    us_bd2 = CustomBusinessDay(calendar=USFederalHolidayCalendar(), n=2)
    settlements: List[Optional[pd.Timestamp]] = []
    for d in trade_dates:
        if pd.isna(d):
            settlements.append(pd.NaT)
        elif d < SWITCH_DATE:
            settlements.append(d + us_bd2)
        else:
            settlements.append(d + us_bd1)
    return settlements


def calculate_rate_dates(settlement_dates: List[Optional[pd.Timestamp]]) -> List[Optional[pd.Timestamp]]:
    """
    Calculate rate_date as the previous Polish business day before settlement_date.
    """
    pl_bd1 = CustomBusinessDay(calendar=Poland(), n=1)
    return [sd - pl_bd1 if sd is not None and not pd.isna(sd) else pd.NaT for sd in settlement_dates]


def merge_with_rates(tx_df: pd.DataFrame, rates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge transaction DataFrame with exchange rates by asof on 'rate_date'.
    """
    tx_sorted = tx_df.sort_values('rate_date').reset_index(drop=True)
    rates_sorted = rates_df.rename(columns={'date': 'rate_date'}).sort_values('rate_date').reset_index(drop=True)
    merged = pd.merge_asof(
        tx_sorted,
        rates_sorted,
        on='rate_date',
        direction='backward'
    )
    missing: int = merged['rate'].isna().sum()
    if missing:
        logging.error(f"{missing} transactions missing exchange rate.")
    merged['amount_pln'] = merged['amount_usd'] * merged['rate']
    return merged


def process_fifo(merged: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Compute FIFO-based proceeds, costs, and total gain.
    Returns tuple (total_proceeds, total_costs, total_gain).
    """
    buys = merged[merged['Transaction type'].str.contains('YOU BOUGHT', na=False)].sort_values('settlement_date').copy()
    sells = merged[merged['Transaction type'].str.contains('YOU SOLD', na=False)].sort_values('settlement_date').copy()
    buys['remaining'] = buys['shares']
    allocs: List[dict] = []
    for _, sale in sells.iterrows():
        qty = abs(sale['shares'])
        price_per = sale['amount_pln'] / qty if qty else 0
        while qty > 0:
            idx = buys[buys['remaining'] > 0].index.min()
            lot = buys.loc[idx]
            match_qty = min(qty, lot['remaining'])
            cost_per = (-lot['amount_pln']) / lot['shares'] if lot['shares'] else 0
            proceeds = round(match_qty * price_per, 2)
            cost = round(match_qty * cost_per, 2)
            allocs.append({'proceeds': proceeds, 'cost': cost})
            buys.at[idx, 'remaining'] -= match_qty
            qty -= match_qty
    total_proceeds: float = sum(a['proceeds'] for a in allocs)
    total_costs: float = sum(a['cost'] for a in allocs)
    total_gain: float = round(total_proceeds - total_costs, 2)
    logging.info(f"FIFO: matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def process_custom(merged: pd.DataFrame, custom_summary_path: str) -> Tuple[float, float, float]:
    """
    Compute proceeds, costs, and gain based on custom summary matching.
    Returns tuple (total_proceeds, total_costs, total_gain).
    """
    custom = pd.read_csv(custom_summary_path, sep='\t', engine='python')
    custom['Date sold'] = pd.to_datetime(custom['Date sold or transferred'], format='%b-%d-%Y', errors='coerce').dt.normalize()
    custom['Date acquired'] = pd.to_datetime(custom['Date acquired'], format='%b-%d-%Y', errors='coerce').dt.normalize()
    custom['Quantity'] = pd.to_numeric(custom['Quantity'], errors='coerce')
    merged['trade_date_norm'] = merged['trade_date'].dt.normalize()
    allocs: List[dict] = []
    for _, row in custom.iterrows():
        sale_tx = merged[(merged['trade_date_norm'] == row['Date sold']) & merged['Transaction type'].str.contains('YOU SOLD', na=False)]
        if sale_tx.empty:
            logging.error(f"No sale record for {row['Date sold']}")
            continue
        sale = sale_tx.iloc[0]
        price_per = sale['amount_pln'] / abs(sale['shares'])
        proceeds = round(row['Quantity'] * price_per, 2)
        buy_tx = merged[(merged['trade_date_norm'] == row['Date acquired']) & merged['Transaction type'].str.contains('YOU BOUGHT', na=False)]
        if buy_tx.empty:
            logging.error(f"No buy record for {row['Date acquired']}")
            continue
        buy = buy_tx.iloc[0]
        cost_per = (-buy['amount_pln']) / buy['shares']
        cost = round(row['Quantity'] * cost_per, 2)
        allocs.append({'proceeds': proceeds, 'cost': cost})
    total_proceeds: float = sum(a['proceeds'] for a in allocs)
    total_costs: float = sum(a['cost'] for a in allocs)
    total_gain: float = round(total_proceeds - total_costs, 2)
    logging.info(f"Custom: matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def compute_dividends_and_tax(merged: pd.DataFrame) -> Tuple[float, float]:
    """
    Compute total dividends and foreign withholding tax.
    Returns tuple (total_dividends, foreign_tax).
    """
    gross_div: float = merged.loc[merged['Transaction type'] == 'DIVIDEND RECEIVED', 'amount_pln'].sum()
    reinv_div: float = merged.loc[merged['Transaction type'].str.contains('REINVESTMENT', na=False), 'amount_pln'].sum()
    total_div: float = gross_div + reinv_div
    wd: float = -merged.loc[merged['Transaction type'] == 'NON-RESIDENT TAX DIVIDEND RECEIVED', 'amount_pln'].sum()
    wk: float = -merged.loc[merged['Transaction type'].str.contains('NON-RESIDENT TAX KKR WITH-HOLDING PROCESSING', na=False), 'amount_pln'].sum()
    foreign_tax: float = round(wd + wk, 2)
    logging.info(f"Dividends PLN: {total_div:.2f}; Foreign tax PLN: {foreign_tax:.2f}")
    return total_div, foreign_tax


def main() -> None:
    parser = argparse.ArgumentParser(description='Compute PIT-38 summary from Fidelity transactions')
    parser.add_argument('tx_csv', help='Path to the transaction history CSV file')
    parser.add_argument('--method', choices=['fifo', 'custom'], default='fifo', help='Use FIFO (default) or custom summary')
    parser.add_argument('--custom_summary', help='Path to custom transaction summary (TXT) for method=custom')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Load data
    tx = pd.read_csv(args.tx_csv)
    tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
    tx['trade_date'] = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
    tx['shares'] = pd.to_numeric(tx['Shares'], errors='coerce')
    tx['amount_usd'] = pd.to_numeric(tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')

    # Exchange rates
    NBP_RATE_URLS: List[str] = [
        "https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_2024.csv",
        # add more URLs for other years
    ]
    nbp_rates: pd.DataFrame = load_nbp_rates(NBP_RATE_URLS)

    # Settlement and rate dates
    tx['settlement_date'] = calculate_settlement_dates(tx['trade_date'])
    tx['rate_date'] = calculate_rate_dates(tx['settlement_date'])

    # Merge rates
    merged: pd.DataFrame = merge_with_rates(tx, nbp_rates)

    # Choose method
    if args.method == 'fifo':
        total_proceeds, total_costs, total_gain = process_fifo(merged)
    else:
        if not args.custom_summary:
            parser.error("--custom_summary is required when method=custom")
        total_proceeds, total_costs, total_gain = process_custom(merged, args.custom_summary)

    # Dividends and foreign tax
    total_dividends, foreign_tax = compute_dividends_and_tax(merged)

    # PIT-38 and PIT-ZG calculations
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

    # Output
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
    print(f"Poz. 30 (Podatek zapłacony za granicą od dochodów z poz. 29): {pitzg_poz30:.2f}")


if __name__ == '__main__':
    main()
