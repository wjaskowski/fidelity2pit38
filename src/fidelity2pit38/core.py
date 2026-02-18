#!/usr/bin/env python3
# DISCLAIMER: This script is provided "as is" for informational purposes only.
# I am not a certified accountant or tax advisor; consult a professional for personalized guidance.

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland

# constant for switch from T+2 to T+1
SWITCH_DATE = pd.Timestamp('2024-05-28')


def load_nbp_rates(urls: List[str]) -> pd.DataFrame:
    """Load and merge USD/PLN exchange rates from NBP (National Bank of Poland) CSV archives.

    Fetches semicolon-separated, cp1250-encoded CSV files from static.nbp.pl,
    parses the '1USD' column (comma-decimal format) into float rates, filters
    rows whose 'data' column matches an 8-digit date pattern (YYYYMMDD), and
    deduplicates by date.

    Args:
        urls: URLs to NBP archival CSV files, e.g.
              "https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_2024.csv".

    Returns:
        DataFrame with columns ['date', 'rate'], sorted by date, deduplicated.
    """
    rates_list = []
    for url in urls:
        df = pd.read_csv(url, sep=';', encoding='cp1250', header=0, dtype=str)
        df = df[df['data'].str.match(r"\d{8}", na=False)]
        df['date'] = pd.to_datetime(df['data'], format='%Y%m%d', errors='coerce')
        df['rate'] = pd.to_numeric(df['1USD'].str.replace(',', '.'), errors='coerce')
        df = df.dropna(subset=['date', 'rate'])[['date', 'rate']]
        rates_list.append(df)
    rates = pd.concat(rates_list).drop_duplicates('date').sort_values('date').reset_index(drop=True)
    logging.info(f"Loaded {len(rates)} exchange-rate entries.")
    return rates


def calculate_settlement_dates(trade_dates: pd.Series, tx_types: pd.Series) -> pd.Series:
    """Calculate US equity settlement dates per SEC rules.

    Market trades (transaction types containing 'YOU BOUGHT', 'YOU SOLD', or
    'ESPP') settle on the next US business day(s) after the trade date, skipping
    US federal holidays and weekends:
      - Before SWITCH_DATE (2024-05-28): T+2 (two US business days)
      - On/after SWITCH_DATE:            T+1 (one US business day)

    Corporate actions and cash events (RSU vests, dividends, reinvestments,
    non-resident tax, fees) settle on the trade date itself (T+0).

    Args:
        trade_dates: Series of trade-date timestamps (NaT values pass through).
        tx_types: Series of Fidelity 'Transaction type' strings, e.g.
                  "YOU SOLD", "YOU BOUGHT ESPP###", "DIVIDEND RECEIVED".

    Returns:
        Series of settlement-date timestamps, aligned to the input index.
    """
    us_bd1 = CustomBusinessDay(calendar=USFederalHolidayCalendar(), n=1)
    us_bd2 = CustomBusinessDay(calendar=USFederalHolidayCalendar(), n=2)

    settlements: List[Optional[pd.Timestamp]] = []
    for d, ttype in zip(trade_dates, tx_types):
        if pd.isna(d):
            settlements.append(pd.NaT)
            continue
        market_tags = ['YOU BOUGHT', 'YOU SOLD', 'ESPP']
        if any(tag in ttype for tag in market_tags):
            # T+2 before SWITCH_DATE, T+1 after
            settlements.append(d + (us_bd2 if d < SWITCH_DATE else us_bd1))
        else:
            # corporate actions & cash events: immediate settlement
            settlements.append(d)
    return pd.Series(settlements, index=trade_dates.index)


def calculate_rate_dates(settlement_dates: pd.Series) -> pd.Series:
    """Determine the NBP exchange-rate lookup date for each settlement date.

    Polish tax rules require using the exchange rate published on the last
    Polish business day *before* the settlement date. This subtracts one
    Polish business day (skipping Polish public holidays and weekends) using
    the workalendar Poland calendar.

    Example: settlement on Thursday 2024-12-19 -> rate date Wednesday 2024-12-18;
             settlement on Monday 2024-12-16   -> rate date Friday 2024-12-13.

    Args:
        settlement_dates: Series of settlement-date timestamps.

    Returns:
        Series of rate-date timestamps (one Polish business day earlier).
    """
    pl_bd1 = CustomBusinessDay(calendar=Poland(), n=1)
    return settlement_dates - pl_bd1


def merge_with_rates(tx: pd.DataFrame, nbp_rates: pd.DataFrame) -> pd.DataFrame:
    """Join transactions with NBP exchange rates and compute PLN amounts.

    Performs a backward asof-merge on 'rate_date': each transaction picks up
    the most recent available NBP rate on or before its rate_date. This handles
    weekends and holidays where no rate is published. Logs an error if any
    transactions remain unmatched (rate_date before the earliest available rate).

    Adds two columns to the result: 'rate' (USD/PLN) and 'amount_pln'
    (amount_usd * rate).

    Args:
        tx: Transaction DataFrame, must contain 'rate_date' and 'amount_usd'.
        nbp_rates: Rate DataFrame with columns ['date', 'rate'].

    Returns:
        Merged DataFrame sorted by rate_date, with 'rate' and 'amount_pln' added.
    """
    tx_sorted = tx.sort_values('rate_date').reset_index(drop=True)
    rates_sorted = nbp_rates.rename(columns={'date': 'rate_date'}).sort_values('rate_date').reset_index(drop=True)
    merged = pd.merge_asof(tx_sorted, rates_sorted, on='rate_date', direction='backward')
    missing = merged['rate'].isna().sum()
    if missing:
        logging.error(f"{missing} transactions missing exchange rate.")
    merged['amount_pln'] = merged['amount_usd'] * merged['rate']
    return merged


def process_fifo(merged: pd.DataFrame) -> Tuple[float, float, float]:
    """Match stock sales to purchases using FIFO (First-In, First-Out) ordering.

    Buys ('YOU BOUGHT') and sells ('YOU SOLD') are each sorted by settlement
    date. Each sale consumes buy lots in chronological order, splitting across
    lots when a single buy lot doesn't cover the full sale quantity. Per-share
    cost is derived from the buy's amount_pln / shares (negative buy amounts
    are negated to get positive cost). Per-share proceeds come from the sale's
    amount_pln / shares.

    Handles mixed sources (RSU with zero cost, ESPP with actual cost) naturally
    since cost is derived from each lot's amount_pln.

    Args:
        merged: DataFrame with 'Transaction type', 'settlement_date', 'shares',
                and 'amount_pln' columns.

    Returns:
        Tuple of (total_proceeds, total_costs, total_gain) in PLN, where
        total_gain = total_proceeds - total_costs.
    """
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
            cost_per = (-lot['amount_pln']) / lot['shares'] if lot['shares'] else 0
            allocs.append({
                'proceeds': round(match * price_per, 2),
                'cost':     round(match * cost_per,   2)
            })
            buys.at[idx, 'remaining'] -= match
            qty -= match
    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs    = sum(a['cost']     for a in allocs)
    total_gain     = round(total_proceeds - total_costs, 2)
    logging.info(f"FIFO: matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def process_custom(merged: pd.DataFrame, custom_summary_path: str) -> Tuple[float, float, float]:
    """Match stock sales to specific lots using a Fidelity custom summary file.

    Reads a tab-separated summary file (e.g. stock-sales.txt) with columns:
    'Date sold or transferred', 'Date acquired', 'Quantity', 'Stock source',
    etc. Each row identifies a specific lot to match against the transaction
    history.

    Cost basis depends on the 'Stock source' column:
      - 'RS' (Restricted Stock / RSU): cost = 0 (vesting FMV already taxed
        as ordinary income, so Polish cost basis is zero).
      - 'SP' (ESPP): cost derived from the matching 'YOU BOUGHT ESPP###'
        transaction in the history, converted to PLN via the buy's rate.
      - Other sources: cost derived from the matching buy transaction.

    Sale and buy lookups try trade_date first, then fall back to
    settlement_date, to handle date ambiguity in Fidelity exports.

    Args:
        merged: Transaction DataFrame (output of merge_with_rates), must include
                'trade_date', 'settlement_date', 'Transaction type', 'shares',
                and 'amount_pln'.
        custom_summary_path: Path to the tab-separated custom summary TXT file.

    Returns:
        Tuple of (total_proceeds, total_costs, total_gain) in PLN.
    """
    # normalize dates for matching
    merged['trade_date_norm'] = merged['trade_date'].dt.normalize()
    merged['settlement_norm'] = merged['settlement_date'].dt.normalize()

    custom = pd.read_csv(custom_summary_path, sep='\t', engine='python')
    custom['Date sold']     = pd.to_datetime(custom['Date sold or transferred'], format='%b-%d-%Y', errors='coerce')
    custom['Date acquired'] = pd.to_datetime(custom['Date acquired'],              format='%b-%d-%Y', errors='coerce')
    custom['Quantity']      = pd.to_numeric(custom['Quantity'],                    errors='coerce')

    allocs = []
    for _, row in custom.iterrows():
        sale_date = row['Date sold'].normalize()
        acq_date  = row['Date acquired'].normalize()
        qty       = row['Quantity']
        source    = row.get('Stock source')

        # match sale by trade_date or settlement_date
        sale_tx = merged[
            (merged['trade_date_norm'] == sale_date) &
            merged['Transaction type'].str.contains('YOU SOLD', na=False)
        ]
        if sale_tx.empty:
            sale_tx = merged[
                (merged['settlement_norm'] == sale_date) &
                merged['Transaction type'].str.contains('YOU SOLD', na=False)
            ]
        if sale_tx.empty:
            logging.error(f"No sale record found for {sale_date}")
            continue
        sale = sale_tx.iloc[0]
        price_per = sale['amount_pln'] / abs(sale['shares']) if sale['shares'] else 0
        proceeds   = round(qty * price_per, 2)

        # determine cost
        if source == 'RS':
            cost = 0.0
        else:
            # ESPP: match buy by trade_date or settlement_date
            buy_tx = merged[
                (merged['trade_date_norm'] == acq_date) &
                merged['Transaction type'].str.contains('YOU BOUGHT', na=False)
            ]
            if source == 'SP':
                buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('ESPP', na=False)]
            if buy_tx.empty:
                buy_tx = merged[
                    (merged['settlement_norm'] == acq_date) &
                    merged['Transaction type'].str.contains('YOU BOUGHT', na=False)
                ]
                if source == 'SP':
                    buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('ESPP', na=False)]
            if buy_tx.empty:
                logging.error(f"No buy record found for {acq_date} (source={source})")
                continue
            buy      = buy_tx.iloc[0]
            cost_per = (-buy['amount_pln']) / buy['shares'] if buy['shares'] else 0
            cost     = round(qty * cost_per, 2)

        allocs.append({'proceeds': proceeds, 'cost': cost})

    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs    = sum(a['cost']     for a in allocs)
    total_gain     = round(total_proceeds - total_costs, 2)
    logging.info(f"Custom (by specific lots): matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def compute_dividends_and_tax(merged: pd.DataFrame) -> Tuple[float, float]:
    """Compute total dividend income and US withholding tax in PLN.

    Dividend income sums:
      - 'DIVIDEND RECEIVED' rows (gross cash dividends)
      - 'REINVESTMENT' rows (dividends reinvested into shares; typically
        negative amounts representing the reinvestment outflow)

    Foreign withholding tax sums (negated, since they appear as negative
    amounts):
      - 'NON-RESIDENT TAX DIVIDEND RECEIVED' (exact match)
      - Any row containing 'NON-RESIDENT TAX' (broader match)

    Both are reported on PIT-38/PIT-ZG: dividends feed into Poz. 22 (income),
    foreign tax into Poz. 32 (tax credit).

    Args:
        merged: Transaction DataFrame with 'Transaction type' and 'amount_pln'.

    Returns:
        Tuple of (total_dividends_pln, foreign_tax_pln).
    """
    gross_div = merged[merged['Transaction type'] == 'DIVIDEND RECEIVED']['amount_pln'].sum()
    reinv_div = merged[merged['Transaction type'].str.contains('REINVESTMENT', na=False)]['amount_pln'].sum()
    total_dividends = gross_div + reinv_div
    wd = -merged[merged['Transaction type'] == 'NON-RESIDENT TAX DIVIDEND RECEIVED']['amount_pln'].sum()
    wk = -merged[merged['Transaction type'].str.contains('NON-RESIDENT TAX', na=False)]['amount_pln'].sum()
    foreign_tax = round(wd + wk, 2)
    logging.info(f"Dividends PLN: {total_dividends:.2f}; Foreign tax PLN: {foreign_tax:.2f}")
    return total_dividends, foreign_tax


def load_transactions(tx_csv: str) -> pd.DataFrame:
    """Load and clean a Fidelity transaction history CSV.

    Parses the CSV, strips semicolons from transaction types, converts
    dates to timestamps, and parses share counts and dollar amounts.

    Args:
        tx_csv: Path to the Fidelity transaction history CSV file.

    Returns:
        DataFrame with added columns: 'trade_date', 'shares', 'amount_usd'.
    """
    tx_raw = pd.read_csv(tx_csv)
    tx = tx_raw.copy()
    tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
    tx['trade_date']       = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
    tx['shares']           = pd.to_numeric(tx['Shares'], errors='coerce')
    tx['amount_usd']       = pd.to_numeric(tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')
    return tx


def calculate_pit38_fields(
    total_proceeds: float,
    total_costs: float,
    total_gain: float,
    total_dividends: float,
    foreign_tax: float,
) -> Dict[str, float]:
    """Compute PIT-38 and PIT-ZG field values from aggregated totals.

    Args:
        total_proceeds: Total sale proceeds in PLN.
        total_costs: Total cost basis in PLN.
        total_gain: Net gain from stock sales in PLN (proceeds - costs).
        total_dividends: Total dividend income in PLN.
        foreign_tax: Total foreign withholding tax in PLN.

    Returns:
        Dict with keys: poz22, poz23, poz26, poz29, poz30_rate, poz31,
        poz32, tax_final, pitzg_poz29, pitzg_poz30.
    """
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

    return {
        'poz22': poz22,
        'poz23': poz23,
        'poz26': poz26,
        'poz29': poz29,
        'poz30_rate': poz30_rate,
        'poz31': poz31,
        'poz32': poz32,
        'tax_final': tax_final,
        'pitzg_poz29': pitzg_poz29,
        'pitzg_poz30': pitzg_poz30,
    }


def calculate_pit38(
    tx_csv: str,
    year: int = 2024,
    method: str = 'fifo',
    custom_summary: Optional[str] = None,
) -> Dict[str, float]:
    """Run the full PIT-38 calculation pipeline.

    Args:
        tx_csv: Path to the Fidelity transaction history CSV file.
        year: Tax year to process.
        method: 'fifo' or 'custom' lot matching method.
        custom_summary: Path to custom summary TXT file (required when method='custom').

    Returns:
        Dict with PIT-38 and PIT-ZG fields plus 'year'.
    """
    NBP_RATE_URLS = [
        "https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_2024.csv",
    ]
    nbp_rates = load_nbp_rates(NBP_RATE_URLS)

    tx = load_transactions(tx_csv)
    tx['settlement_date'] = calculate_settlement_dates(tx['trade_date'], tx['Transaction type'])
    tx = tx[tx['settlement_date'].dt.year == year]
    tx['rate_date'] = calculate_rate_dates(tx['settlement_date'])
    merged = merge_with_rates(tx, nbp_rates)

    if method == 'fifo':
        total_proceeds, total_costs, total_gain = process_fifo(merged)
    else:
        if not custom_summary:
            raise ValueError("custom_summary is required when method='custom'")
        total_proceeds, total_costs, total_gain = process_custom(merged, custom_summary)

    total_dividends, foreign_tax = compute_dividends_and_tax(merged)

    result = calculate_pit38_fields(
        total_proceeds, total_costs, total_gain, total_dividends, foreign_tax,
    )
    result['year'] = year
    return result
