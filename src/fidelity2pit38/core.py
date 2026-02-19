#!/usr/bin/env python3
# DISCLAIMER: This script is provided "as is" for informational purposes only.
# I am not a certified accountant or tax advisor; consult a professional for personalized guidance.

import io
import logging
import ssl
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import certifi
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland

# constant for switch from T+2 to T+1
SWITCH_DATE = pd.Timestamp('2024-05-28')


def discover_transaction_files(directory: str) -> Tuple[List[str], List[str]]:
    """Glob for transaction history CSVs and stock-sales TXTs in a directory.

    Args:
        directory: Path to the directory to scan.

    Returns:
        Tuple of (csv_paths, txt_paths), each sorted alphabetically.
    """
    d = Path(directory)
    csv_paths = sorted(str(p) for p in d.glob("Transaction history*.csv"))
    txt_paths = sorted(str(p) for p in d.glob("stock-sales*.txt"))
    logging.info(f"Discovered {len(csv_paths)} CSV(s) and {len(txt_paths)} TXT(s) in {directory}")
    for p in csv_paths:
        logging.info(f"  CSV: {p}")
    for p in txt_paths:
        logging.info(f"  TXT: {p}")
    return csv_paths, txt_paths


def build_nbp_rate_urls(years: List[int]) -> List[str]:
    """Build NBP archive URLs covering the given years plus one year before.

    Rates from the year before the earliest are needed because transactions
    near January 1 may require a rate from the previous year.

    Args:
        years: List of years present in the transaction data.

    Returns:
        List of NBP CSV archive URLs.
    """
    if not years:
        return []
    all_years = range(min(years) - 1, max(years) + 1)
    urls = [
        f"https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_{y}.csv"
        for y in all_years
    ]
    logging.info(f"NBP rate URLs for years {list(all_years)}: {len(urls)} files")
    return urls


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
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    rates_list = []
    for url in urls:
        with urllib.request.urlopen(url, context=ssl_ctx) as resp:
            raw = resp.read().decode('cp1250')
        df = pd.read_csv(io.StringIO(raw), sep=';', header=0, dtype=str)
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


def process_fifo(merged: pd.DataFrame, year: Optional[int] = None) -> Tuple[float, float, float]:
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
        year: If set, only sells settling in this year are matched.
              All buys are kept so cross-year FIFO works correctly.

    Returns:
        Tuple of (total_proceeds, total_costs, total_gain) in PLN, where
        total_gain = total_proceeds - total_costs.
    """
    buys = merged[merged['Transaction type'].str.contains('YOU BOUGHT', na=False)].sort_values('settlement_date').copy()
    sells = merged[merged['Transaction type'].str.contains('YOU SOLD', na=False)].sort_values('settlement_date').copy()
    if year is not None:
        sells = sells[sells['settlement_date'].dt.year == year]
    buys['remaining'] = buys['shares']
    allocs = []
    for _, sale in sells.iterrows():
        qty = abs(sale['shares'])
        price_per = sale['amount_pln'] / qty if qty else 0
        available_qty = buys[buys['remaining'] > 0]['remaining'].sum()
        if qty > available_qty:
            logging.error(
                f"FIFO inconsistency: attempting to sell {qty:.4f} shares on "
                f"{sale['settlement_date'].date()}, but only {available_qty:.4f} shares remain in buy lots."
            )
        while qty > 0:
            open_lots = buys[buys['remaining'] > 0]
            if open_lots.empty:
                logging.error(
                    f"FIFO inconsistency: no remaining buy lots for sale on "
                    f"{sale['settlement_date'].date()}; unmatched quantity {qty:.4f} shares."
                )
                break
            idx = open_lots.index.min()
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


def process_custom(merged: pd.DataFrame, custom_summary_path: Union[str, List[str]], year: Optional[int] = None) -> Tuple[float, float, float]:
    """Match stock sales to specific lots using a Fidelity custom summary file.

    Reads one or more tab-separated summary files (e.g. stock-sales.txt) with
    columns: 'Date sold or transferred', 'Date acquired', 'Quantity',
    'Stock source', etc. Each row identifies a specific lot to match against
    the transaction history.

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
        custom_summary_path: Path (or list of paths) to tab-separated custom
                summary TXT file(s).
        year: If set, only rows whose sale date falls in this year are matched.

    Returns:
        Tuple of (total_proceeds, total_costs, total_gain) in PLN.
    """
    # normalize dates for matching
    merged['trade_date_norm'] = merged['trade_date'].dt.normalize()
    merged['settlement_norm'] = merged['settlement_date'].dt.normalize()

    paths = [custom_summary_path] if isinstance(custom_summary_path, str) else custom_summary_path
    custom_frames = [pd.read_csv(p, sep='\t', engine='python') for p in paths]
    custom = pd.concat(custom_frames, ignore_index=True).drop_duplicates()
    custom['Date sold']     = pd.to_datetime(custom['Date sold or transferred'], format='%b-%d-%Y', errors='coerce')
    custom['Date acquired'] = pd.to_datetime(custom['Date acquired'],              format='%b-%d-%Y', errors='coerce')
    custom['Quantity']      = pd.to_numeric(custom['Quantity'],                    errors='coerce')
    if year is not None:
        custom = custom[custom['Date sold'].dt.year == year]

    invalid_rows = custom[
        custom['Date sold'].isna() |
        custom['Date acquired'].isna() |
        custom['Quantity'].isna() |
        (custom['Quantity'] <= 0)
    ]
    if len(invalid_rows):
        logging.error(
            f"Custom summary inconsistency: {len(invalid_rows)} row(s) have invalid "
            "Date sold/Date acquired/Quantity and will be skipped."
        )

    # Validate sale-date quantities from custom summary against transaction history.
    sells = merged[merged['Transaction type'].str.contains('YOU SOLD', na=False)].copy()
    if year is not None:
        sells = sells[sells['settlement_date'].dt.year == year]
    trade_sale_qty = sells.groupby(sells['trade_date_norm'])['shares'].sum().abs()
    settle_sale_qty = sells.groupby(sells['settlement_norm'])['shares'].sum().abs()
    custom_valid_sale = custom.dropna(subset=['Date sold', 'Quantity']).copy()
    custom_valid_sale['Date sold norm'] = custom_valid_sale['Date sold'].dt.normalize()
    custom_sale_qty = custom_valid_sale.groupby('Date sold norm')['Quantity'].sum()
    for sale_date, custom_qty in custom_sale_qty.items():
        trade_qty = float(trade_sale_qty.get(sale_date, 0.0))
        settle_qty = float(settle_sale_qty.get(sale_date, 0.0))
        available_qty = trade_qty if trade_qty > 0 else settle_qty
        if available_qty == 0:
            logging.error(
                f"Custom summary inconsistency: no YOU SOLD transaction found for sale date {sale_date.date()}."
            )
        elif custom_qty > available_qty:
            logging.error(
                f"Custom summary inconsistency: sale-date quantity {custom_qty:.4f} on {sale_date.date()} "
                f"exceeds available sold quantity {available_qty:.4f}."
            )

    # Validate acquired quantities by source/date against available buy lots.
    buys = merged[merged['Transaction type'].str.contains('YOU BOUGHT', na=False)].copy()
    custom_valid_acq = custom.dropna(subset=['Date acquired', 'Quantity', 'Stock source']).copy()
    custom_valid_acq['Date acquired norm'] = custom_valid_acq['Date acquired'].dt.normalize()
    for (acq_date, source), group in custom_valid_acq.groupby(['Date acquired norm', 'Stock source']):
        needed_qty = float(group['Quantity'].sum())
        candidate = buys
        if source == 'SP':
            candidate = candidate[candidate['Transaction type'].str.contains('ESPP', na=False)]
        elif source == 'RS':
            candidate = candidate[candidate['Transaction type'].str.contains('RSU', na=False)]
        trade_qty = float(candidate[candidate['trade_date_norm'] == acq_date]['shares'].sum())
        settle_qty = float(candidate[candidate['settlement_norm'] == acq_date]['shares'].sum())
        available_qty = trade_qty if trade_qty > 0 else settle_qty
        if available_qty == 0:
            logging.error(
                f"Custom summary inconsistency: no matching buy lot for Date acquired={acq_date.date()} "
                f"and Stock source={source}."
            )
        elif needed_qty > available_qty:
            logging.error(
                f"Custom summary inconsistency: acquired quantity {needed_qty:.4f} for "
                f"Date acquired={acq_date.date()}, source={source} exceeds available buy quantity "
                f"{available_qty:.4f}."
            )

    allocs = []
    for _, row in custom.iterrows():
        sale_date = row['Date sold'].normalize()
        acq_date  = row['Date acquired'].normalize()
        qty       = row['Quantity']
        source    = row.get('Stock source')

        if pd.isna(sale_date) or pd.isna(acq_date) or pd.isna(qty) or qty <= 0:
            continue

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
        if len(sale_tx) > 1:
            logging.error(
                f"Custom summary ambiguity: {len(sale_tx)} sale rows match {sale_date.date()}; using the first one."
            )
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
            if len(buy_tx) > 1:
                logging.error(
                    f"Custom summary ambiguity: {len(buy_tx)} buy rows match {acq_date.date()} "
                    f"(source={source}); using the first one."
                )
            buy      = buy_tx.iloc[0]
            cost_per = (-buy['amount_pln']) / buy['shares'] if buy['shares'] else 0
            cost     = round(qty * cost_per, 2)

        allocs.append({'proceeds': proceeds, 'cost': cost})

    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs    = sum(a['cost']     for a in allocs)
    total_gain     = round(total_proceeds - total_costs, 2)
    logging.info(f"Custom (by specific lots): matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def compute_dividends_and_tax(merged: pd.DataFrame, year: Optional[int] = None) -> Tuple[float, float]:
    """Compute total dividend income and US withholding tax in PLN.

    Dividend income sums:
      - 'DIVIDEND RECEIVED' rows (gross cash dividends)
      - 'REINVESTMENT' rows (dividends reinvested into shares; typically
        negative amounts representing the reinvestment outflow)

    Foreign withholding tax: all rows containing 'NON-RESIDENT TAX' in the
    transaction type. Amounts are negative in the source data and negated here
    to report as a positive value.

    Dividends are taxed separately under art. 30a (Section G of PIT-38).
    The foreign withholding tax on dividends feeds into Poz. 46 of Section G.

    Args:
        merged: Transaction DataFrame with 'Transaction type', 'amount_pln',
                and 'settlement_date'.
        year: If set, only rows settling in this year are included.

    Returns:
        Tuple of (total_dividends_pln, foreign_tax_on_dividends_pln).
    """
    df = merged
    if year is not None:
        df = merged[merged['settlement_date'].dt.year == year]
    gross_div = df[df['Transaction type'] == 'DIVIDEND RECEIVED']['amount_pln'].sum()
    reinv_div = df[df['Transaction type'].str.contains('REINVESTMENT', na=False)]['amount_pln'].sum()
    total_dividends = gross_div + reinv_div
    # All NON-RESIDENT TAX rows (no double-counting)
    foreign_tax = -df[df['Transaction type'].str.contains('NON-RESIDENT TAX', na=False)]['amount_pln'].sum()
    foreign_tax = round(foreign_tax, 2) + 0.0  # +0.0 avoids -0.00 display
    logging.info(f"Dividends PLN: {total_dividends:.2f}; Foreign tax on dividends PLN: {foreign_tax:.2f}")
    return total_dividends, foreign_tax


def load_transactions(tx_csv: Union[str, List[str]]) -> pd.DataFrame:
    """Load and clean one or more Fidelity transaction history CSVs.

    Parses each CSV, concatenates them, deduplicates, strips semicolons
    from transaction types, converts dates to timestamps, and parses
    share counts and dollar amounts.

    Args:
        tx_csv: Path (or list of paths) to Fidelity transaction history CSV file(s).

    Returns:
        DataFrame with added columns: 'trade_date', 'shares', 'amount_usd'.
    """
    paths = [tx_csv] if isinstance(tx_csv, str) else tx_csv
    frames = [pd.read_csv(p) for p in paths]
    tx_raw = pd.concat(frames, ignore_index=True).drop_duplicates()
    tx = tx_raw.copy()
    # Fidelity CSV exports may include footer text rows that are not transactions.
    footer_mask = (
        tx['Transaction type'].isna() &
        tx['Investment name'].isna() &
        tx['Shares'].isna() &
        tx['Amount'].isna() &
        tx['Transaction date'].astype(str).str.contains(
            r"Unless noted otherwise|Stock plan account history as of",
            case=False,
            na=False,
        )
    )
    if footer_mask.any():
        # Silently drop known Fidelity footer rows.
        tx = tx.loc[~footer_mask].copy()
    tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
    tx['trade_date']       = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
    tx['shares']           = pd.to_numeric(tx['Shares'], errors='coerce')
    tx['amount_usd']       = pd.to_numeric(tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')
    invalid_trade_dates = tx['trade_date'].isna()
    if invalid_trade_dates.any():
        malformed_rows = tx[invalid_trade_dates & tx[['Transaction type', 'Investment name', 'Shares', 'Amount']].notna().any(axis=1)]
        if len(malformed_rows):
            logging.error(
                f"Data inconsistency: {len(malformed_rows)} row(s) have invalid 'Transaction date' "
                "with non-empty transaction fields."
            )
        ignored_blank_date_rows = int((invalid_trade_dates & ~tx[['Transaction type', 'Investment name', 'Shares', 'Amount']].notna().any(axis=1)).sum())
        if ignored_blank_date_rows:
            logging.info(
                f"Ignoring {ignored_blank_date_rows} row(s) with empty transaction fields and invalid date."
            )
    market_mask = tx['Transaction type'].str.contains('YOU BOUGHT|YOU SOLD', na=False)
    missing_market_shares = tx[market_mask]['shares'].isna().sum()
    if missing_market_shares:
        logging.error(
            f"Data inconsistency: {missing_market_shares} market-trade row(s) have missing/invalid 'Shares'."
        )
    missing_market_amount = tx[market_mask]['amount_usd'].isna().sum()
    if missing_market_amount:
        logging.error(
            f"Data inconsistency: {missing_market_amount} market-trade row(s) have missing/invalid 'Amount'."
        )
    logging.info(f"Loaded {len(tx)} transactions from {len(paths)} file(s).")
    return tx


def _round_tax(value: float) -> int:
    """Round to full PLN per Ordynacja Podatkowa art. 63 §1.

    Fractional amounts below 50 groszy are dropped; 50 groszy and above
    are rounded up to the next full zloty.

    Examples: 1234.49 -> 1234, 1234.50 -> 1235, 1234.99 -> 1235, 0.0 -> 0
    """
    import math
    if value < 0:
        return 0
    return int(math.floor(value + 0.5))


def calculate_pit38_fields(
    total_proceeds: float,
    total_costs: float,
    total_gain: float,
    total_dividends: float,
    foreign_tax_dividends: float,
) -> Dict[str, float]:
    """Compute PIT-38 and PIT-ZG field values from aggregated totals.

    Section C/D (art. 30b) — capital gains from stock sales:
      Poz. 22 = total proceeds (stock sales only, no dividends)
      Poz. 23 = total costs
      Poz. 26 = Poz. 22 - Poz. 23 (income)
      Poz. 29 = tax base, rounded per Ordynacja art. 63 §1
      Poz. 30 = 19% rate
      Poz. 31 = Poz. 29 × 19%
      Poz. 32 = foreign tax paid on capital gains (0 for Fidelity US stocks;
                US doesn't withhold on stock sale proceeds)
      Poz. 33 = max(Poz. 31 - Poz. 32, 0), rounded per art. 63

    Section G (art. 30a) — dividends:
      Poz. 45 = 19% tax on gross dividends (rounded to grosze up, per art.63 §1a)
      Poz. 46 = foreign withholding tax on dividends
      Poz. 47 = max(Poz. 45 - Poz. 46, 0) (rounded per art. 63)

    PIT-ZG attachment — foreign income:
      pitzg_poz29 = capital gains from foreign sources
      pitzg_poz30 = foreign tax on capital gains (0 for US stocks)

    Args:
        total_proceeds: Total sale proceeds in PLN.
        total_costs: Total cost basis in PLN.
        total_gain: Net gain from stock sales in PLN (proceeds - costs).
        total_dividends: Total gross dividend income in PLN.
        foreign_tax_dividends: Foreign withholding tax on dividends in PLN.

    Returns:
        Dict with PIT-38 Section C/D, Section G, and PIT-ZG fields.
    """
    # --- Section C/D: capital gains (art. 30b) ---
    poz22 = round(total_proceeds, 2)
    poz23 = round(total_costs, 2)
    poz26 = round(poz22 - poz23, 2)
    poz29 = _round_tax(poz26)           # tax base
    poz30_rate = 0.19
    poz31 = round(poz29 * poz30_rate, 2)
    # US does not withhold tax on stock sale proceeds; foreign tax on
    # capital gains is 0 for Fidelity accounts.
    poz32 = 0.0
    tax_final = _round_tax(max(poz31 - poz32, 0))  # Poz. 33

    # --- Section G: dividends (art. 30a) ---
    import math
    poz45 = math.ceil(round(total_dividends * poz30_rate, 2) * 100) / 100  # 19% rounded up to grosze
    poz45 = round(poz45, 2) + 0.0
    poz46 = round(min(foreign_tax_dividends, poz45), 2)  # credit capped at Polish tax
    poz47 = _round_tax(max(poz45 - poz46, 0))

    # --- PIT-ZG: foreign income ---
    pitzg_poz29 = total_gain
    pitzg_poz30 = poz32  # foreign tax on capital gains (not dividends)

    return {
        'poz22': poz22,
        'poz23': poz23,
        'poz26': poz26,
        'poz29': poz29,
        'poz30_rate': poz30_rate,
        'poz31': poz31,
        'poz32': poz32,
        'tax_final': tax_final,
        'poz45': poz45,
        'poz46': poz46,
        'poz47': poz47,
        'pitzg_poz29': pitzg_poz29,
        'pitzg_poz30': pitzg_poz30,
    }


def calculate_pit38(
    tx_csv: Union[str, List[str]],
    year: int = 2024,
    method: str = 'fifo',
    custom_summary: Union[str, List[str], None] = None,
) -> Dict[str, float]:
    """Run the full PIT-38 calculation pipeline.

    Args:
        tx_csv: Path (or list of paths) to Fidelity transaction history CSV file(s).
        year: Tax year to process.
        method: 'fifo' or 'custom' lot matching method.
        custom_summary: Path (or list of paths) to custom summary TXT file(s)
                (required when method='custom').

    Returns:
        Dict with PIT-38 and PIT-ZG fields plus 'year'.
    """
    tx = load_transactions(tx_csv)
    tx['settlement_date'] = calculate_settlement_dates(tx['trade_date'], tx['Transaction type'])
    tx = tx.dropna(subset=['settlement_date'])

    # Build NBP rate URLs dynamically from the years present in the data
    data_years = sorted(int(y) for y in tx['settlement_date'].dt.year.unique())
    nbp_urls = build_nbp_rate_urls(data_years)
    nbp_rates = load_nbp_rates(nbp_urls)

    if year not in data_years:
        logging.warning(
            f"Target year {year} not found in transaction data. "
            f"Data contains years: {data_years}. "
            f"Use --year to specify the correct tax year."
        )

    tx['rate_date'] = calculate_rate_dates(tx['settlement_date'])
    merged = merge_with_rates(tx, nbp_rates)

    if method == 'fifo':
        total_proceeds, total_costs, total_gain = process_fifo(merged, year=year)
    else:
        if not custom_summary:
            raise ValueError("custom_summary is required when method='custom'")
        total_proceeds, total_costs, total_gain = process_custom(merged, custom_summary, year=year)

    total_dividends, foreign_tax = compute_dividends_and_tax(merged, year=year)

    result = calculate_pit38_fields(
        total_proceeds, total_costs, total_gain, total_dividends, foreign_tax,
    )
    result['year'] = year
    return result
