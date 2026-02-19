#!/usr/bin/env python3
# DISCLAIMER: This script is provided "as is" for informational purposes only.
# I am not a certified accountant or tax advisor; consult a professional for personalized guidance.

import io
import logging
import re
import ssl
import urllib.request
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import certifi
import pandas as pd
from pandas.tseries.holiday import AbstractHolidayCalendar, GoodFriday, USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from workalendar.europe import Poland

from .validation import (
    check_custom_acquired_quantities,
    check_custom_buy_match_unambiguous,
    check_custom_buy_record_exists,
    check_custom_sale_date_quantities,
    check_custom_sale_match_unambiguous,
    check_custom_sale_record_exists,
    check_custom_summary_rows_valid,
    check_exchange_rates_present,
    check_fifo_open_lots_available,
    check_fifo_sale_not_oversell,
    check_no_cross_file_duplicates,
    check_transaction_data_consistency,
    strip_known_fidelity_footer_rows,
)

# constant for switch from T+2 to T+1
SWITCH_DATE = pd.Timestamp('2024-05-28')


class USSettlementHolidayCalendar(AbstractHolidayCalendar):
    """US settlement calendar: federal holidays plus Good Friday."""
    rules = list(USFederalHolidayCalendar.rules) + [GoodFriday]


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
    us_bd1 = CustomBusinessDay(calendar=USSettlementHolidayCalendar(), n=1)
    us_bd2 = CustomBusinessDay(calendar=USSettlementHolidayCalendar(), n=2)

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
    pl_calendar = Poland()
    rate_dates: List[Optional[pd.Timestamp]] = []
    for d in settlement_dates:
        if pd.isna(d):
            rate_dates.append(pd.NaT)
            continue
        prev_pl_workday = pl_calendar.add_working_days(pd.Timestamp(d).date(), -1)
        rate_dates.append(pd.Timestamp(prev_pl_workday))
    return pd.Series(rate_dates, index=settlement_dates.index)


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
    check_exchange_rates_present(int(missing))
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
        sale_investment = sale.get('Investment name') if 'Investment name' in sale.index else None

        def _open_lots_for_sale() -> pd.DataFrame:
            open_lots = buys[buys['remaining'] > 0]
            if 'Investment name' in open_lots.columns and pd.notna(sale_investment):
                open_lots = open_lots[open_lots['Investment name'] == sale_investment]
            return open_lots

        qty = abs(sale['shares'])
        price_per = sale['amount_pln'] / qty if qty else 0
        available_qty = _open_lots_for_sale()['remaining'].sum()
        check_fifo_sale_not_oversell(sale['settlement_date'], qty, available_qty)
        while qty > 0:
            open_lots = _open_lots_for_sale()
            if not check_fifo_open_lots_available(sale['settlement_date'], qty, has_open_lots=not open_lots.empty):
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

    Cost basis logic:
      - Preferred: if the custom file provides parseable 'Cost basis', this
        value is treated as the lot USD basis and converted to PLN using the
        matched acquisition transaction's exchange rate.
      - Fallback (when 'Cost basis' is missing/unparseable):
        - 'RS' (Restricted Stock / RSU): cost = 0.0.
        - 'SP' (ESPP): cost derived from matching 'YOU BOUGHT ESPP###' rows.
        - Other sources: cost derived from matching buy transactions.

    Sale and buy lookups try trade_date first, then fall back to
    settlement_date, to handle date ambiguity in Fidelity exports.

    Args:
        merged: Transaction DataFrame (output of merge_with_rates), must include
                'trade_date', 'settlement_date', 'Transaction type', 'shares',
                and 'amount_pln'.
        custom_summary_path: Path (or list of paths) to tab-separated custom
                summary TXT file(s).
        year: If set, only rows matched to sales settling in this year are included.

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
    if 'Cost basis' in custom.columns:
        cost_basis_raw = custom['Cost basis'].astype(str).str.strip()
        paren_negative = cost_basis_raw.str.match(r'^\(.*\)$', na=False)
        cost_basis_clean = cost_basis_raw.str.replace(r'[\s$,()]', '', regex=True)
        custom['Cost basis USD'] = pd.to_numeric(cost_basis_clean, errors='coerce')
        custom.loc[paren_negative, 'Cost basis USD'] = (
            -custom.loc[paren_negative, 'Cost basis USD'].abs()
        )
    else:
        custom['Cost basis USD'] = pd.NA

    custom['Date sold norm'] = custom['Date sold'].dt.normalize()
    if year is not None:
        sells_in_year = merged[
            merged['Transaction type'].str.contains('YOU SOLD', na=False) &
            (merged['settlement_date'].dt.year == year)
        ]
        allowed_sale_dates = pd.Index(sells_in_year['trade_date_norm'].dropna().unique()).union(
            pd.Index(sells_in_year['settlement_norm'].dropna().unique())
        )
        custom = custom[custom['Date sold'].isna() | custom['Date sold norm'].isin(allowed_sale_dates)]

    check_custom_summary_rows_valid(custom)
    check_custom_sale_date_quantities(custom, merged, year=year)
    check_custom_acquired_quantities(custom, merged)

    allocs = []
    for _, row in custom.iterrows():
        sale_date = row['Date sold norm']
        acq_date  = row['Date acquired'].normalize()
        qty       = row['Quantity']
        source    = row.get('Stock source')
        reported_cost_basis_usd = row.get('Cost basis USD')
        custom_symbol = None
        for symbol_col in ('Symbol', 'Ticker', 'Security Symbol'):
            candidate = row.get(symbol_col)
            if pd.notna(candidate):
                txt = str(candidate).strip()
                if txt and txt != '-':
                    custom_symbol = txt
                    break
        custom_investment_name = row.get('Investment name')
        if pd.notna(custom_investment_name):
            custom_investment_name = str(custom_investment_name).strip()
            if not custom_investment_name or custom_investment_name == '-':
                custom_investment_name = None

        if pd.isna(sale_date) or pd.isna(acq_date) or pd.isna(qty) or qty <= 0:
            continue

        def _apply_custom_identifier_filter(df: pd.DataFrame, label: str, date_value: pd.Timestamp) -> pd.DataFrame:
            if df.empty:
                return df

            if custom_symbol:
                symbol_upper = custom_symbol.upper()
                if 'Symbol' in df.columns:
                    symbol_col = df['Symbol'].astype(str).str.strip().str.upper()
                    exact_mask = symbol_col == symbol_upper
                    if exact_mask.any():
                        return df[exact_mask]

                if 'Investment name' in df.columns:
                    investment_col = df['Investment name'].astype(str).str.strip()
                    exact_name_mask = investment_col.str.upper() == symbol_upper
                    if exact_name_mask.any():
                        return df[exact_name_mask]
                    token_mask = investment_col.str.contains(
                        rf"\b{re.escape(custom_symbol)}\b", case=False, regex=True, na=False
                    )
                    if token_mask.any():
                        return df[token_mask]

                logging.error(
                    "Custom summary inconsistency: Symbol '%s' could not be matched to %s rows on %s.",
                    custom_symbol,
                    label,
                    date_value.date(),
                )
                return df.iloc[0:0]

            if custom_investment_name and 'Investment name' in df.columns:
                investment_col = df['Investment name'].astype(str).str.strip()
                name_mask = investment_col.str.upper() == custom_investment_name.upper()
                if name_mask.any():
                    return df[name_mask]
            return df

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
        sale_tx = _apply_custom_identifier_filter(sale_tx, label='sale', date_value=sale_date)
        if not check_custom_sale_record_exists(sale_tx, sale_date):
            continue
        check_custom_sale_match_unambiguous(sale_date, len(sale_tx))
        sale = sale_tx.iloc[0]
        sale_investment = sale.get('Investment name') if 'Investment name' in sale.index else None
        price_per = sale['amount_pln'] / abs(sale['shares']) if sale['shares'] else 0
        proceeds   = round(qty * price_per, 2)

        # determine cost
        buy = None
        need_buy_lookup = pd.notna(reported_cost_basis_usd) or source != 'RS'
        if need_buy_lookup:
            # ESPP: match buy by trade_date or settlement_date
            buy_tx = merged[
                (merged['trade_date_norm'] == acq_date) &
                merged['Transaction type'].str.contains('YOU BOUGHT', na=False)
            ]
            if pd.notna(sale_investment) and 'Investment name' in merged.columns:
                buy_tx = buy_tx[buy_tx['Investment name'] == sale_investment]
            if source == 'SP':
                buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('ESPP', na=False)]
            if buy_tx.empty:
                buy_tx = merged[
                    (merged['settlement_norm'] == acq_date) &
                    merged['Transaction type'].str.contains('YOU BOUGHT', na=False)
                ]
                if pd.notna(sale_investment) and 'Investment name' in merged.columns:
                    buy_tx = buy_tx[buy_tx['Investment name'] == sale_investment]
                if source == 'SP':
                    buy_tx = buy_tx[buy_tx['Transaction type'].str.contains('ESPP', na=False)]
            if not check_custom_buy_record_exists(buy_tx, acq_date, source):
                continue
            check_custom_buy_match_unambiguous(acq_date, source, len(buy_tx))
            buy = buy_tx.iloc[0]

        if pd.notna(reported_cost_basis_usd):
            if reported_cost_basis_usd < 0:
                logging.error(
                    "Custom summary inconsistency: negative Cost basis %.2f USD for sale date %s.",
                    float(reported_cost_basis_usd),
                    sale_date.date(),
                )
                continue
            if buy is None or pd.isna(buy.get('rate')):
                logging.error(
                    "Custom summary inconsistency: cannot convert Cost basis to PLN "
                    "for sale date %s due to missing acquisition-rate match.",
                    sale_date.date(),
                )
                continue
            cost = round(float(reported_cost_basis_usd) * float(buy['rate']), 2)
        elif source == 'RS':
            logging.warning(
                "Custom summary fallback: missing/invalid Cost basis for RS lot sold on %s; using 0.0 PLN cost.",
                sale_date.date(),
            )
            cost = 0.0
        else:
            if source == 'SP':
                logging.warning(
                    "Custom summary fallback: missing/invalid Cost basis for SP lot sold on %s; "
                    "deriving cost from matching ESPP buy.",
                    sale_date.date(),
                )
            cost_per = (-buy['amount_pln']) / buy['shares'] if buy['shares'] else 0
            cost     = round(qty * cost_per, 2)

        allocs.append({'proceeds': proceeds, 'cost': cost})

    total_proceeds = sum(a['proceeds'] for a in allocs)
    total_costs    = sum(a['cost']     for a in allocs)
    total_gain     = round(total_proceeds - total_costs, 2)
    logging.info(f"Custom (by specific lots): matched {len(allocs)} lots; Gain PLN: {total_gain:.2f}")
    return total_proceeds, total_costs, total_gain


def _is_fund_like_investment(investment_name: object) -> bool:
    """Heuristic classifier for fund/cash-sweep positions vs equity names."""
    if investment_name is None or pd.isna(investment_name):
        return False
    name = str(investment_name).upper()
    fund_markers = ("FUND", "MMKT", "MONEY MARKET", "CASH RESERVES")
    return any(marker in name for marker in fund_markers)


def compute_section_g_income_components(merged: pd.DataFrame, year: Optional[int] = None) -> Dict[str, float]:
    """Compute Section G (art. 30a ust.1 pkt 1-5) income components in PLN.

    For Fidelity exports used by this project, Section G income is sourced from
    'DIVIDEND RECEIVED' rows. These are split into:
      - equity-like dividends
      - fund/cash-sweep distributions (e.g. money market funds)

    Reinvestment rows are not income tax base rows.
    """
    df = merged
    if year is not None:
        df = merged[merged['settlement_date'].dt.year == year]

    div_rows = df[df['Transaction type'] == 'DIVIDEND RECEIVED'].copy()
    if 'Investment name' in div_rows.columns:
        fund_mask = div_rows['Investment name'].apply(_is_fund_like_investment)
    else:
        fund_mask = pd.Series(False, index=div_rows.index)

    fund_distributions = round(div_rows.loc[fund_mask, 'amount_pln'].sum(), 2) + 0.0
    equity_dividends = round(div_rows.loc[~fund_mask, 'amount_pln'].sum(), 2) + 0.0
    total_income = round(equity_dividends + fund_distributions, 2) + 0.0

    foreign_tax_mask = (
        df['Transaction type'].str.contains('NON-RESIDENT TAX', na=False) &
        df['Transaction type'].str.contains('DIVIDEND', na=False)
    )
    foreign_tax = -df[foreign_tax_mask]['amount_pln'].sum()
    foreign_tax = round(foreign_tax, 2) + 0.0  # +0.0 avoids -0.00 display

    return {
        'section_g_total_income': total_income,
        'section_g_equity_dividends': equity_dividends,
        'section_g_fund_distributions': fund_distributions,
        'section_g_foreign_tax': foreign_tax,
    }


def compute_dividends_and_tax(merged: pd.DataFrame, year: Optional[int] = None) -> Tuple[float, float]:
    """Compute Section G income (legacy name) and withholding tax in PLN.

    Backward-compatible wrapper over `compute_section_g_income_components`.
    The first value contains Section G gross income base used for Poz. 45.

    Args:
        merged: Transaction DataFrame with 'Transaction type', 'amount_pln',
                and 'settlement_date'.
        year: If set, only rows settling in this year are included.

    Returns:
        Tuple of (section_g_income_pln, foreign_tax_section_g_pln).
    """
    components = compute_section_g_income_components(merged, year=year)
    logging.info(
        "Section G income PLN: %.2f (equity dividends: %.2f; fund distributions: %.2f); "
        "Foreign tax PLN: %.2f",
        components['section_g_total_income'],
        components['section_g_equity_dividends'],
        components['section_g_fund_distributions'],
        components['section_g_foreign_tax'],
    )
    return components['section_g_total_income'], components['section_g_foreign_tax']


def compute_foreign_tax_capital_gains(merged: pd.DataFrame, year: Optional[int] = None) -> float:
    """Compute foreign tax attributable to capital gains (art. 30b) in PLN.

    Matches rows marked as foreign tax that are not dividend-related.
    """
    df = merged
    if year is not None:
        df = merged[merged['settlement_date'].dt.year == year]
    foreign_tax_mask = df['Transaction type'].str.contains('NON-RESIDENT TAX', na=False)
    dividend_context_mask = df['Transaction type'].str.contains('DIVIDEND|REINVESTMENT', na=False)
    capital_tax = -df[foreign_tax_mask & ~dividend_context_mask]['amount_pln'].sum()
    capital_tax = round(max(capital_tax, 0.0), 2) + 0.0
    return capital_tax


def load_transactions(tx_csv: Union[str, List[str]]) -> pd.DataFrame:
    """Load and clean one or more Fidelity transaction history CSVs.

    Parses each CSV, concatenates them, strips semicolons from transaction
    types, converts dates to timestamps, and parses share counts and dollar
    amounts.

    Technical footer rows emitted by Fidelity exports are removed silently.
    If identical transaction rows are found across different CSV files,
    a ValueError is raised (no automatic deduplication).

    Args:
        tx_csv: Path (or list of paths) to Fidelity transaction history CSV file(s).

    Returns:
        DataFrame with added columns: 'trade_date', 'shares', 'amount_usd'.
    """
    paths = [tx_csv] if isinstance(tx_csv, str) else tx_csv
    frames = []
    for p in paths:
        frame = pd.read_csv(p)
        frame['_source_file'] = str(p)
        frames.append(frame)
    tx_raw = pd.concat(frames, ignore_index=True)
    tx_raw = strip_known_fidelity_footer_rows(tx_raw)
    if len(paths) > 1:
        check_no_cross_file_duplicates(tx_raw)

    tx = tx_raw.drop(columns=['_source_file']).copy()
    tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
    tx['trade_date']       = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
    tx['shares']           = pd.to_numeric(tx['Shares'], errors='coerce')
    tx['amount_usd']       = pd.to_numeric(tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')
    check_transaction_data_consistency(tx)
    logging.info(f"Loaded {len(tx)} transactions from {len(paths)} file(s).")
    return tx


def _round_tax(value: float) -> int:
    """Round to full PLN per Ordynacja Podatkowa art. 63 §1.

    Fractional amounts below 50 groszy are dropped; 50 groszy and above
    are rounded up to the next full zloty.

    Examples: 1234.49 -> 1234, 1234.50 -> 1235, 1234.99 -> 1235, 0.0 -> 0
    """
    if value < 0:
        return 0
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _round_up_to_grosz(value: float) -> float:
    """Round up to full grosz (0.01 PLN) per Ordynacja art. 63 §1a."""
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_CEILING))


def calculate_pit38_fields(
    total_proceeds: float,
    total_costs: float,
    total_gain: float,
    total_dividends: float,
    foreign_tax_dividends: float,
    foreign_tax_capital_gains: float = 0.0,
) -> Dict[str, float]:
    """Compute PIT-38 and PIT-ZG field values from aggregated totals.

    Section C/D (art. 30b) — capital gains from stock sales:
      Poz. 22 = total proceeds (stock sales only, no dividends)
      Poz. 23 = total costs
      Poz. 26 = Poz. 22 - Poz. 23 (income)
      Poz. 29 = tax base, rounded per Ordynacja art. 63 §1
      Poz. 30 = 19% rate
      Poz. 31 = Poz. 29 × 19%
      Poz. 32 = foreign tax paid on capital gains (credit, capped at Poz. 31)
      Poz. 33 = max(Poz. 31 - Poz. 32, 0), rounded per art. 63

    Section G (art. 30a ust.1 pkt 1-5) — zryczałtowane przychody zagraniczne:
      Poz. 45 = 19% tax on gross Section-G income (rounded to grosze up, per art.63 §1a)
      Poz. 46 = foreign withholding tax attributable to Section G income
      Poz. 47 = max(Poz. 45 - Poz. 46, 0) (rounded per art. 63)

    PIT-ZG attachment — foreign income:
      pitzg_poz29 = capital gains from foreign sources
      pitzg_poz30 = foreign tax on capital gains

    Args:
        total_proceeds: Total sale proceeds in PLN.
        total_costs: Total cost basis in PLN.
        total_gain: Net gain from stock sales in PLN (proceeds - costs).
        total_dividends: Total Section-G gross income in PLN (includes
                dividend-like and fund-like distributions from input).
        foreign_tax_dividends: Foreign withholding tax for Section G income in PLN.
        foreign_tax_capital_gains: Foreign tax paid on capital gains in PLN
                (credit in Poz. 32, capped at Polish tax from Poz. 31).

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
    poz32 = round(min(max(foreign_tax_capital_gains, 0.0), poz31), 2)
    tax_final = _round_tax(max(poz31 - poz32, 0))  # Poz. 33

    # --- Section G: zryczałtowane przychody (art. 30a ust.1 pkt 1-5) ---
    poz45 = _round_up_to_grosz(total_dividends * poz30_rate)
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
    dropped_settlement_rows = int(tx['settlement_date'].isna().sum())
    tx = tx.dropna(subset=['settlement_date'])
    if dropped_settlement_rows:
        logging.warning(
            "Dropping %d transaction row(s) with missing settlement_date; "
            "verify Transaction date parsing and transaction-type classification.",
            dropped_settlement_rows,
        )

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

    section_g = compute_section_g_income_components(merged, year=year)
    total_dividends = section_g['section_g_total_income']
    foreign_tax_dividends = section_g['section_g_foreign_tax']
    foreign_tax_capital_gains = compute_foreign_tax_capital_gains(merged, year=year)

    result = calculate_pit38_fields(
        total_proceeds,
        total_costs,
        total_gain,
        total_dividends,
        foreign_tax_dividends,
        foreign_tax_capital_gains=foreign_tax_capital_gains,
    )
    result.update(
        {
            'section_g_total_income': section_g['section_g_total_income'],
            'section_g_equity_dividends': section_g['section_g_equity_dividends'],
            'section_g_fund_distributions': section_g['section_g_fund_distributions'],
        }
    )
    result['year'] = year
    return result
