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

from .pit38_fields import PIT38Fields
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
)

# constant for switch from T+2 to T+1
SWITCH_DATE = pd.Timestamp('2024-05-28')
DecimalLike = Union[Decimal, float, int, str]
TWO_PLACES = Decimal("0.01")

class USSettlementHolidayCalendar(AbstractHolidayCalendar):
    """US settlement calendar: federal holidays plus Good Friday."""
    rules = list(USFederalHolidayCalendar.rules) + [GoodFriday]


# US settlement calendar offsets — stateless, built once at import time
_US_BD1 = CustomBusinessDay(calendar=USSettlementHolidayCalendar(), n=1)
_US_BD2 = CustomBusinessDay(calendar=USSettlementHolidayCalendar(), n=2)

# Transaction types that trigger market (T+1/T+2) settlement
_MARKET_SETTLEMENT_TAGS = ('YOU BOUGHT', 'YOU SOLD', 'ESPP')


def _as_list(value: Union[str, List[str]]) -> List[str]:
    """Normalize a single path string or list of path strings to a list."""
    return [value] if isinstance(value, str) else value


def _strip_known_fidelity_footer_rows(tx_raw: pd.DataFrame) -> pd.DataFrame:
    """Silently remove known non-transaction footer rows from Fidelity CSV exports."""
    footer_mask = (
        tx_raw['Transaction type'].isna() &
        tx_raw['Investment name'].isna() &
        tx_raw['Shares'].isna() &
        tx_raw['Amount'].isna() &
        tx_raw['Transaction date'].astype(str).str.contains(
            r"Unless noted otherwise|Stock plan account history as of",
            case=False,
            na=False,
        )
    )
    if footer_mask.any():
        return tx_raw.loc[~footer_mask].copy()
    return tx_raw


def discover_transaction_files(directory: str) -> Tuple[List[str], List[str]]:
    """Glob for transaction history CSVs and stock-sales TXTs in a directory.

    Args:
        directory: Path to the directory to scan.

    Returns:
        Tuple of (transaction_history_csv_files, stock_sales_txt_files),
        each sorted alphabetically.
    """
    d = Path(directory)
    transaction_history_csv_files = sorted(str(p) for p in d.glob("Transaction history*.csv"))
    stock_sales_txt_files = sorted(str(p) for p in d.glob("stock-sales*.txt"))
    logging.info(
        "Discovered %d CSV(s) and %d TXT(s) in %s",
        len(transaction_history_csv_files),
        len(stock_sales_txt_files),
        directory,
    )
    for p in transaction_history_csv_files:
        logging.info("  CSV: %s", p)
    for p in stock_sales_txt_files:
        logging.info("  TXT: %s", p)
    return transaction_history_csv_files, stock_sales_txt_files


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
    logging.info("NBP rate URLs for years %s: %d files", list(all_years), len(urls))
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
    logging.info("Loaded %d exchange-rate entries.", len(rates))
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
    settlements: List[Optional[pd.Timestamp]] = []
    for d, ttype in zip(trade_dates, tx_types):
        if pd.isna(d):
            settlements.append(pd.NaT)
            continue
        if any(tag in ttype for tag in _MARKET_SETTLEMENT_TAGS):
            # T+2 before SWITCH_DATE, T+1 after
            settlements.append(d + (_US_BD2 if d < SWITCH_DATE else _US_BD1))
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


def _open_buy_lots(buys: pd.DataFrame, sale_investment: Optional[str]) -> pd.DataFrame:
    """Return buy lots with remaining shares > 0, filtered to the given investment name."""
    open_lots = buys[buys['remaining'] > 0]
    if 'Investment name' in open_lots.columns and pd.notna(sale_investment):
        open_lots = open_lots[open_lots['Investment name'] == sale_investment]
    return open_lots


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
        qty = abs(sale['shares'])
        price_per = sale['amount_pln'] / qty if qty else 0
        available_qty = _open_buy_lots(buys, sale_investment)['remaining'].sum()
        check_fifo_sale_not_oversell(sale['settlement_date'], qty, available_qty)
        while qty > 0:
            open_lots = _open_buy_lots(buys, sale_investment)
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
    logging.info("FIFO: matched %d lots; Gain PLN: %.2f", len(allocs), total_gain)
    return total_proceeds, total_costs, total_gain


def _filter_by_identifier(
    df: pd.DataFrame,
    custom_symbol: Optional[str],
    custom_investment_name: Optional[str],
    label: str,
    date_value: pd.Timestamp,
) -> pd.DataFrame:
    """Filter transaction rows to those matching the given symbol or investment name.

    Tries, in order: exact Symbol match, exact Investment name match, token
    match in Investment name. Returns an empty DataFrame if a symbol is given
    but cannot be matched; returns the input DataFrame unchanged when no
    identifier is available.
    """
    if df.empty:
        return df

    if custom_symbol:
        symbol_upper = custom_symbol.upper()
        if 'Symbol' in df.columns:
            exact_mask = df['Symbol'].astype(str).str.strip().str.upper() == symbol_upper
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
        name_mask = df['Investment name'].astype(str).str.strip().str.upper() == custom_investment_name.upper()
        if name_mask.any():
            return df[name_mask]

    return df


def process_custom(
    merged: pd.DataFrame,
    custom_summary_path: Union[str, List[str]],
    year: Optional[int] = None,
    nbp_rates: Optional[pd.DataFrame] = None,  # kept for API compatibility; unused
) -> Tuple[float, float, float]:
    """Match stock sales to specific lots using a Fidelity custom summary file.

    Reads one or more tab-separated summary files (e.g. stock-sales.txt) with
    columns: 'Date sold or transferred', 'Date acquired', 'Quantity',
    'Stock source', etc. Each row identifies a specific lot to match against
    the transaction history.

    Cost basis logic:
      - 'RS' (RSU): cost is always 0.0 under Polish art. 30b. The 'Cost basis'
        column in Fidelity exports reflects the US FMV-at-vest amount (ordinary
        income already recognized in the US), which is not a deductible cost
        in the Polish tax calculation. The value is ignored entirely.
      - 'SP' (ESPP) with parseable 'Cost basis': cost basis converted to PLN
        using the matching buy transaction's exchange rate.
      - 'SP' / other without parseable 'Cost basis': cost derived from the
        matching buy transaction's amount_pln.

    Sale and buy lookups try trade_date first, then fall back to
    settlement_date, to handle date ambiguity in Fidelity exports.

    Args:
        merged: Transaction DataFrame (output of merge_with_rates), must include
                'trade_date', 'settlement_date', 'Transaction type', 'shares',
                and 'amount_pln'.
        custom_summary_path: Path (or list of paths) to tab-separated custom
                summary TXT file(s).
        year: If set, only rows matched to sales settling in this year are included.
        nbp_rates: Unused; kept for API compatibility.

    Returns:
        Tuple of (total_proceeds, total_costs, total_gain) in PLN.
    """
    # normalize dates for matching
    merged['trade_date_norm'] = merged['trade_date'].dt.normalize()
    merged['settlement_norm'] = merged['settlement_date'].dt.normalize()

    paths = _as_list(custom_summary_path)
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
        sale_tx = _filter_by_identifier(sale_tx, custom_symbol, custom_investment_name, label='sale', date_value=sale_date)
        if not check_custom_sale_record_exists(sale_tx, sale_date):
            continue
        check_custom_sale_match_unambiguous(sale_date, len(sale_tx))
        sale = sale_tx.iloc[0]
        sale_investment = sale.get('Investment name') if 'Investment name' in sale.index else None
        price_per = sale['amount_pln'] / abs(sale['shares']) if sale['shares'] else 0
        proceeds   = round(qty * price_per, 2)

        # determine cost
        # RS (RSU) lots: cost is always 0.0 under Polish art. 30b.  The 'Cost basis'
        # column in Fidelity exports is the US FMV-at-vest figure (ordinary income
        # already taxed in the US) — it is not a deductible cost for Polish tax.
        # SP/other: look up the matching buy transaction to get its PLN amount or rate.
        buy = None
        if source != 'RS':
            # match buy by trade_date or settlement_date
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

        if source == 'RS':
            cost = 0.0
        elif pd.notna(reported_cost_basis_usd):
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
    logging.info("Custom (by specific lots): matched %d lots; Gain PLN: %.2f", len(allocs), total_gain)
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

    fund_distributions = abs(round(div_rows.loc[fund_mask, 'amount_pln'].sum(), 2))
    equity_dividends = abs(round(div_rows.loc[~fund_mask, 'amount_pln'].sum(), 2))
    total_income = round(equity_dividends + fund_distributions, 2)

    foreign_tax_mask = (
        df['Transaction type'].str.contains('NON-RESIDENT TAX', na=False) &
        df['Transaction type'].str.contains('DIVIDEND', na=False)
    )
    foreign_tax = round(-df[foreign_tax_mask]['amount_pln'].sum(), 2)
    foreign_tax = max(foreign_tax, 0.0)  # avoid -0.00 display; tax withheld is non-negative

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
        "Section G income PLN: %.2f (equity dividends: %.2f; fund distributions: %.2f); Foreign tax PLN: %.2f",
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
    capital_tax = round(max(capital_tax, 0.0), 2)
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
    paths = _as_list(tx_csv)
    frames = []
    for p in paths:
        frame = pd.read_csv(p)
        frame['_source_file'] = str(p)
        frames.append(frame)
    tx_raw = pd.concat(frames, ignore_index=True)
    tx_raw = _strip_known_fidelity_footer_rows(tx_raw)
    if len(paths) > 1:
        check_no_cross_file_duplicates(tx_raw)

    tx = tx_raw.drop(columns=['_source_file']).copy()
    tx['Transaction type'] = tx['Transaction type'].astype(str).str.split(';').str[0]
    tx['trade_date']       = pd.to_datetime(tx['Transaction date'], format='%b-%d-%Y', errors='coerce')
    tx['shares']           = pd.to_numeric(tx['Shares'], errors='coerce')
    tx['amount_usd']       = pd.to_numeric(tx['Amount'].str.replace('[$,]', '', regex=True), errors='coerce')
    check_transaction_data_consistency(tx)
    logging.info("Loaded %d transactions from %d file(s).", len(tx), len(paths))
    return tx


def _round_tax(value: DecimalLike) -> int:
    """Round to full PLN per Ordynacja Podatkowa art. 63 §1.

    Fractional amounts below 50 groszy are dropped; 50 groszy and above
    are rounded up to the next full zloty.

    Examples: 1234.49 -> 1234, 1234.50 -> 1235, 1234.99 -> 1235, 0.0 -> 0
    """
    value_dec = Decimal(value)
    if value_dec < 0:
        return 0
    return int(value_dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _round_up_to_grosz(value: DecimalLike) -> Decimal:
    """Round up to full grosz (0.01 PLN) per Ordynacja art. 63 §1a."""
    return Decimal(value).quantize(TWO_PLACES, rounding=ROUND_CEILING)


def calculate_pit38_fields(
    total_proceeds: DecimalLike,
    total_costs: DecimalLike,
    total_gain: DecimalLike,
    total_dividends: DecimalLike,
    foreign_tax_dividends: DecimalLike,
    foreign_tax_capital_gains: DecimalLike = Decimal("0.0"),
    *,
    section_g_equity_dividends: DecimalLike = Decimal("0.0"),
    section_g_fund_distributions: DecimalLike = Decimal("0.0"),
    year: Optional[int] = None,
) -> PIT38Fields:
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
        section_g_equity_dividends: Equity-dividend portion of Section G income (metadata).
        section_g_fund_distributions: Fund-distribution portion of Section G income (metadata).
        year: Tax year (metadata, stored on the returned object).

    Returns:
        PIT38Fields object with PIT-38 Section C/D, Section G, and PIT-ZG fields.
    """
    proceeds_dec = Decimal(total_proceeds)
    costs_dec = Decimal(total_costs)
    gain_dec = Decimal(total_gain)
    dividends_dec = Decimal(total_dividends)
    foreign_tax_div_dec = Decimal(foreign_tax_dividends)
    foreign_tax_cap_gain_dec = Decimal(foreign_tax_capital_gains)

    # --- Section C/D: capital gains (art. 30b) ---
    poz22 = proceeds_dec.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    poz23 = costs_dec.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    poz26 = (poz22 - poz23).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    poz29 = Decimal(_round_tax(poz26))          # tax base
    poz30_rate = Decimal("0.19")
    poz31 = (poz29 * poz30_rate).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    poz32 = min(max(foreign_tax_cap_gain_dec, Decimal("0.00")), poz31).quantize(
        TWO_PLACES,
        rounding=ROUND_HALF_UP,
    )
    tax_final = Decimal(_round_tax(poz31 - poz32))  # Poz. 33

    # --- Section G: zryczałtowane przychody (art. 30a ust.1 pkt 1-5) ---
    poz45 = _round_up_to_grosz(dividends_dec * poz30_rate)
    poz46 = min(foreign_tax_div_dec, poz45).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    poz47 = Decimal(_round_tax(poz45 - poz46))

    # --- PIT-ZG: foreign income ---
    pitzg_poz29 = gain_dec.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    pitzg_poz30 = poz32  # foreign tax on capital gains (not dividends)

    return PIT38Fields(
        poz22=poz22,
        poz23=poz23,
        poz26=poz26,
        poz29=poz29,
        poz30_rate=poz30_rate,
        poz31=poz31,
        poz32=poz32,
        tax_final=tax_final,
        poz45=poz45,
        poz46=poz46,
        poz47=poz47,
        pitzg_poz29=pitzg_poz29,
        pitzg_poz30=pitzg_poz30,
        section_g_total_income=dividends_dec.quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
        section_g_equity_dividends=Decimal(section_g_equity_dividends).quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
        section_g_fund_distributions=Decimal(section_g_fund_distributions).quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
        year=year,
    )


def calculate_pit38(
    tx_csv: Union[str, List[str]],
    year: int = 2024,
    method: str = 'fifo',
    custom_summary: Optional[List[str]] = None,
) -> PIT38Fields:
    """Run the full PIT-38 calculation pipeline.

    Args:
        tx_csv: Path (or list of paths) to Fidelity transaction history CSV file(s).
        year: Tax year to process.
        method: 'fifo' or 'custom' lot matching method.
        custom_summary: List of paths to custom summary TXT files.
                Required when method='custom'; use [] in non-custom flows.

    Returns:
        PIT38Fields with PIT-38/PIT-ZG values and report metadata.
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
            "Target year %d not found in transaction data. Data contains years: %s. "
            "Use --year to specify the correct tax year.",
            year,
            data_years,
        )

    tx['rate_date'] = calculate_rate_dates(tx['settlement_date'])
    merged = merge_with_rates(tx, nbp_rates)

    custom_summary_paths = custom_summary or []

    if method == 'fifo':
        total_proceeds, total_costs, total_gain = process_fifo(merged, year=year)
    else:
        if not custom_summary_paths:
            raise ValueError("custom_summary is required when method='custom'")
        total_proceeds, total_costs, total_gain = process_custom(merged, custom_summary_paths, year=year, nbp_rates=nbp_rates)

    section_g = compute_section_g_income_components(merged, year=year)
    logging.info(
        "Section G income PLN: %.2f (equity dividends: %.2f; fund distributions: %.2f); Foreign tax PLN: %.2f",
        section_g['section_g_total_income'],
        section_g['section_g_equity_dividends'],
        section_g['section_g_fund_distributions'],
        section_g['section_g_foreign_tax'],
    )
    foreign_tax_capital_gains = compute_foreign_tax_capital_gains(merged, year=year)

    return calculate_pit38_fields(
        total_proceeds,
        total_costs,
        total_gain,
        section_g['section_g_total_income'],
        section_g['section_g_foreign_tax'],
        foreign_tax_capital_gains=foreign_tax_capital_gains,
        section_g_equity_dividends=section_g['section_g_equity_dividends'],
        section_g_fund_distributions=section_g['section_g_fund_distributions'],
        year=year,
    )
