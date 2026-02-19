import logging
from typing import Optional

import pandas as pd

def check_no_cross_file_duplicates(tx_raw: pd.DataFrame) -> None:
    """Raise if identical transaction rows are present in different source files."""
    if '_source_file' not in tx_raw.columns:
        return
    value_cols = [c for c in tx_raw.columns if c != '_source_file']
    cross_file_dup = (
        tx_raw.groupby(value_cols, dropna=False)['_source_file']
        .nunique()
        .reset_index(name='n_files')
    )
    cross_file_dup = cross_file_dup[cross_file_dup['n_files'] > 1]
    if len(cross_file_dup):
        raise ValueError(
            "Duplicate transaction rows found across different CSV files. "
            f"Detected {len(cross_file_dup)} duplicated row pattern(s); "
            "remove overlap in input files."
        )


def check_transaction_data_consistency(tx: pd.DataFrame) -> None:
    """Log data-quality issues detected in parsed transaction rows."""
    core_cols = ['Transaction type', 'Investment name', 'Shares', 'Amount']
    has_any_core_value = tx[core_cols].notna().any(axis=1)

    invalid_trade_dates = tx['trade_date'].isna()
    if invalid_trade_dates.any():
        malformed_rows = tx[invalid_trade_dates & has_any_core_value]
        if len(malformed_rows):
            logging.error(
                f"Data inconsistency: {len(malformed_rows)} row(s) have invalid 'Transaction date' "
                "with non-empty transaction fields."
            )
        ignored_blank_date_rows = int((invalid_trade_dates & ~has_any_core_value).sum())
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


def check_fifo_sale_not_oversell(settlement_date: pd.Timestamp, qty: float, available_qty: float) -> None:
    """Log when sale quantity exceeds currently available buy lots."""
    if qty > available_qty:
        logging.error(
            f"FIFO inconsistency: attempting to sell {qty:.4f} shares on "
            f"{settlement_date.date()}, but only {available_qty:.4f} shares remain in buy lots."
        )


def check_fifo_open_lots_available(settlement_date: pd.Timestamp, qty: float, has_open_lots: bool) -> bool:
    """Check whether any open buy lots are available for current sale matching."""
    if not has_open_lots:
        logging.error(
            f"FIFO inconsistency: no remaining buy lots for sale on "
            f"{settlement_date.date()}; unmatched quantity {qty:.4f} shares."
        )
        return False
    return True


def check_custom_summary_rows_valid(custom: pd.DataFrame) -> None:
    """Log invalid custom-summary rows (missing dates/quantity or non-positive quantity)."""
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


def check_custom_sale_date_quantities(
    custom: pd.DataFrame,
    merged: pd.DataFrame,
    year: Optional[int] = None,
) -> None:
    """Validate sale-date quantities from custom summary against transaction history."""
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


def check_custom_acquired_quantities(custom: pd.DataFrame, merged: pd.DataFrame) -> None:
    """Validate acquired quantities by source/date against available buy lots."""
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


def check_custom_sale_match_unambiguous(sale_date: pd.Timestamp, count: int) -> None:
    """Log ambiguous sale-date matching in custom mode."""
    if count > 1:
        logging.error(
            f"Custom summary ambiguity: {count} sale rows match {sale_date.date()}; using the first one."
        )


def check_custom_buy_match_unambiguous(acq_date: pd.Timestamp, source: Optional[str], count: int) -> None:
    """Log ambiguous buy-date matching in custom mode."""
    if count > 1:
        logging.error(
            f"Custom summary ambiguity: {count} buy rows match {acq_date.date()} "
            f"(source={source}); using the first one."
        )


def check_exchange_rates_present(missing_count: int) -> None:
    """Log missing exchange-rate matches after rate merge."""
    if missing_count:
        logging.error(f"{missing_count} transactions missing exchange rate.")


def check_custom_sale_record_exists(sale_tx: pd.DataFrame, sale_date: pd.Timestamp) -> bool:
    """Check whether a custom-summary row can be matched to a sale record."""
    if sale_tx.empty:
        logging.error(f"No sale record found for {sale_date}")
        return False
    return True


def check_custom_buy_record_exists(buy_tx: pd.DataFrame, acq_date: pd.Timestamp, source: Optional[str]) -> bool:
    """Check whether a custom-summary row can be matched to a buy record."""
    if buy_tx.empty:
        logging.error(f"No buy record found for {acq_date} (source={source})")
        return False
    return True
