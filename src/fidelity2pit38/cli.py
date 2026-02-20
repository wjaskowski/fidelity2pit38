#!/usr/bin/env python3

import argparse
import datetime
import logging

from .core import calculate_pit38, discover_transaction_files


def main() -> None:
    """CLI entry point: parse arguments and print PIT-38/PIT-ZG results."""
    default_year = datetime.date.today().year - 1

    parser = argparse.ArgumentParser(
        description='Compute PIT-38 summary from Fidelity CSV',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--data-dir', default='data',
                        help='Directory with transaction files')
    parser.add_argument('--method', choices=['fifo', 'custom'], default='fifo',
                        help='Use FIFO or custom summary for matching')
    parser.add_argument('--year', type=int, default=default_year,
                        help='Tax year to process')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Discover transaction files
    transaction_history_csv_files, stock_sales_txt_files = discover_transaction_files(args.data_dir)
    if not transaction_history_csv_files:
        parser.error(f"No 'Transaction history*.csv' files found in {args.data_dir}")

    # Determine stock-sales TXT paths for custom mode.
    custom_mode_stock_sales_txt_files = []
    if args.method == 'custom':
        if not stock_sales_txt_files:
            parser.error(f"No 'stock-sales*.txt' files found in {args.data_dir}")
        custom_mode_stock_sales_txt_files = stock_sales_txt_files

    result = calculate_pit38(
        tx_csv=transaction_history_csv_files,
        year=args.year,
        method=args.method,
        custom_summary=custom_mode_stock_sales_txt_files,
    )
    result.print()
