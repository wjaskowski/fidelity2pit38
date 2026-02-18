#!/usr/bin/env python3

import argparse
import datetime
import logging

from .core import calculate_pit38, discover_transaction_files


def main() -> None:
    """CLI entry point: parse arguments and print PIT-38/PIT-ZG results."""
    default_year = datetime.date.today().year - 1

    parser = argparse.ArgumentParser(description='Compute PIT-38 summary from Fidelity CSV')
    parser.add_argument('--data-dir', default='data',
                        help='Directory with transaction files (default: data)')
    parser.add_argument('--method', choices=['fifo', 'custom'], default='fifo',
                        help='Use FIFO (default) or custom summary for matching')
    parser.add_argument('--custom-summary', nargs='+',
                        help='Path(s) to custom transaction summary TXT file(s)')
    parser.add_argument('--year', type=int, default=default_year,
                        help=f'Tax year to process (default: {default_year})')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Discover transaction files
    csv_paths, discovered_txt_paths = discover_transaction_files(args.data_dir)
    if not csv_paths:
        parser.error(f"No 'Transaction history*.csv' files found in {args.data_dir}")
    tx_csv = csv_paths if len(csv_paths) > 1 else csv_paths[0]

    # Determine custom summary paths
    custom_summary = None
    if args.method == 'custom':
        if args.custom_summary:
            custom_summary = args.custom_summary if len(args.custom_summary) > 1 else args.custom_summary[0]
        else:
            if not discovered_txt_paths:
                parser.error(f"No 'stock-sales*.txt' files found in {args.data_dir}; use --custom-summary")
            custom_summary = discovered_txt_paths if len(discovered_txt_paths) > 1 else discovered_txt_paths[0]

    result = calculate_pit38(
        tx_csv=tx_csv,
        year=args.year,
        method=args.method,
        custom_summary=custom_summary,
    )

    print(f"\n\nPIT-38 for year {result['year']}:")
    print(f"Poz. 22 (Przychód): {result['poz22']:.2f} PLN")
    print(f"Poz. 23 (Koszty uzyskania): {result['poz23']:.2f} PLN")
    print(f"Poz. 26 (Dochód): {result['poz26']:.2f} PLN")
    print(f"Poz. 29 (Podstawa opodatkowania): {result['poz29']}.00 PLN")
    print(f"Poz. 30 (Stawka podatku): {int(result['poz30_rate']*100)}%")
    print(f"Poz. 31 (Podatek od dochodów z poz. 29): {result['poz31']:.2f} PLN")
    print(f"Poz. 32 (Podatek zapłacony za granicą): {result['poz32']:.2f} PLN")
    print(f"Poz. 33 (Podatek należny): {result['tax_final']:.2f} PLN")

    print("\nPIT-ZG:")
    print(f"Poz. 29 (Dochód, o którym mowa w art. 30b ust.5 i 5b): {result['pitzg_poz29']:.2f} PLN")
    print(f"Poz. 30 (Podatek zapłacony za granicą od dochodów z poz. 29): {result['pitzg_poz30']:.2f} PLN")
