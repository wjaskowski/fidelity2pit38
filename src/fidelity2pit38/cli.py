#!/usr/bin/env python3

import argparse
import logging

from .core import calculate_pit38


def main() -> None:
    """CLI entry point: parse arguments and print PIT-38/PIT-ZG results."""
    parser = argparse.ArgumentParser(description='Compute PIT-38 summary from Fidelity CSV')
    parser.add_argument('tx_csv', help='Path to the transaction history CSV file')
    parser.add_argument('--method', choices=['fifo', 'custom'], default='fifo',
                        help='Use FIFO (default) or custom summary for matching')
    parser.add_argument('--custom_summary',
                        help='Path to custom transaction summary (TXT) for method=custom')
    parser.add_argument('--year', type=int, default=2024,
                        help='Tax year to process (default: 2024)')
    args = parser.parse_args()

    if args.method == 'custom' and not args.custom_summary:
        parser.error("--custom_summary is required when method=custom")

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    result = calculate_pit38(
        tx_csv=args.tx_csv,
        year=args.year,
        method=args.method,
        custom_summary=args.custom_summary,
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
