import os

import pytest

from fidelity2pit38 import build_nbp_rate_urls, discover_transaction_files


class TestDiscoverTransactionFiles:
    def test_finds_csvs_and_txts(self, tmp_path):
        (tmp_path / "Transaction history.csv").touch()
        (tmp_path / "Transaction history 2024.csv").touch()
        (tmp_path / "stock-sales.txt").touch()
        (tmp_path / "stock-sales-2024.txt").touch()
        (tmp_path / "unrelated.csv").touch()

        transaction_history_csv_files, stock_sales_txt_files = discover_transaction_files(str(tmp_path))
        assert len(transaction_history_csv_files) == 2
        assert len(stock_sales_txt_files) == 2
        assert all("Transaction history" in c for c in transaction_history_csv_files)
        assert all("stock-sales" in t for t in stock_sales_txt_files)

    def test_empty_directory(self, tmp_path):
        transaction_history_csv_files, stock_sales_txt_files = discover_transaction_files(str(tmp_path))
        assert transaction_history_csv_files == []
        assert stock_sales_txt_files == []

    def test_only_csvs(self, tmp_path):
        (tmp_path / "Transaction history.csv").touch()
        transaction_history_csv_files, stock_sales_txt_files = discover_transaction_files(str(tmp_path))
        assert len(transaction_history_csv_files) == 1
        assert len(stock_sales_txt_files) == 0

    def test_sorted_alphabetically(self, tmp_path):
        (tmp_path / "Transaction history 2025.csv").touch()
        (tmp_path / "Transaction history 2024.csv").touch()
        transaction_history_csv_files, _ = discover_transaction_files(str(tmp_path))
        assert "2024" in transaction_history_csv_files[0]
        assert "2025" in transaction_history_csv_files[1]


class TestBuildNbpRateUrls:
    def test_single_year(self):
        urls = build_nbp_rate_urls([2024])
        assert len(urls) == 2
        assert any("2023" in u for u in urls)
        assert any("2024" in u for u in urls)

    def test_multi_year(self):
        urls = build_nbp_rate_urls([2024, 2025])
        assert len(urls) == 3
        assert any("2023" in u for u in urls)
        assert any("2024" in u for u in urls)
        assert any("2025" in u for u in urls)

    def test_empty_years(self):
        urls = build_nbp_rate_urls([])
        assert urls == []

    def test_url_format(self):
        urls = build_nbp_rate_urls([2024])
        for url in urls:
            assert url.startswith("https://static.nbp.pl/dane/kursy/Archiwum/archiwum_tab_a_")
            assert url.endswith(".csv")
