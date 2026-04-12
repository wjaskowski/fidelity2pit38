#!/usr/bin/env python3
"""Replace the ```sh + ```diff blocks in README.md with live output from data-sample."""
import pathlib
import re
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent

from fidelity2pit38.core import calculate_pit38, discover_transaction_files
from fidelity2pit38.report import ReportData, render_console

# Single source of truth for the example command shown in README.
EXAMPLE_DATA_DIR = "data-sample"
EXAMPLE_YEAR = 2025
EXAMPLE_METHOD = "fifo"


def main() -> None:
    csv_files, _ = discover_transaction_files(str(ROOT / EXAMPLE_DATA_DIR))

    with tempfile.TemporaryDirectory() as tmp:
        result = calculate_pit38(
            tx_csv=csv_files,
            year=EXAMPLE_YEAR,
            method=EXAMPLE_METHOD,
            report_dir=tmp,
            open_browser=False,
        )

    method_label = "FIFO" if EXAMPLE_METHOD == "fifo" else "Custom"
    diff_text = render_console(
        ReportData(year=EXAMPLE_YEAR, capital_gains=[], dividends=[], pit38=result, method=method_label),
        diff_format=True,
    )

    cmd = f"uv run fidelity2pit38 --data-dir {EXAMPLE_DATA_DIR} --year {EXAMPLE_YEAR}"
    new_blocks = f"```sh\n{cmd}\n```\n\n```diff\n{diff_text.strip()}\n```"

    readme_path = ROOT / "README.md"
    readme = readme_path.read_text()
    updated, n = re.subn(
        r"```sh\nuv run fidelity2pit38[^\n]*\n```\n\n```diff\n.*?```",
        new_blocks,
        readme,
        flags=re.DOTALL,
    )
    if n == 0:
        print("update_readme.py: marker blocks not found in README.md — nothing updated", file=sys.stderr)
        sys.exit(1)
    readme_path.write_text(updated)
    print(f"update_readme.py: README.md updated ({n} block(s) replaced).")


if __name__ == "__main__":
    main()
