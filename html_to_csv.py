from __future__ import annotations

import argparse
import csv
from pathlib import Path

from bs4 import BeautifulSoup


def clean_text(value: str) -> str:
	return " ".join(value.replace("\xa0", " ").split())


def extract_table_to_rows(html_path: Path, table_id: str) -> tuple[list[str], list[list[str]]]:
	html = html_path.read_text(encoding="utf-8")
	soup = BeautifulSoup(html, "lxml")

	table = soup.find("table", id=table_id)
	if table is None:
		raise ValueError(f"No table with id '{table_id}' found in {html_path}")

	header_cells = table.select("thead tr th")
	headers = [clean_text(cell.get_text()) for cell in header_cells]
	if not headers:
		raise ValueError(f"Table '{table_id}' has no header row")

	body_rows = table.select("tbody tr")
	rows: list[list[str]] = []

	for row in body_rows:
		cells = row.find_all("td")
		values: list[str] = []

		for cell in cells:
			link = cell.find("a")
			if link and link.get("href"):
				values.append(clean_text(link["href"]))
			else:
				values.append(clean_text(cell.get_text()))

		if values:
			rows.append(values)

	return headers, rows


def write_csv(csv_path: Path, headers: list[str], rows: list[list[str]]) -> None:
	with csv_path.open("w", newline="", encoding="utf-8") as handle:
		writer = csv.writer(handle)
		writer.writerow(headers)
		writer.writerows(rows)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Convert an HTML table into a CSV file")
	parser.add_argument(
		"input_html",
		nargs="?",
		default="disks.html",
		help="Path to source HTML file (default: disks.html)",
	)
	parser.add_argument(
		"output_csv",
		nargs="?",
		default="disks.csv",
		help="Path to output CSV file (default: disks.csv)",
	)
	parser.add_argument(
		"--table-id",
		default="diskprices",
		help="ID of the table to extract (default: diskprices)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	input_path = Path(args.input_html)
	output_path = Path(args.output_csv)

	headers, rows = extract_table_to_rows(input_path, args.table_id)
	write_csv(output_path, headers, rows)

	print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
	main()
