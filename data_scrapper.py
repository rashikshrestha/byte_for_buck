import csv
import sys
import time
from pathlib import Path

from scrap_a_page import fetch_page, extract_relevant_sections, extract_disk_info_with_claude

INPUT_CSV = "data/raw.csv"
OUTPUT_CSV = "data/scrapped.csv"
URL_COLUMN = "Affiliate Link"

# Fields scraped by Claude that we do NOT want as extra columns
EXCLUDED_FIELDS = {"price_usd"}

# Ordered list of scraped fields to add as columns (stable ordering)
SCRAPED_COLUMNS = [
    "product_name",
    "average_rating",
    "total_no_of_reviews",
    "brand",
    "model",
    "in_stock",
    "condition",
    "ships_from",
    "sold_by",
    "prime_eligible",
    "drive_type",
    "form_factor",
    "interface",
    "read_speed_mbps",
    "write_speed_mbps",
    "estimated_delivery_date",
    "warranty_months",
]


def scrape_row(url: str) -> dict:
    try:
        html = fetch_page(url)
        page_content = extract_relevant_sections(html)
        return extract_disk_info_with_claude(page_content, url)
    except Exception as exc:
        print(f"  ERROR scraping {url}: {exc}", file=sys.stderr)
        return {}


def load_scraped_urls(output_path: Path) -> set:
    if not output_path.exists():
        return set()
    with output_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row[URL_COLUMN].strip() for row in reader if row.get(URL_COLUMN)}


def main() -> None:
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_CSV)

    rows = list(csv.DictReader(input_path.open(encoding="utf-8")))
    if not rows:
        print("disks.csv is empty.")
        return

    already_scraped = load_scraped_urls(output_path)
    print(f"Already scraped: {len(already_scraped)} rows — will skip those.")

    original_columns = list(rows[0].keys())
    all_columns = original_columns + SCRAPED_COLUMNS

    output_exists = output_path.exists()
    out_file = output_path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=all_columns, extrasaction="ignore")

    if not output_exists:
        writer.writeheader()

    total = len(rows)
    for i, row in enumerate(rows, start=1):
        url = row.get(URL_COLUMN, "").strip()

        if url in already_scraped:
            print(f"[{i}/{total}] SKIP (already scraped): {url}")
            continue

        print(f"[{i}/{total}] Scraping: {url}")

        scraped = scrape_row(url) if url else {}

        for col in SCRAPED_COLUMNS:
            row[col] = scraped.get(col, "")

        writer.writerow(row)
        out_file.flush()
        already_scraped.add(url)

        if i < total:
            time.sleep(1)  # polite delay between requests

    out_file.close()
    print(f"\nDone. Output in {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
