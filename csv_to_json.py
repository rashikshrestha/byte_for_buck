import csv
import json
import re
from pathlib import Path

INPUT_CSV = "data/data_v1.csv"
OUTPUT_JSON = "data/data_v1.json"
DROP_COLUMNS = {"Warranty", "Form Factor"}


def clean_capacity(value: str) -> float | str:
    value = re.sub(r'\s*x\d+\s*$', '', value, flags=re.IGNORECASE).strip()
    m = re.search(r'([\d.]+)\s*(TB|GB|MB)', value, flags=re.IGNORECASE)
    if not m:
        return value
    amount, unit = float(m.group(1)), m.group(2).upper()
    if unit == "GB":
        amount /= 1024
    elif unit == "MB":
        amount /= 1024 ** 2
    return round(amount, 6)


def main() -> None:
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_JSON)

    rows = []
    with input_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if "Capacity" in row:
                row["Capacity"] = clean_capacity(row["Capacity"])
            for speed_col in ("read_speed_mbps", "write_speed_mbps"):
                if row.get(speed_col) == "7200":
                    row[speed_col] = "0"
            rows.append({k: v for k, v in row.items() if k not in DROP_COLUMNS})

    output_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"Wrote {len(rows)} records to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
