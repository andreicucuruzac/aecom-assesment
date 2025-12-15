#!/usr/bin/env python
import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SALES_PATH = DATA_DIR / "sales.csv"

N_NEW_ROWS = 10  


def main():
    if not SALES_PATH.exists():
        raise FileNotFoundError(f"{SALES_PATH} not found. Run generate_data.py first.")

    df = pd.read_csv(SALES_PATH)


    max_id = (
        df["transaction_id"]
        .astype(str)
        .str.extract(r"T(\d+)", expand=False)
        .astype(int)
        .max()
    )


    tx_dates = pd.to_datetime(df["transaction_date"], errors="coerce", infer_datetime_format=True)
    max_date = tx_dates.max().date()
    start_date = max_date + timedelta(days=1)

    rows = []
    for i in range(1, N_NEW_ROWS + 1):
        new_tid = f"T{max_id + i:06d}"
        new_pid = f"P{random.randint(1, 150):04d}"
        new_cid = f"C{random.randint(1, 600):04d}"
        qty = random.randint(1, 10)
        d = start_date + timedelta(days=i - 1)
        date_str = d.strftime("%Y-%m-%d")  
        rows.append([new_tid, new_pid, new_cid, qty, date_str])


    with open(SALES_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"Appended {len(rows)} new sales rows starting from date {start_date}")


if __name__ == "__main__":
    main()
