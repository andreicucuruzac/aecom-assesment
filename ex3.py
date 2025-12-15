#!/usr/bin/env python
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_PATH = BASE_DIR / "etl_ex3.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ],
)


DB_HOST = "192.168.0.12"
DB_PORT = 3306
DB_USER = "AECOM"
DB_PASS = "12345"
DB_NAME = "AECOMDB"

TABLE_PREFIX = "ex_3_"

ENGINE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(ENGINE_URL)


CANONICAL_CATEGORIES = {
    "electronics": "Electronics",
    "clothing": "Clothing",
    "books": "Books",
    "home": "Home",
    "sports": "Sports",
}


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== Transforming products ===")
    logging.info("Raw products rows: %d", len(df))

    df["category"] = (
        df["category"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map(CANONICAL_CATEGORIES)
    )
    unknown_cats = df["category"].isna().sum()
    if unknown_cats:
        logging.warning("Products with unknown category after normalization: %d", unknown_cats)
        df.loc[df["category"].isna(), "category"] = "Unknown"

    missing_name_mask = df["product_name"].isna() | (df["product_name"].astype(str).str.strip() == "")
    missing_name_count = missing_name_mask.sum()
    if missing_name_count:
        logging.warning("Dropping %d products with missing product_name", missing_name_count)
        df = df[~missing_name_mask]

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    invalid_price_count = df["price"].isna().sum()
    if invalid_price_count:
        logging.warning("Found %d products with non-numeric price (including N/A)", invalid_price_count)

    negative_mask = df["price"] < 0
    negative_count = negative_mask.sum()
    if negative_count:
        logging.warning("Setting %d negative prices to NULL", negative_count)
        df.loc[negative_mask, "price"] = pd.NA

    logging.info("Clean products rows: %d", len(df))
    return df[["product_id", "product_name", "category", "price"]]


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== Transforming customers ===")
    logging.info("Raw customers rows (including duplicates): %d", len(df))

    df = df.replace({"": pd.NA})

    email_pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    df["email"] = df["email"].astype("string")

    invalid_email_mask = df["email"].notna() & ~df["email"].str.match(email_pattern)
    invalid_email_count = invalid_email_mask.sum()
    if invalid_email_count:
        logging.warning("Setting %d invalid emails to NULL", invalid_email_count)
        df.loc[invalid_email_mask, "email"] = pd.NA

    def choose_best_record(group: pd.DataFrame) -> pd.Series:
        if len(group) > 1:
            logging.warning(
                "Duplicate customer_id %s with %d records, resolving...",
                group["customer_id"].iloc[0],
                len(group),
            )
        scores = group[["name", "email", "country"]].notna().sum(axis=1)
        best_idx = scores.idxmax()
        return group.loc[best_idx]

    customers_dedup = (
        df.groupby("customer_id", as_index=False, group_keys=False)
        .apply(choose_best_record)
        .reset_index(drop=True)
    )

    logging.info("Customers after de-duplication: %d", len(customers_dedup))
    return customers_dedup[["customer_id", "name", "email", "country"]]


def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== Transforming sales ===")
    logging.info("Raw sales rows: %d", len(df))

    df = df.replace({"": pd.NA})

    missing_fk_mask = df["product_id"].isna() | df["customer_id"].isna()
    missing_fk_count = missing_fk_mask.sum()
    if missing_fk_count:
        logging.warning(
            "Dropping %d sales rows with missing product_id or customer_id",
            missing_fk_count,
        )
        df = df[~missing_fk_mask]

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    invalid_qty_mask = df["quantity"].isna() | (df["quantity"] <= 0)
    invalid_qty_count = invalid_qty_mask.sum()
    if invalid_qty_count:
        logging.warning(
            "Dropping %d sales rows with invalid quantity (<=0 or non-numeric)",
            invalid_qty_count,
        )
        df = df[~invalid_qty_mask]

    df["transaction_date_parsed"] = pd.to_datetime(
        df["transaction_date"], errors="coerce", infer_datetime_format=True
    )
    invalid_date_mask = df["transaction_date_parsed"].isna()
    invalid_date_count = invalid_date_mask.sum()
    if invalid_date_count:
        logging.warning(
            "Dropping %d sales rows with unparseable transaction_date",
            invalid_date_count,
        )
        df = df[~invalid_date_mask]

    df["transaction_date"] = df["transaction_date_parsed"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["transaction_date_parsed"])

    logging.info("Clean sales rows: %d", len(df))
    return df[["transaction_id", "product_id", "customer_id", "quantity", "transaction_date"]]



def ensure_state_table():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ex_3_etl_state (
                pipeline_name        VARCHAR(50) PRIMARY KEY,
                last_processed_date  DATE,
                last_run_ts          DATETIME,
                last_run_rows        INT
            )
        """))


def get_last_watermark():
    ensure_state_table()
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT last_processed_date FROM ex_3_etl_state WHERE pipeline_name = 'ex_3_sales'")
        )
        row = result.fetchone()
        if row is None:
            logging.info("No previous watermark found (first run, full load).")
            return None
        logging.info("Last watermark from state table: %s", row[0])
        return row[0]  


def update_state(new_watermark, rows_processed):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ex_3_etl_state (pipeline_name, last_processed_date, last_run_ts, last_run_rows)
                VALUES ('ex_3_sales', :wm, :ts, :rows)
                ON DUPLICATE KEY UPDATE
                    last_processed_date = VALUES(last_processed_date),
                    last_run_ts        = VALUES(last_run_ts),
                    last_run_rows      = VALUES(last_run_rows)
            """),
            {
                "wm": new_watermark,
                "ts": datetime.now(),
                "rows": int(rows_processed),
            },
        )



def load_incremental_sales(sales_clean: pd.DataFrame):
    logging.info("=== Incremental load for ex_3_sales ===")

    last_wm = get_last_watermark()

    
    sales_clean = sales_clean.copy()
    sales_clean["transaction_date_dt"] = pd.to_datetime(sales_clean["transaction_date"])

    if last_wm is None:
        mode = "full"
        inc_df = sales_clean
    else:
        mode = "incremental"
        inc_df = sales_clean[sales_clean["transaction_date_dt"] > pd.to_datetime(last_wm)]

    inc_df = inc_df.sort_values("transaction_date_dt")
    inc_df = inc_df[["transaction_id", "product_id", "customer_id", "quantity", "transaction_date"]]

    rows_to_load = len(inc_df)
    logging.info("Sales load mode: %s. Rows to load: %d", mode, rows_to_load)

    if rows_to_load == 0:
        
        update_state(last_wm, 0)
        logging.info("No new sales to load. Watermark unchanged.")
        return

   
    inc_df.to_sql("ex_3_sales_stage", engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ex_3_sales (
                transaction_id   VARCHAR(20),
                product_id       VARCHAR(10),
                customer_id      VARCHAR(10),
                quantity         INT,
                transaction_date DATE
            )
        """))

        conn.execute(text("""
            DELETE s
            FROM ex_3_sales s
            JOIN ex_3_sales_stage st
              ON s.transaction_id = st.transaction_id
        """))

        conn.execute(text("""
            INSERT INTO ex_3_sales (transaction_id, product_id, customer_id, quantity, transaction_date)
            SELECT transaction_id, product_id, customer_id, quantity, transaction_date
            FROM ex_3_sales_stage
        """))

        conn.execute(text("DROP TABLE ex_3_sales_stage"))


    new_wm_str = inc_df["transaction_date"].max()
    new_wm_date = pd.to_datetime(new_wm_str).date()
    update_state(new_wm_date, rows_to_load)
    logging.info("Updated watermark to %s", new_wm_date)



def load_to_mariadb_incremental(products: pd.DataFrame,
                                customers: pd.DataFrame,
                                sales: pd.DataFrame) -> None:
    logging.info("=== Loading ex_3_* tables into MariaDB ===")
    logging.info("Using DB: %s @ %s:%s", DB_NAME, DB_HOST, DB_PORT)


    products.to_sql(TABLE_PREFIX + "products", engine, if_exists="replace", index=False)
    customers.to_sql(TABLE_PREFIX + "customers", engine, if_exists="replace", index=False)
    logging.info("Loaded %d products into ex_3_products", len(products))
    logging.info("Loaded %d customers into ex_3_customers", len(customers))


    load_incremental_sales(sales)


def main():
    logging.info("=== ETL pipeline (Exercise 3 - incremental) started ===")

    products_path = DATA_DIR / "products.csv"
    customers_path = DATA_DIR / "customers.csv"
    sales_path = DATA_DIR / "sales.csv"

    logging.info("Reading CSV files from %s", DATA_DIR)
    products_raw = pd.read_csv(products_path)
    customers_raw = pd.read_csv(customers_path)
    sales_raw = pd.read_csv(sales_path)

    products_clean = transform_products(products_raw)
    customers_clean = transform_customers(customers_raw)
    sales_clean = transform_sales(sales_raw)

    load_to_mariadb_incremental(products_clean, customers_clean, sales_clean)

    logging.info("=== ETL pipeline finished successfully ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("ETL pipeline failed with an error: %s", e)
        raise
