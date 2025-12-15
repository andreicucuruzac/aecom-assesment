#!/usr/bin/env python
import logging
import re
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_PATH = BASE_DIR / "etl.log"

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

TABLE_PREFIX = "ex_2_"

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



def load_to_mariadb(products: pd.DataFrame, customers: pd.DataFrame, sales: pd.DataFrame) -> None:
    logging.info("=== Loading into MariaDB (replace mode, no FKs) ===")
    logging.info("Using DB: %s @ %s:%s", DB_NAME, DB_HOST, DB_PORT)

    # Overwrite tables each run; pandas creates the table structure
    products.to_sql(TABLE_PREFIX + "products", engine, if_exists="replace", index=False)
    customers.to_sql(TABLE_PREFIX + "customers", engine, if_exists="replace", index=False)
    sales.to_sql(TABLE_PREFIX + "sales", engine, if_exists="replace", index=False)

    logging.info(
        "Loaded %d products, %d customers, %d sales rows into DB",
        len(products), len(customers), len(sales)
    )



def main():
    logging.info("=== ETL pipeline (Exercise 2, simple) started ===")

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

    load_to_mariadb(products_clean, customers_clean, sales_clean)

    logging.info("=== ETL pipeline finished successfully ===")


if __name__ == "__main__":
    main()
