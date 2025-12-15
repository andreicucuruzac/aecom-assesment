#!/usr/bin/env python
import logging
import re
import time
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests
from sqlalchemy import create_engine


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_PATH = BASE_DIR / "etl_ex6.log"

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

TABLE_PREFIX = "ex_6_"

ENGINE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(ENGINE_URL)


MOCK_API_BASE_URL = "http://localhost:3000/products_metadata"

REQUEST_TIMEOUT = 5   
MAX_RETRIES = 3
BACKOFF_FACTOR = 2   


CANONICAL_CATEGORIES = {
    "electronics": "Electronics",
    "clothing": "Clothing",
    "books": "Books",
    "home": "Home",
    "sports": "Sports",
}


def fetch_all_product_metadata() -> pd.DataFrame:
    """
    Fetch all product metadata from Mockoon API.

    Expected JSON format (array):
    [
      {
        "product_id": "P0001",
        "description": "...",
        "rating": 4.3,
        "availability_status": "In Stock"
      },
      ...
    ]
    """
    logging.info("=== [ex_6] Fetching product metadata from Mockoon ===")
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(MOCK_API_BASE_URL, timeout=REQUEST_TIMEOUT)
        except requests.Timeout:
            attempt += 1
            wait = BACKOFF_FACTOR ** attempt
            logging.warning(
                "[API] Timeout calling %s. Retry %d/%d in %ds",
                MOCK_API_BASE_URL, attempt, MAX_RETRIES, wait
            )
            time.sleep(wait)
            continue
        except requests.RequestException as e:
            logging.error("[API] Error calling Mockoon: %s", e)
            return pd.DataFrame()

        if resp.status_code != 200:
            attempt += 1
            if attempt >= MAX_RETRIES:
                logging.error(
                    "[API] Mockoon returned HTTP %d after %d attempts: %s",
                    resp.status_code, attempt, resp.text
                )
                return pd.DataFrame()
            wait = BACKOFF_FACTOR ** attempt
            logging.warning(
                "[API] Mockoon HTTP %d. Retry %d/%d in %ds",
                resp.status_code, attempt, MAX_RETRIES, wait
            )
            time.sleep(wait)
            continue


        try:
            data = resp.json()
        except ValueError:
            logging.error("[API] Could not parse JSON from Mockoon")
            return pd.DataFrame()

        if not isinstance(data, list):
            logging.error("[API] Expected JSON array but got %s", type(data))
            return pd.DataFrame()

        api_df = pd.json_normalize(data)


        for col in ["product_id", "description", "rating", "availability_status"]:
            if col not in api_df.columns:
                logging.warning("[API] Missing field '%s' in Mockoon records, filling NULL", col)
                api_df[col] = pd.NA


        api_df["product_id"] = api_df["product_id"].astype(str).str.strip()
        api_df["description"] = api_df["description"].astype("string")
        api_df["rating"] = pd.to_numeric(api_df["rating"], errors="coerce")
        api_df["availability_status"] = api_df["availability_status"].astype("string")


        out_of_bounds = (api_df["rating"] < 0) | (api_df["rating"] > 5)
        if out_of_bounds.any():
            count_oob = out_of_bounds.sum()
            logging.warning("[API] %d ratings outside 0–5; clamping", count_oob)
            api_df.loc[out_of_bounds & api_df["rating"].notna(), "rating"] = api_df["rating"].clip(0, 5)


        api_df = (
            api_df.sort_index()
            .drop_duplicates(subset=["product_id"], keep="last")
            .reset_index(drop=True)
        )

        logging.info("[API] Retrieved %d metadata rows from Mockoon", len(api_df))
        return api_df[["product_id", "description", "rating", "availability_status"]]


    logging.error("[API] Failed to fetch metadata from Mockoon after %d attempts", MAX_RETRIES)
    return pd.DataFrame()


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_6] Cleaning products (CSV) ===")
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip()

    df["category"] = (
        df["category"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map(CANONICAL_CATEGORIES)
    )
    df.loc[df["category"].isna(), "category"] = "Unknown"

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df.loc[df["price"] < 0, "price"] = pd.NA

    crit_missing = (df["product_id"] == "") | (df["product_name"] == "")
    dropped = crit_missing.sum()
    if dropped:
        logging.warning("[ex_6/products] Dropping %d rows with missing critical fields", dropped)
        df = df[~crit_missing]

    logging.info("[ex_6/products] Clean rows: %d", len(df))
    return df[["product_id", "product_name", "category", "price"]]


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_6] Cleaning customers (CSV) ===")
    df = df.replace({"": pd.NA})
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()

    email_pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    df["email"] = df["email"].astype("string")
    invalid_email = df["email"].notna() & ~df["email"].str.match(email_pattern)
    if invalid_email.any():
        logging.warning("[ex_6/customers] %d invalid emails set to NULL", invalid_email.sum())
        df.loc[invalid_email, "email"] = pd.NA

    df = (
        df.sort_index()
        .drop_duplicates(subset=["customer_id"], keep="first")
        .reset_index(drop=True)
    )

    logging.info("[ex_6/customers] Clean rows: %d", len(df))
    return df[["customer_id", "name", "email", "country"]]


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_6] Cleaning sales (CSV) ===")
    df = df.replace({"": pd.NA})

    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    crit_missing = (
        (df["transaction_id"] == "") |
        df["product_id"].isna() | (df["product_id"] == "") |
        df["customer_id"].isna() | (df["customer_id"] == "")
    )
    if crit_missing.any():
        logging.warning("[ex_6/sales] Dropping %d rows with missing critical fields", crit_missing.sum())
        df = df[~crit_missing]

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    invalid_qty = df["quantity"].isna() | (df["quantity"] <= 0)
    if invalid_qty.any():
        logging.warning("[ex_6/sales] Dropping %d rows with invalid quantity", invalid_qty.sum())
        df = df[~invalid_qty]

    df["transaction_date_parsed"] = pd.to_datetime(
        df["transaction_date"], errors="coerce", infer_datetime_format=True
    )
    invalid_date = df["transaction_date_parsed"].isna()
    if invalid_date.any():
        logging.warning("[ex_6/sales] Dropping %d rows with invalid transaction_date", invalid_date.sum())
        df = df[~invalid_date]

    df["transaction_date"] = df["transaction_date_parsed"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["transaction_date_parsed"])

    logging.info("[ex_6/sales] Clean rows: %d", len(df))
    return df[["transaction_id", "product_id", "customer_id", "quantity", "transaction_date"]]


def enrich_products_with_api(products_df: pd.DataFrame,
                             api_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_6] Enriching products with API metadata ===")

    if api_df.empty:
        logging.warning("[ex_6] No API metadata available. Returning base products only.")
        products_df["description"] = pd.NA
        products_df["rating"] = pd.NA
        products_df["availability_status"] = pd.NA
        return products_df

    merged = products_df.merge(
        api_df,
        on="product_id",
        how="left",
        suffixes=("", "_api"),
    )

    total = len(merged)
    enriched_count = merged["description"].notna().sum()
    logging.info(
        "[ex_6] API metadata matched %d/%d products (%.1f%%)",
        enriched_count,
        total,
        (enriched_count / total * 100) if total else 0.0,
    )


    missing_desc = merged["description"].isna().sum()
    if missing_desc:
        logging.warning("[ex_6] %d products without description; using default text.", missing_desc)
        merged.loc[merged["description"].isna(), "description"] = "No description available."

    missing_rating = merged["rating"].isna().sum()
    if missing_rating:
        logging.warning("[ex_6] %d products without rating; leaving rating as NULL.", missing_rating)

    missing_avail = merged["availability_status"].isna().sum()
    if missing_avail:
        logging.warning("[ex_6] %d products without availability_status; setting to 'Unknown'.", missing_avail)
        merged.loc[merged["availability_status"].isna(), "availability_status"] = "Unknown"

    return merged[
        [
            "product_id",
            "product_name",
            "category",
            "price",
            "description",
            "rating",
            "availability_status",
        ]
    ]


def load_to_mariadb(products_enriched: pd.DataFrame,
                    customers: pd.DataFrame,
                    sales: pd.DataFrame) -> None:
    logging.info("=== [ex_6] Loading data into MariaDB ===")
    logging.info("DB: %s @ %s:%s", DB_NAME, DB_HOST, DB_PORT)

    products_enriched.to_sql(TABLE_PREFIX + "products", engine, if_exists="replace", index=False)
    customers.to_sql(TABLE_PREFIX + "customers", engine, if_exists="replace", index=False)
    sales.to_sql(TABLE_PREFIX + "sales", engine, if_exists="replace", index=False)

    logging.info(
        "[load] ex_6_products: %d, ex_6_customers: %d, ex_6_sales: %d",
        len(products_enriched), len(customers), len(sales),
    )


def main():
    logging.info("=== [ex_6] ETL pipeline with Mockoon integration started ===")

    products_path = DATA_DIR / "products.csv"
    customers_path = DATA_DIR / "customers.csv"
    sales_path = DATA_DIR / "sales.csv"

    for p in [products_path, customers_path, sales_path]:
        if not p.exists():
            logging.error("Input file not found: %s", p)
            raise FileNotFoundError(p)

    logging.info("Reading CSV files from %s", DATA_DIR)
    products_raw = pd.read_csv(products_path)
    customers_raw = pd.read_csv(customers_path)
    sales_raw = pd.read_csv(sales_path)

    products_clean = clean_products(products_raw)
    customers_clean = clean_customers(customers_raw)
    sales_clean = clean_sales(sales_raw)

    api_metadata_df = fetch_all_product_metadata()
    products_enriched = enrich_products_with_api(products_clean, api_metadata_df)

    load_to_mariadb(products_enriched, customers_clean, sales_clean)

    logging.info("=== [ex_6] ETL pipeline finished successfully ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("[ex_6] ETL pipeline failed with an error: %s", e)
        raise
