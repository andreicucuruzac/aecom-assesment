#!/usr/bin/env python
import logging
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_PATH = BASE_DIR / "etl_ex5.log"

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

TABLE_PREFIX = "ex_5_"

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



def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_5] Cleaning products ===")
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
        logging.warning("[products] %d rows with unknown category; setting to 'Unknown'", unknown_cats)
        df.loc[df["category"].isna(), "category"] = "Unknown"


    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip()

    crit_missing_mask = (df["product_id"] == "") | (df["product_name"] == "")
    crit_missing_count = crit_missing_mask.sum()
    if crit_missing_count:
        logging.error(
            "[products] %d rows with missing critical fields (product_id/product_name) will be dropped",
            crit_missing_count,
        )
        df = df[~crit_missing_mask]


    df["price_raw"] = df["price"]
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    invalid_type_mask = df["price"].isna() & df["price_raw"].notna()
    invalid_type_count = invalid_type_mask.sum()
    if invalid_type_count:
        logging.warning(
            "[products] %d rows with invalid price data type (e.g. text) -> price set to NULL",
            invalid_type_count,
        )

    negative_mask = df["price"] < 0
    negative_count = negative_mask.sum()
    if negative_count:
        logging.warning(
            "[products] %d rows with negative price; price set to NULL",
            negative_count,
        )
        df.loc[negative_mask, "price"] = pd.NA

    df = df.drop(columns=["price_raw"])

    logging.info("Clean products rows: %d", len(df))
    return df[["product_id", "product_name", "category", "price"]]



def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_5] Cleaning customers ===")
    logging.info("Raw customers rows (including duplicates): %d", len(df))

    df = df.replace({"": pd.NA})
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()


    crit_missing_mask = df["customer_id"].isna() | (df["customer_id"] == "") | (df["name"] == "")
    crit_missing_count = crit_missing_mask.sum()
    if crit_missing_count:
        logging.error(
            "[customers] %d rows with missing critical fields (customer_id/name) will be dropped",
            crit_missing_count,
        )
        df = df[~crit_missing_mask]


    df["email"] = df["email"].astype("string")
    email_pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    invalid_email_mask = df["email"].notna() & ~df["email"].str.match(email_pattern)
    invalid_email_count = invalid_email_mask.sum()
    if invalid_email_count:
        logging.warning(
            "[customers] %d rows with invalid email format; email set to NULL",
            invalid_email_count,
        )
        df.loc[invalid_email_mask, "email"] = pd.NA


    def choose_best_record(group: pd.DataFrame) -> pd.Series:
        if len(group) > 1:
            logging.warning(
                "[customers] Duplicate customer_id %s with %d records; resolving...",
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

    logging.info("Clean customers rows after de-duplication: %d", len(customers_dedup))
    return customers_dedup[["customer_id", "name", "email", "country"]]



def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("=== [ex_5] Cleaning sales ===")
    logging.info("Raw sales rows: %d", len(df))

    df = df.replace({"": pd.NA})


    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    crit_missing_mask = (
        (df["transaction_id"] == "") |
        df["product_id"].isna() | (df["product_id"] == "") |
        df["customer_id"].isna() | (df["customer_id"] == "") |
        df["transaction_date"].isna()
    )
    crit_missing_count = crit_missing_mask.sum()
    if crit_missing_count:
        logging.error(
            "[sales] %d rows with missing critical fields will be dropped",
            crit_missing_count,
        )
        df = df[~crit_missing_mask]


    df["quantity_raw"] = df["quantity"]
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    invalid_qty_type_mask = df["quantity"].isna()
    invalid_qty_type_count = invalid_qty_type_mask.sum()
    if invalid_qty_type_count:
        logging.error(
            "[sales] %d rows with invalid quantity data type (e.g. text); dropping these rows",
            invalid_qty_type_count,
        )
        df = df[~invalid_qty_type_mask]

    non_positive_mask = df["quantity"] <= 0
    non_positive_count = non_positive_mask.sum()
    if non_positive_count:
        logging.error(
            "[sales] %d rows with non-positive quantity (<=0); dropping these rows",
            non_positive_count,
        )
        df = df[~non_positive_mask]

    df = df.drop(columns=["quantity_raw"])


    df["transaction_date_parsed"] = pd.to_datetime(
        df["transaction_date"], errors="coerce", infer_datetime_format=True
    )
    invalid_date_mask = df["transaction_date_parsed"].isna()
    invalid_date_count = invalid_date_mask.sum()
    if invalid_date_count:
        logging.error(
            "[sales] %d rows with invalid transaction_date format; dropping these rows",
            invalid_date_count,
        )
        df = df[~invalid_date_mask]

    df["transaction_date"] = df["transaction_date_parsed"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["transaction_date_parsed"])

    logging.info("Clean sales rows after basic validation: %d", len(df))
    return df[["transaction_id", "product_id", "customer_id", "quantity", "transaction_date"]]



def validate_referential_integrity(
    sales_df: pd.DataFrame,
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split sales into valid and rejected based on referential integrity:
    - product_id must exist in products_df.product_id
    - customer_id must exist in customers_df.customer_id

    Returns: (sales_valid, sales_rejected_with_reason)
    """
    logging.info("=== [ex_5] Checking referential integrity for sales ===")

    valid_products = set(products_df["product_id"])
    valid_customers = set(customers_df["customer_id"])

    prod_ok = sales_df["product_id"].isin(valid_products)
    cust_ok = sales_df["customer_id"].isin(valid_customers)

    referential_ok = prod_ok & cust_ok
    rejected_mask = ~referential_ok

    rejected_df = sales_df[rejected_mask].copy()
    valid_df = sales_df[referential_ok].copy()


    rejected_df["reject_reason"] = ""
    rejected_df.loc[~prod_ok[rejected_mask], "reject_reason"] += "INVALID_PRODUCT_ID;"
    rejected_df.loc[~cust_ok[rejected_mask], "reject_reason"] += "INVALID_CUSTOMER_ID;"

    rejected_count = len(rejected_df)
    if rejected_count:
        logging.error(
            "[sales] Referential integrity violations: %d rows with non-existent product_id/customer_id",
            rejected_count,
        )

    logging.info("Sales rows valid after referential check: %d", len(valid_df))
    return valid_df, rejected_df



def load_to_mariadb(
    products: pd.DataFrame,
    customers: pd.DataFrame,
    sales_valid: pd.DataFrame,
    sales_rejected: pd.DataFrame,
) -> None:
    logging.info("=== [ex_5] Loading cleaned data into MariaDB ===")
    logging.info("DB: %s @ %s:%s", DB_NAME, DB_HOST, DB_PORT)


    products.to_sql(TABLE_PREFIX + "products", engine, if_exists="replace", index=False)
    customers.to_sql(TABLE_PREFIX + "customers", engine, if_exists="replace", index=False)
    sales_valid.to_sql(TABLE_PREFIX + "sales", engine, if_exists="replace", index=False)

    logging.info(
        "[load] ex_5_products: %d rows, ex_5_customers: %d rows, ex_5_sales (valid): %d rows",
        len(products),
        len(customers),
        len(sales_valid),
    )


    if len(sales_rejected) > 0:
        sales_rejected.to_sql(
            TABLE_PREFIX + "sales_rejected",
            engine,
            if_exists="replace",
            index=False,
        )
        logging.info(
            "[load] ex_5_sales_rejected: %d rows with validation / referential errors",
            len(sales_rejected),
        )
    else:
        logging.info("[load] No rejected sales rows to store.")



def main():
    logging.info("=== [ex_5] ETL pipeline with validation started ===")

    products_path = DATA_DIR / "products.csv"
    customers_path = DATA_DIR / "customers.csv"
    sales_path = DATA_DIR / "sales.csv"


    for p in [products_path, customers_path, sales_path]:
        if not p.exists():
            logging.error("Required input file not found: %s", p)
            raise FileNotFoundError(p)


    logging.info("Reading CSV files from %s", DATA_DIR)
    products_raw = pd.read_csv(products_path)
    customers_raw = pd.read_csv(customers_path)
    sales_raw = pd.read_csv(sales_path)


    products_clean = clean_products(products_raw)
    customers_clean = clean_customers(customers_raw)
    sales_clean = clean_sales(sales_raw)


    sales_valid, sales_rejected = validate_referential_integrity(
        sales_clean, products_clean, customers_clean
    )


    load_to_mariadb(products_clean, customers_clean, sales_valid, sales_rejected)

    logging.info("=== [ex_5] ETL pipeline finished successfully ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("[ex_5] ETL pipeline failed with an error: %s", e)
        raise
