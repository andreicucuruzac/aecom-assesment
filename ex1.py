import random
import csv
from pathlib import Path
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

N_PRODUCTS = 150
N_CUSTOMERS = 600
N_SALES = 1500
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def generate_products(path=None):
    if path is None:
        path = DATA_DIR / "products.csv"

    PRODUCT_NAME_TEMPLATES = {
        "Electronics": [
            "Wireless Mouse", "Wired Mouse", "Bluetooth Speaker", "USB Keyboard",
            "Gaming Keyboard", "Wireless Headphones", "Wired Headphones",
            "Portable Charger", "Smartwatch", "Webcam HD", "LED Monitor"
        ],
        "Clothing": [
            "Cotton T-Shirt", "Running Shoes", "Leather Jacket", "Sports Shorts",
            "Wool Sweater", "Baseball Cap", "Raincoat", "Denim Jeans"
        ],
        "Books": [
            "Paperback Book", "Hardcover Book", "Science Fiction Novel",
            "Fantasy Novel", "History Book", "Cookbook", "Programming Guide"
        ],
        "Home": [
            "Stainless Steel Bottle", "Ceramic Mug", "Non-stick Pan",
            "Vacuum Cleaner", "Air Purifier", "Table Lamp", "Throw Pillow"
        ],
        "Sports": [
            "Yoga Mat", "Basketball", "Tennis Racket", "Football",
            "Hiking Backpack", "Cycling Helmet", "Jump Rope"
        ]
    }

    rows = []
    for i in range(1, N_PRODUCTS + 1):
        product_id = f"P{i:04d}"


        category = random.choice(PRODUCT_CATEGORIES)


        name = random.choice(PRODUCT_NAME_TEMPLATES[category])


        price = round(random.uniform(5, 500), 2)

        if random.random() < 0.08:
            name = ""

        if random.random() < 0.20:
            category = random.choice([
                category.lower(),    
                category.upper(),    
                category.title()     
            ])


        if random.random() < 0.05:
            price = -abs(price)
        elif random.random() < 0.05:
            price = "N/A"

        rows.append([product_id, name, category, price])

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "price"])
        writer.writerows(rows)



def generate_customers(path=None):
    if path is None:
        path = DATA_DIR / "customers.csv"

    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        customer_id = f"C{i:04d}"
        name = fake.name()
        email = fake.unique.email()
        country = random.choice(["United States", "Canada", "Germany", "France", "Romania", ""])


        if random.random() < 0.05:
            email = email.replace("@", "")  

        rows.append([customer_id, name, email, country])


        if random.random() < 0.05:
            dup_name = fake.name()
            dup_email = fake.email() 
            rows.append([customer_id, dup_name, dup_email, country])

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "email", "country"])
        writer.writerows(rows)


def random_date(start, end):
    """Return random datetime between start and end"""
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def generate_sales(path=None):
    if path is None:
        path = DATA_DIR / "sales.csv"

    rows = []

    start = datetime(2024, 1, 1)
    end = datetime(2025, 12, 1)

    for i in range(1, N_SALES + 1):
        transaction_id = f"T{i:06d}"
        product_id = f"P{random.randint(1, N_PRODUCTS):04d}"
        customer_id = f"C{random.randint(1, N_CUSTOMERS):04d}"
        quantity = random.randint(1, 10)
        tx_date = random_date(start, end)


        if random.random() < 0.03:
            product_id = ""
        if random.random() < 0.03:
            customer_id = ""

        if random.random() < 0.05:
            quantity = 0
        elif random.random() < 0.03:
            quantity = -quantity


        if random.random() < 0.05:

            date_str = tx_date.strftime("%Y/%m/%d")
        elif random.random() < 0.05:
            date_str = tx_date.strftime("%b %d %Y")  
        else:
            date_str = tx_date.strftime("%Y-%m-%d")

        rows.append([transaction_id, product_id, customer_id, quantity, date_str])

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id", "product_id", "customer_id",
            "quantity", "transaction_date"
        ])
        writer.writerows(rows)


if __name__ == "__main__":
    generate_products()
    generate_customers()
    generate_sales()
