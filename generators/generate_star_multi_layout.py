"""Generate mock star-schema multi-layout pipe-delimited data file."""

import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker
from loguru import logger

fake = Faker("en-NZ")

NZ_REGIONS = [
    "Northland",
    "Auckland",
    "Waikato",
    "Bay of Plenty",
    "Gisborne",
    "Hawke's Bay",
    "Taranaki",
    "Manawatū-Whanganui",
    "Wellington",
    "Tasman",
    "Nelson",
    "Marlborough",
    "West Coast",
    "Canterbury",
    "Otago",
    "Southland",
]

# Ensure output directory exists relative to project root
PROJECT_ROOT = Path(__file__).parent.parent
OUT_FILE = PROJECT_ROOT / "data" / "input" / "star_multi_layout_data.txt"


def generate_dates(start_date: date, days: int):
    """Generate DimDate records (Type 10)."""
    dates = []
    for i in range(days):
        current_date = start_date + timedelta(days=i)
        date_key = int(current_date.strftime("%Y%m%d"))

        if not date_key:
            raise ValueError("Key field 'date_key' cannot be null")

        record = [
            "10",  # RecordType
            str(date_key),
            current_date.isoformat(),
            str(current_date.day),
            current_date.strftime("%A"),
            str(current_date.month),
            current_date.strftime("%B"),
            str((current_date.month - 1) // 3 + 1),
            str(current_date.year),
            str(current_date.weekday() >= 5).lower(),
        ]
        dates.append(record)
    return dates


def generate_customers(count: int):
    """Generate DimCustomer records (Type 20)."""
    customers = []
    for i in range(1, count + 1):
        customer_key = str(i)
        customer_id = f"CUST-{i:04d}"

        if not customer_key or not customer_id:
            raise ValueError(
                "Key fields 'customer_key' and 'customer_id' cannot be null"
            )

        record = [
            "20",  # RecordType
            customer_key,
            customer_id,
            fake.name(),
            fake.email(),
            fake.city(),
            random.choice(NZ_REGIONS),
            "New Zealand",
            random.choice(["Retail", "Wholesale", "Corporate"]),
        ]
        customers.append(record)
    return customers


def generate_products(count: int):
    """Generate DimProduct records (Type 30)."""
    products = []
    categories = {
        "Electronics": ["Phones", "Laptops", "Accessories"],
        "Home": ["Furniture", "Kitchen", "Decor"],
        "Clothing": ["Menswear", "Womenswear", "Kids"],
    }
    brands = ["Fisher & Paykel", "Icebreaker", "Kathmandu", "Swanndri"]
    colors = ["Red", "Blue", "Green", "Black", "White"]

    for i in range(1, count + 1):
        product_key = str(i)
        product_id = f"PROD-{i:04d}"

        if not product_key or not product_id:
            raise ValueError("Key fields 'product_key' and 'product_id' cannot be null")

        cat = random.choice(list(categories.keys()))
        subcat = random.choice(categories[cat])
        cost = round(random.uniform(5.0, 500.0), 2)
        record = [
            "30",  # RecordType
            product_key,
            product_id,
            fake.catch_phrase(),
            cat,
            subcat,
            random.choice(brands),
            random.choice(colors),
            f"{cost:.2f}",
        ]
        products.append(record)
    return products


def generate_stores(count: int):
    """Generate DimStore records (Type 40)."""
    stores = []
    for i in range(1, count + 1):
        store_key = str(i)
        store_id = f"STORE-{i:03d}"

        if not store_key or not store_id:
            raise ValueError("Key fields 'store_key' and 'store_id' cannot be null")

        record = [
            "40",  # RecordType
            store_key,
            store_id,
            fake.company() + " Store",
            fake.city(),
            random.choice(NZ_REGIONS),
            "New Zealand",
        ]
        stores.append(record)
    return stores


def generate_sales(count: int, date_keys, customer_keys, product_keys, store_keys):
    """Generate FactSales records (Type 50)."""
    sales = []
    for i in range(1, count + 1):
        sales_key = str(i)
        date_key = str(random.choice(date_keys))
        cust_key = str(random.choice(customer_keys))
        prod_key = str(random.choice(product_keys))
        store_key = str(random.choice(store_keys))

        if not all([sales_key, date_key, cust_key, prod_key, store_key]):
            raise ValueError("Key fields in FactSales cannot be null")

        qty = random.randint(1, 10)
        unit_price = round(random.uniform(10.0, 1000.0), 2)
        total = round(qty * unit_price, 2)
        record = [
            "50",  # RecordType
            sales_key,
            date_key,
            cust_key,
            prod_key,
            store_key,
            str(qty),
            f"{unit_price:.2f}",
            f"{total:.2f}",
        ]
        sales.append(record)
    return sales


def main():
    logger.info("Generating star schema multi-layout data...")

    # 1. Prep dimensions
    dates = generate_dates(date(2024, 1, 1), 366)
    customers = generate_customers(200)
    products = generate_products(100)
    stores = generate_stores(20)

    date_keys = [int(d[1]) for d in dates]
    customer_keys = [int(c[1]) for c in customers]
    product_keys = [int(p[1]) for p in products]
    store_keys = [int(s[1]) for s in stores]

    # 2. Prep 10k facts
    sales = generate_sales(100_000, date_keys, customer_keys, product_keys, store_keys)

    # 3. Combine and shuffle records
    all_records = dates + customers + products + stores + sales
    random.shuffle(all_records)

    # 4. Count records for trailer
    counts = {}
    for row in all_records:
        rt = row[0]
        counts[rt] = counts.get(rt, 0) + 1

    # 5. Save to file
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    filename = OUT_FILE.name
    created_at = datetime.now(UTC).isoformat(timespec="seconds")

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        # Header
        f.write(f"H|{filename}|{created_at}\n")

        # Data
        for row in all_records:
            f.write("|".join(row) + "\n")

        # Trailer
        trailer_parts = [f"{rt}-{count:010d}" for rt, count in sorted(counts.items())]
        f.write("T|" + "|".join(trailer_parts) + "\n")

    logger.success(
        f"Successfully generated {len(all_records)} data records with H/T to {OUT_FILE}"
    )


if __name__ == "__main__":
    main()
