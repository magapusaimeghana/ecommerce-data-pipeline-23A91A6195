import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime, timedelta
import yaml
import os

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)


# -----------------------------
# Utility: Load config
# -----------------------------
def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


# -----------------------------
# Mandatory Functions
# -----------------------------

def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []
    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]

    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"user{i}@example.com",
            "phone": fake.msisdn()[:10],
            "registration_date": fake.date_between(start_date="-2y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(age_groups)
        })

    return pd.DataFrame(customers)


def generate_products(num_products: int) -> pd.DataFrame:
    categories = {
        "Electronics": ["Mobile", "Laptop", "Accessories"],
        "Clothing": ["Men", "Women"],
        "Home & Kitchen": ["Appliances", "Furniture"],
        "Books": ["Education", "Fiction"],
        "Sports": ["Indoor", "Outdoor"],
        "Beauty": ["Skincare", "Cosmetics"]
    }

    products = []

    for i in range(1, num_products + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        price = round(random.uniform(100, 50000), 2)
        cost = round(price * random.uniform(0.6, 0.85), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1,50):03d}"
        })

    return pd.DataFrame(products)


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
    transactions = []

    customer_ids = customers_df["customer_id"].tolist()
    start_date = datetime(2024, 1, 1)

    for i in range(1, num_transactions + 1):
        txn_date = start_date + timedelta(days=random.randint(0, 364))
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": txn_date.date(),
            "transaction_time": txn_date.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0  # calculated later
        })

    return pd.DataFrame(transactions)


def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    items = []
    item_id = 1

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        chosen_products = products_df.sample(num_items)

        txn_total = 0.0

        for _, prod in chosen_products.iterrows():
            quantity = random.randint(1, 4)
            discount = random.choice([0, 5, 10, 15, 20])
            line_total = round(quantity * prod["price"] * (1 - discount / 100), 2)

            items.append({
                "item_id": f"ITEM{item_id:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod["product_id"],
                "quantity": quantity,
                "unit_price": prod["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            txn_total += line_total
            item_id += 1

        transactions_df.loc[
            transactions_df["transaction_id"] == txn["transaction_id"],
            "total_amount"
        ] = round(txn_total, 2)

    return pd.DataFrame(items)


def validate_referential_integrity(customers, products, transactions, items) -> dict:
    issues = {
        "orphan_customers": 0,
        "orphan_products": 0,
        "orphan_transactions": 0
    }

    issues["orphan_transactions"] = (~transactions["customer_id"].isin(customers["customer_id"])).sum()
    issues["orphan_products"] = (~items["product_id"].isin(products["product_id"])).sum()
    issues["orphan_transactions"] += (~items["transaction_id"].isin(transactions["transaction_id"])).sum()

    score = 100 if sum(issues.values()) == 0 else 80

    return {
        "orphan_records": issues,
        "data_quality_score": score
    }


# -----------------------------
# Script Execution
# -----------------------------
if __name__ == "__main__":
    config = load_config()
    raw_path = "data/raw"
    os.makedirs(raw_path, exist_ok=True)

    customers_df = generate_customers(config["data_generation"]["customers"])
    products_df = generate_products(config["data_generation"]["products"])
    transactions_df = generate_transactions(
        config["data_generation"]["transactions"], customers_df
    )
    items_df = generate_transaction_items(transactions_df, products_df)

    customers_df.to_csv(f"{raw_path}/customers.csv", index=False)
    products_df.to_csv(f"{raw_path}/products.csv", index=False)
    transactions_df.to_csv(f"{raw_path}/transactions.csv", index=False)
    items_df.to_csv(f"{raw_path}/transaction_items.csv", index=False)

    integrity_report = validate_referential_integrity(
        customers_df, products_df, transactions_df, items_df
    )

    metadata = {
        "generated_at": datetime.now().isoformat(),
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions_df),
            "transaction_items": len(items_df)
        },
        "integrity_check": integrity_report
    }

    def convert_types(obj):
        if isinstance(obj, dict):
            return {k: convert_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_types(i) for i in obj]
        elif hasattr(obj, "item"):  # numpy types
            return obj.item()
        else:
            return obj


    with open("data/raw/generation_metadata.json", "w") as f:
        json.dump(convert_types(metadata), f, indent=4)


    print("✅ Data generation completed successfully")
