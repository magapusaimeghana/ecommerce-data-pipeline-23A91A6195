import psycopg2
import os
from dotenv import load_dotenv

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "password")
}

DATA_PATH = "data/raw"


# -------------------------------------------------
# Database connection
# -------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# -------------------------------------------------
# COPY helper (explicit columns – IMPORTANT)
# -------------------------------------------------
def copy_csv(cursor, table_name, columns, file_path):
    cols = ", ".join(columns)
    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            sql=f"""
                COPY {table_name} ({cols})
                FROM STDIN
                WITH CSV HEADER
            """,
            file=f
        )


# -------------------------------------------------
# Load all CSVs into staging schema
# -------------------------------------------------
def load_all():
    conn = get_connection()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:

            print("🔄 Loading customers...")
            copy_csv(
                cur,
                "staging.customers",
                [
                    "customer_id",
                    "first_name",
                    "last_name",
                    "email",
                    "phone",
                    "registration_date",
                    "city",
                    "state",
                    "country",
                    "age_group"
                ],
                f"{DATA_PATH}/customers.csv"
            )

            print("🔄 Loading products...")
            copy_csv(
                cur,
                "staging.products",
                [
                    "product_id",
                    "product_name",
                    "category",
                    "sub_category",
                    "price",
                    "cost",
                    "brand",
                    "stock_quantity",
                    "supplier_id"
                ],
                f"{DATA_PATH}/products.csv"
            )

            print("🔄 Loading transactions...")
            copy_csv(
                cur,
                "staging.transactions",
                [
                    "transaction_id",
                    "customer_id",
                    "transaction_date",
                    "transaction_time",
                    "payment_method",
                    "shipping_address",
                    "total_amount"
                ],
                f"{DATA_PATH}/transactions.csv"
            )

            print("🔄 Loading transaction items...")
            copy_csv(
                cur,
                "staging.transaction_items",
                [
                    "item_id",
                    "transaction_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                    "discount_percentage",
                    "line_total"
                ],
                f"{DATA_PATH}/transaction_items.csv"
            )

        conn.commit()
        print("✅ All data loaded successfully into staging schema")

    except Exception as e:
        conn.rollback()
        print("❌ Error during loading:", e)
        raise

    finally:
        conn.close()


# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    load_all()
