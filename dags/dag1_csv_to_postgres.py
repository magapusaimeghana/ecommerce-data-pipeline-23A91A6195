from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


# ---------------- DAG CONFIG ----------------
dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "postgres", "csv"]
)


# ---------------- TASK 1 ----------------
def create_employee_table():
    """
    Creates raw_employee_data table if it does not exist.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE
    );
    """

    hook.run(create_table_sql)


# ---------------- TASK 2 ----------------
def truncate_employee_table():
    """
    Truncates the table to ensure idempotency.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("TRUNCATE TABLE raw_employee_data;")


# ---------------- TASK 3 ----------------
def load_csv_to_postgres():
    """
    Loads CSV data into raw_employee_data table.

    Returns:
        int: number of rows inserted
    """
    csv_path = "/opt/airflow/data/input.csv"

    # Read CSV
    df = pd.read_csv(csv_path)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Load to Postgres
    df.to_sql(
        name="raw_employee_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    return len(df)


# ---------------- OPERATORS ----------------
create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag
)

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres,
    dag=dag
)


# ---------------- DEPENDENCIES ----------------
create_table_task >> truncate_table_task >> load_csv_task
