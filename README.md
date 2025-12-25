# E-Commerce Data Pipeline & Analytics Project
## Project Overview

This project implements an end-to-end e-commerce data pipeline using Python, PostgreSQL, Docker, and Power BI.
The pipeline covers data generation, ingestion, transformation, warehousing, and analytics, following industry-standard data engineering practices.

The final output is a star-schema data warehouse with interactive dashboards for business insights.

## Architecture Overview
Raw CSV Data
     ↓
Staging Layer (PostgreSQL)
     ↓
Production Layer (Cleaned Data)
     ↓
Warehouse Layer (Star Schema)
     ↓
Power BI Dashboards

## Technologies Used

Python 3.11

PostgreSQL 14

Docker & Docker Compose

Power BI

Faker (Synthetic Data Generation)

psycopg2 & SQLAlchemy

## Project Structure
ecommerce-data-pipeline-23A91A6195/
│
├── data/
│   └── raw/
│       ├── customers.csv
│       ├── products.csv
│       ├── transactions.csv
│       ├── transaction_items.csv
│       └── generation_metadata.json
│
├── scripts/
│   ├── data_generation/
│   │   └── generate_data.py
│   └── ingestion/
│       └── load_to_staging.py
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── dashboards/
│   ├── powerbi/
│   │   └── ecommerce_dashboard.pbix
│   └── screenshots/
│       ├── sales_overview.png
│       ├── product_performance.png
│       ├── customer_analysis.png
│       └── time_discount_analysis.png
│
├── requirements.txt
└── README.md

## Data Pipeline Phases
Phase 1 — Data Generation

Synthetic e-commerce data generated using Faker

Customers, products, transactions, and transaction items

Metadata includes record counts and data quality score

Phase 2 — Data Ingestion

CSV files ingested into PostgreSQL staging schema

Fast bulk loading using COPY

Data integrity validated (no orphan records)

Phase 3 — Data Transformation

Cleaned data moved to production schema

Standardization (emails lower-cased, duplicates removed)

Ready for analytics

Phase 4 — Data Warehouse

Star schema implemented:

Dimension tables: customers, products, date

Fact table: sales

Optimized for analytical queries

## Data Warehouse Schema
Fact Table

warehouse.fact_sales

Dimension Tables

warehouse.dim_customers

warehouse.dim_products

warehouse.dim_date

This design improves query performance and BI reporting.

## Dashboards & Analytics (Power BI)
Dashboards Created

Sales Overview

Total revenue

Total orders

Average order value

Monthly revenue trend

Product Performance

Revenue by category

Top 10 products by revenue

Brand performance

Customer Analysis

Revenue by age group

Revenue by state

Top customers by spend

Time & Discount Analysis

Monthly sales trend

Quarterly sales

Discount vs revenue impact

## Power BI Connection Details
Field	Value
Server	localhost
Port	5432
Database	ecommerce_db
Username	admin
Password	password

Connect Power BI to warehouse schema only.

 How to Run the Project
 Start PostgreSQL using Docker
docker compose -f docker/docker-compose.yml up -d

 Activate Virtual Environment
venv\Scripts\activate

 Generate Data
python scripts/data_generation/generate_data.py

 Load Data into Staging
python scripts/ingestion/load_to_staging.py

 Run SQL Transformations

Create schemas and tables

Move data from staging → production → warehouse

 Open Power BI

Load warehouse tables

Create dashboards

Save screenshots