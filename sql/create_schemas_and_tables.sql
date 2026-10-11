CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sales_daily (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    price TEXT,
    freight_value TEXT,
    load_date DATE NOT NULL,
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS ix_raw_sales_daily_load_date ON raw.sales_daily (load_date);

CREATE TABLE IF NOT EXISTS raw.holidays (
    holiday_date TEXT,
    holiday_name TEXT,
    holiday_type TEXT,
    weekday TEXT,
    load_year INT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_holidays_load_year ON raw.holidays (load_year);

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.sales_cleaned (
    order_id VARCHAR NOT NULL,
    customer_id VARCHAR NOT NULL,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_delivered_customer_date TIMESTAMP NULL,
    order_estimated_delivery_date TIMESTAMP,
    order_item_seq INT NULL,
    product_id VARCHAR NULL,
    seller_id VARCHAR NULL,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    load_date DATE NOT NULL,
    is_national_holiday BOOLEAN,
    is_holiday_week BOOLEAN
);

CREATE INDEX IF NOT EXISTS ix_staging_sales_cleaned_load_date ON staging.sales_cleaned (load_date);

CREATE TABLE IF NOT EXISTS staging.holidays_validated (
    holiday_date DATE NOT NULL,
    holiday_name VARCHAR,
    holiday_type VARCHAR,
    state CHAR(2) NULL
);

CREATE TABLE IF NOT EXISTS production.holidays (
    holiday_id SERIAL PRIMARY KEY,
    holiday_date DATE NOT NULL,
    holiday_name VARCHAR,
    is_national BOOLEAN,
    state CHAR(2) NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_holidays_date_state
ON production.holidays (
    holiday_date,
    COALESCE(state::text, '__NATIONAL__')
);
