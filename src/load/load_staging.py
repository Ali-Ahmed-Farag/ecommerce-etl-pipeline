import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

CONN_KW = dict(
    host="host.docker.internal",
    port=5433,
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    dbname="ecommerce",
)

SALES_OUT_COLS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "order_item_seq",
    "product_id",
    "seller_id",
    "price",
    "freight_value",
    "load_date",
    "is_national_holiday",
    "is_holiday_week",
]


def get_conn():
    return psycopg2.connect(**CONN_KW)


def load_staging_sales(conn, df: pd.DataFrame, load_date: str) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM staging.sales_cleaned WHERE load_date = %s", (load_date,))
    if len(df) == 0:
        conn.commit()
        return 0
    out = df.copy()
    for col in (
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        out[col] = out[col].astype(object).where(out[col].notna(), None)
    out["order_item_seq"] = out["order_item_seq"].astype(object).where(out["order_item_seq"].notna(), None)
    out["product_id"] = out["product_id"].astype(object).where(out["product_id"].notna(), None)
    out["seller_id"] = out["seller_id"].astype(object).where(out["seller_id"].notna(), None)
    out["price"] = out["price"].astype(object).where(out["price"].notna(), None)
    out["freight_value"] = out["freight_value"].astype(object).where(out["freight_value"].notna(), None)
    out["is_national_holiday"] = out["is_national_holiday"].astype(object).where(out["is_national_holiday"].notna(), None)
    out["is_holiday_week"] = out["is_holiday_week"].astype(object).where(out["is_holiday_week"].notna(), None)
    rows = [tuple(r) for r in out[SALES_OUT_COLS].itertuples(index=False)]
    execute_values(
        cur,
        "INSERT INTO staging.sales_cleaned ("
        + ",".join(SALES_OUT_COLS)
        + ") VALUES %s",
        rows,
    )
    conn.commit()
    return len(rows)


def load_staging_holidays(conn, df: pd.DataFrame) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM staging.holidays_validated")
    if len(df) == 0:
        conn.commit()
        return 0
    rows = [
        (r.holiday_date, str(r.holiday_name) if pd.notna(r.holiday_name) else None,
         str(r.holiday_type) if pd.notna(r.holiday_type) else None, None)
        for r in df.itertuples(index=False)
    ]
    execute_values(
        cur,
        "INSERT INTO staging.holidays_validated (holiday_date, holiday_name, holiday_type, state) VALUES %s",
        rows,
    )
    conn.commit()
    return len(rows)
