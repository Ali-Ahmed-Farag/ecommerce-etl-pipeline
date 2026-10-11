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


def get_conn():
    return psycopg2.connect(**CONN_KW)


def upsert_orders(conn, df: pd.DataFrame) -> int:
    orders = (
        df[
            [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ]
        ]
        .drop_duplicates(subset=["order_id"], keep="first")
        .copy()
    )
    for col in (
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        orders[col] = orders[col].astype(object).where(orders[col].notna(), None)
    rows = [tuple(r) for r in orders.itertuples(index=False)]
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO production.orders (
            order_id, customer_id, order_status,
            order_purchase_timestamp, order_delivered_customer_date,
            order_estimated_delivery_date
        ) VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_status = EXCLUDED.order_status,
            order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
            order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
            order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_order_items(conn, df: pd.DataFrame) -> int:
    items = df[df["order_item_seq"].notna()].copy()
    if len(items) == 0:
        return 0
    rows = [
        (
            r.order_id,
            int(r.order_item_seq),
            r.product_id,
            r.seller_id,
            float(r.price),
            float(r.freight_value),
        )
        for r in items.itertuples(index=False)
    ]
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO production.order_items (
            order_id, order_item_seq, product_id, seller_id, price, freight_value
        ) VALUES %s
        ON CONFLICT (order_id, order_item_seq) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            seller_id = EXCLUDED.seller_id,
            price = EXCLUDED.price,
            freight_value = EXCLUDED.freight_value
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_holidays(conn, holidays_df: pd.DataFrame) -> int:
    if len(holidays_df) == 0:
        return 0
    rows = []
    for r in holidays_df.itertuples(index=False):
        name = str(r.holiday_name) if pd.notna(r.holiday_name) else None
        is_national = True if pd.isna(r.state) else False
        rows.append((r.holiday_date, name, is_national, None))
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO production.holidays (holiday_date, holiday_name, is_national, state)
        VALUES %s
        ON CONFLICT (
            holiday_date,
            (COALESCE(state::text, '__NATIONAL__'))
        ) DO UPDATE SET
            holiday_name = EXCLUDED.holiday_name,
            is_national = EXCLUDED.is_national
        """,
        rows,
    )
    conn.commit()
    return len(rows)
