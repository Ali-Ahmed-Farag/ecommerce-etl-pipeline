import io
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONN_KW = dict(
    host="host.docker.internal",
    port=5433,
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    dbname="ecommerce",
)

SALES_COLS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "order_item_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value",
]


def get_conn():
    return psycopg2.connect(**CONN_KW)


def load_raw_sales(conn, logical_date: str, source_file: str, content: str) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM raw.sales_daily WHERE load_date = %s", (logical_date,))
    reader = csv_reader(content)
    header = next(reader, None)
    if header is None:
        conn.commit()
        return 0
    rows = [tuple(r) + (logical_date, source_file) for r in reader if any(c.strip() for c in r)]
    if rows:
        template = ",".join(["%s"] * (len(SALES_COLS) + 2))
        cur.executemany(
            f"INSERT INTO raw.sales_daily ({','.join(SALES_COLS)}, load_date, source_file) VALUES ({template})",
            rows,
        )
    conn.commit()
    return len(rows)


def load_raw_holidays(conn, load_year: int, holidays: list[dict]) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM raw.holidays WHERE load_year = %s", (load_year,))
    rows = [
        (h.get("date"), h.get("name"), h.get("type"), h.get("weekday"), load_year)
        for h in holidays
    ]
    if rows:
        cur.executemany(
            "INSERT INTO raw.holidays (holiday_date, holiday_name, holiday_type, weekday, load_year) VALUES (%s,%s,%s,%s,%s)",
            rows,
        )
    conn.commit()
    return len(rows)


def csv_reader(content: str):
    return __import__("csv").reader(io.StringIO(content))
