import os

import pandas as pd

from extract.extract_csv import INCOMING_DIR


def incoming_csv_has_data(load_date: str) -> bool:
    path = os.path.join(INCOMING_DIR, f"{load_date}.csv")
    if not os.path.exists(path):
        return False
    with open(path, encoding="utf-8") as fh:
        next(fh, None)
        return any(line.strip() for line in fh)


def run_staging_checks(df: pd.DataFrame) -> list[str]:
    errors = []
    required = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    for col in required:
        n = int(df[col].isna().sum())
        if n:
            errors.append(f"staging_null_required:{col}:{n}")
    items = df[df["order_item_seq"].notna()]
    bad_price = items[items["price"].isna() | (items["price"] <= 0)]
    if len(bad_price):
        errors.append(f"staging_price_not_positive:{len(bad_price)}")
    dupes = int(df.duplicated(subset=["order_id", "order_item_seq", "load_date"]).sum())
    if dupes:
        errors.append(f"staging_duplicate_rows:{dupes}")
    return errors


def run_post_production_checks(conn, staging_df: pd.DataFrame, load_date: str) -> list[str]:
    errors = []
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.sales_daily WHERE load_date = %s", (load_date,))
    raw_count = cur.fetchone()[0]
    incoming_path = os.path.join(INCOMING_DIR, f"{load_date}.csv")
    if not os.path.exists(incoming_path):
        errors.append("freshness_missing_incoming_file")
    elif raw_count == 0 and incoming_csv_has_data(load_date):
        errors.append("freshness_raw_missing")

    cur.execute("SELECT COUNT(*) FROM staging.sales_cleaned WHERE load_date = %s", (load_date,))
    staging_count = cur.fetchone()[0]
    if staging_count == 0 and raw_count > 0:
        errors.append("freshness_staging_missing")

    cur.execute(
        """
        SELECT COUNT(*) FROM production.orders o
        WHERE o.order_id IN (
            SELECT DISTINCT order_id FROM staging.sales_cleaned WHERE load_date = %s
        )
        """,
        (load_date,),
    )
    prod_orders_for_day = cur.fetchone()[0]
    staging_orders = int(staging_df["order_id"].nunique())
    if prod_orders_for_day != staging_orders:
        errors.append(
            f"rowcount_orders_staging_vs_production:{staging_orders}!={prod_orders_for_day}"
        )

    cur.execute(
        """
        SELECT COUNT(*) FROM production.order_items oi
        WHERE (oi.order_id, oi.order_item_seq) IN (
            SELECT order_id, order_item_seq FROM staging.sales_cleaned
            WHERE load_date = %s AND order_item_seq IS NOT NULL
        )
        """,
        (load_date,),
    )
    prod_items_for_day = cur.fetchone()[0]
    staging_items = int(staging_df["order_item_seq"].notna().sum())
    if prod_items_for_day != staging_items:
        errors.append(
            f"rowcount_items_staging_vs_production:{staging_items}!={prod_items_for_day}"
        )

    cur.execute(
        """
        SELECT COUNT(*) FROM staging.sales_cleaned s
        WHERE s.load_date = %s
          AND NOT EXISTS (
              SELECT 1 FROM production.customers c WHERE c.customer_id = s.customer_id
          )
        """,
        (load_date,),
    )
    orphan_customers = cur.fetchone()[0]
    if orphan_customers:
        errors.append(f"referential_orphan_customers:{orphan_customers}")

    cur.execute(
        """
        SELECT COUNT(*) FROM staging.sales_cleaned s
        WHERE s.load_date = %s
          AND s.order_item_seq IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM production.products p WHERE p.product_id = s.product_id
          )
        """,
        (load_date,),
    )
    orphan_products = cur.fetchone()[0]
    if orphan_products:
        errors.append(f"referential_orphan_products:{orphan_products}")

    cur.execute(
        """
        SELECT COUNT(*) FROM staging.sales_cleaned s
        WHERE s.load_date = %s
          AND s.order_item_seq IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM production.sellers sel WHERE sel.seller_id = s.seller_id
          )
        """,
        (load_date,),
    )
    orphan_sellers = cur.fetchone()[0]
    if orphan_sellers:
        errors.append(f"referential_orphan_sellers:{orphan_sellers}")

    return errors
