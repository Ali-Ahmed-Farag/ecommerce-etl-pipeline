import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ecommerce-database-design", "data")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "incoming", "sales")

START = "2016-01-01"
END = "2018-12-31"

COLUMNS = [
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


def main():
    orders = pd.read_csv(
        os.path.join(DATA_DIR, "olist_orders_dataset.csv"),
        usecols=[
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        dtype=str,
    )
    items = pd.read_csv(
        os.path.join(DATA_DIR, "olist_order_items_dataset.csv"),
        usecols=["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
        dtype=str,
    )
    merged = orders.merge(items, on="order_id", how="left")
    merged["purchase_date"] = pd.to_datetime(merged["order_purchase_timestamp"]).dt.date.astype(str)

    os.makedirs(OUT_DIR, exist_ok=True)
    all_dates = pd.date_range(START, END).strftime("%Y-%m-%d").tolist()
    by_date = {d: g[COLUMNS] for d, g in merged.groupby("purchase_date")}

    for d in all_dates:
        path = os.path.join(OUT_DIR, f"{d}.csv")
        if d in by_date:
            by_date[d].to_csv(path, index=False)
        else:
            pd.DataFrame(columns=COLUMNS).to_csv(path, index=False)

    nonempty = sum(1 for d in all_dates if d in by_date)
    print(f"dates_total={len(all_dates)}")
    print(f"dates_with_orders={nonempty}")
    print(f"dates_header_only={len(all_dates) - nonempty}")
    print(f"rows_total={len(merged)}")


if __name__ == "__main__":
    main()
