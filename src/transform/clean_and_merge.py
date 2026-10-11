import pandas as pd

ALLOWED_STATUS = {
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
}


def clean_sales(raw_df: pd.DataFrame, load_date: str) -> pd.DataFrame:
    df = raw_df.copy()
    df = df.replace({"": pd.NA, " ": pd.NA})
    df = df.rename(columns={"order_item_id": "order_item_seq"})
    df["order_id"] = df["order_id"].astype("string")
    df["customer_id"] = df["customer_id"].astype("string")
    df["order_status"] = df["order_status"].astype("string")
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df["order_delivered_customer_date"] = pd.to_datetime(
        df["order_delivered_customer_date"], errors="coerce"
    )
    df["order_estimated_delivery_date"] = pd.to_datetime(
        df["order_estimated_delivery_date"], errors="coerce"
    )
    df["order_item_seq"] = pd.to_numeric(df["order_item_seq"], errors="coerce").astype("Int64")
    df["product_id"] = df["product_id"].astype("string")
    df["seller_id"] = df["seller_id"].astype("string")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")
    df["load_date"] = load_date
    df = df.drop_duplicates(subset=["order_id", "order_item_seq", "load_date"], keep="first")
    return df.reset_index(drop=True)


def validate_sales(df: pd.DataFrame) -> list[str]:
    errors = []
    required = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    for col in required:
        n = int(df[col].isna().sum())
        if n:
            errors.append(f"null_required:{col}:{n}")
    bad_status = df[~df["order_status"].isin(ALLOWED_STATUS) & df["order_status"].notna()]
    if len(bad_status):
        errors.append(f"invalid_order_status:{len(bad_status)}")
    items = df[df["order_item_seq"].notna()]
    nonpositive = items[items["price"].isna() | (items["price"] <= 0)]
    if len(nonpositive):
        errors.append(f"price_not_positive:{len(nonpositive)}")
    dupes = int(df.duplicated(subset=["order_id", "order_item_seq", "load_date"]).sum())
    if dupes:
        errors.append(f"duplicate_rows:{dupes}")
    missing_item_fields = items[
        items["product_id"].isna() | items["seller_id"].isna() | items["freight_value"].isna()
    ]
    if len(missing_item_fields):
        errors.append(f"incomplete_item_row:{len(missing_item_fields)}")
    return errors


def fetch_national_holiday_dates(conn, load_date: str) -> set:
    year = int(load_date[:4])
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT holiday_date FROM production.holidays
        WHERE state IS NULL
        UNION
        SELECT DISTINCT holiday_date::date FROM raw.holidays
        WHERE load_year = %s AND holiday_type = 'national'
        """,
        (year,),
    )
    return {str(r[0]) for r in cur.fetchall()}


def _iso_week(ts) -> str:
    return ts.isocalendar()[:2] if pd.notna(ts) else None


def merge_holiday_flags(df: pd.DataFrame, holiday_dates: set) -> pd.DataFrame:
    out = df.copy()
    holiday_weeks = set()
    for d in holiday_dates:
        ts = pd.Timestamp(d)
        holiday_weeks.add(_iso_week(ts))
    flags_nat = []
    flags_week = []
    for ts in out["order_purchase_timestamp"]:
        if pd.isna(ts):
            flags_nat.append(pd.NA)
            flags_week.append(pd.NA)
            continue
        dstr = ts.strftime("%Y-%m-%d")
        flags_nat.append(dstr in holiday_dates)
        flags_week.append(_iso_week(ts) in holiday_weeks)
    out["is_national_holiday"] = pd.array(flags_nat, dtype="boolean")
    out["is_holiday_week"] = pd.array(flags_week, dtype="boolean")
    return out


def clean_holidays(raw_holidays_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_holidays_df.copy()
    df["holiday_date"] = pd.to_datetime(df["holiday_date"], errors="coerce").dt.date
    df["holiday_name"] = df["holiday_name"].astype("string")
    df["holiday_type"] = df["holiday_type"].astype("string")
    df["state"] = pd.Series([pd.NA] * len(df), dtype="string")
    df = df[df["holiday_date"].notna()]
    df = df.drop_duplicates(subset=["holiday_date", "holiday_type", "state"], keep="first")
    return df.reset_index(drop=True)
