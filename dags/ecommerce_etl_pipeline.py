import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/src")

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ecommerce_etl_pipeline",
    schedule="@daily",
    start_date=datetime(2016, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "etl"],
)
def ecommerce_etl_pipeline():
    @task
    def extract_sales_csv(**context):
        from extract.extract_csv import extract_sales_csv as run

        return run(context["ds"])

    @task
    def extract_holidays_api(**context):
        from extract.extract_holidays import extract_holidays_api as run
        from load.load_raw import get_conn

        year = int(context["ds"][:4])
        conn = get_conn()
        try:
            return run(conn, year)
        finally:
            conn.close()

    @task
    def load_raw(sales_payload: dict, holidays_payload, **context):
        from load.load_raw import get_conn, load_raw_holidays, load_raw_sales

        ds = context["ds"]
        year = int(ds[:4])
        conn = get_conn()
        try:
            n_sales = load_raw_sales(
                conn, ds, sales_payload["source_file"], sales_payload["content"]
            )
            n_holidays = 0
            if holidays_payload:
                n_holidays = load_raw_holidays(conn, year, holidays_payload)
            return {"sales_rows": n_sales, "holiday_rows": n_holidays, "year": year}
        finally:
            conn.close()

    @task
    def clean_and_validate_staging(**context):
        import pandas as pd
        from load.load_raw import get_conn
        from load.load_staging import load_staging_holidays, load_staging_sales
        from transform.clean_and_merge import (
            clean_holidays,
            clean_sales,
            fetch_national_holiday_dates,
            merge_holiday_flags,
            validate_sales,
        )

        ds = context["ds"]
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT order_id, customer_id, order_status, order_purchase_timestamp, "
                "order_delivered_customer_date, order_estimated_delivery_date, "
                "order_item_id, product_id, seller_id, price, freight_value "
                "FROM raw.sales_daily WHERE load_date = %s",
                (ds,),
            )
            cols = [d[0] for d in cur.description]
            raw_df = pd.DataFrame(cur.fetchall(), columns=cols)
            cleaned = clean_sales(raw_df, ds)
            errors = validate_sales(cleaned)
            if errors:
                raise AirflowException(f"staging validation failed: {errors}")
            holiday_dates = fetch_national_holiday_dates(conn, ds)
            flagged = merge_holiday_flags(cleaned, holiday_dates)
            year = int(ds[:4])
            cur.execute(
                "SELECT holiday_date, holiday_name, holiday_type, weekday, load_year "
                "FROM raw.holidays WHERE load_year = %s",
                (year,),
            )
            hcols = [d[0] for d in cur.description]
            raw_hol_df = pd.DataFrame(cur.fetchall(), columns=hcols)
            cleaned_hols = clean_holidays(raw_hol_df)
            n_hol = load_staging_holidays(conn, cleaned_hols)
            n_sales = load_staging_sales(conn, flagged, ds)
            return {"staging_sales_rows": n_sales, "staging_holiday_rows": n_hol}
        finally:
            conn.close()

    @task
    def upsert_production(**context):
        import pandas as pd
        from load.upsert_production import (
            get_conn,
            upsert_holidays,
            upsert_order_items,
            upsert_orders,
        )
        from transform.clean_and_merge import clean_holidays

        ds = context["ds"]
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT order_id, customer_id, order_status, order_purchase_timestamp, "
                "order_delivered_customer_date, order_estimated_delivery_date, "
                "order_item_seq, product_id, seller_id, price, freight_value, load_date, "
                "is_national_holiday, is_holiday_week "
                "FROM staging.sales_cleaned WHERE load_date = %s",
                (ds,),
            )
            cols = [d[0] for d in cur.description]
            stg_df = pd.DataFrame(cur.fetchall(), columns=cols)
            for c in (
                "order_purchase_timestamp",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ):
                stg_df[c] = pd.to_datetime(stg_df[c])
            stg_df["order_item_seq"] = pd.to_numeric(
                stg_df["order_item_seq"], errors="coerce"
            ).astype("Int64")
            stg_df["price"] = pd.to_numeric(stg_df["price"], errors="coerce")
            stg_df["freight_value"] = pd.to_numeric(stg_df["freight_value"], errors="coerce")
            n_ord = upsert_orders(conn, stg_df)
            n_oi = upsert_order_items(conn, stg_df)
            year = int(ds[:4])
            cur.execute(
                "SELECT holiday_date, holiday_name, holiday_type, weekday, load_year "
                "FROM raw.holidays WHERE load_year = %s",
                (year,),
            )
            hcols = [d[0] for d in cur.description]
            raw_hol_df = pd.DataFrame(cur.fetchall(), columns=hcols)
            cleaned_hols = clean_holidays(raw_hol_df)
            n_hol = upsert_holidays(conn, cleaned_hols)
            return {"orders_upserted": n_ord, "items_upserted": n_oi, "holidays_upserted": n_hol}
        finally:
            conn.close()

    @task
    def data_quality_check(**context):
        import pandas as pd
        from load.load_raw import get_conn
        from quality.data_quality_checks import run_post_production_checks, run_staging_checks

        ds = context["ds"]
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT order_id, customer_id, order_status, order_purchase_timestamp, "
                "order_delivered_customer_date, order_estimated_delivery_date, "
                "order_item_seq, product_id, seller_id, price, freight_value, load_date, "
                "is_national_holiday, is_holiday_week "
                "FROM staging.sales_cleaned WHERE load_date = %s",
                (ds,),
            )
            cols = [d[0] for d in cur.description]
            stg_df = pd.DataFrame(cur.fetchall(), columns=cols)
            for c in (
                "order_purchase_timestamp",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ):
                stg_df[c] = pd.to_datetime(stg_df[c])
            stg_df["order_item_seq"] = pd.to_numeric(
                stg_df["order_item_seq"], errors="coerce"
            ).astype("Int64")
            stg_df["price"] = pd.to_numeric(stg_df["price"], errors="coerce")
            errors = run_staging_checks(stg_df)
            errors += run_post_production_checks(conn, stg_df, ds)
            if errors:
                raise AirflowException(f"data quality gate failed: {errors}")
            return {"status": "passed", "errors": 0}
        finally:
            conn.close()

    @task
    def notify_success(**context):
        print(
            f"DAG run succeeded run_id={context['run_id']} "
            f"logical_date={context.get('logical_date')}"
        )
        return "success"

    sales = extract_sales_csv()
    holidays = extract_holidays_api()
    raw_loaded = load_raw(sales, holidays)
    staged = clean_and_validate_staging()
    upserted = upsert_production()
    dq = data_quality_check()
    notify = notify_success()

    raw_loaded >> staged >> upserted >> dq >> notify


dag = ecommerce_etl_pipeline()
