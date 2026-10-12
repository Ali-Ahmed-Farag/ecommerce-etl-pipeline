import logging
import os
import time

from load.load_raw import get_conn

logger = logging.getLogger(__name__)

DIM_TABLES = [
    "analytics.dim_date",
    "analytics.dim_customers",
    "analytics.dim_products",
    "analytics.dim_sellers",
]


def analytics_sql_dir() -> str:
    return os.getenv("ANALYTICS_SQL_DIR", "/opt/airflow/analytics_sql")


def run_load_dimension_tables(conn=None) -> dict:
    path = os.path.join(analytics_sql_dir(), "load_dimension_tables.sql")
    with open(path, encoding="utf-8") as fh:
        sql = fh.read()
    own = conn is None
    if own:
        conn = get_conn()
    try:
        start = time.monotonic()
        cur = conn.cursor()
        cur.execute(sql)
        counts = {}
        for table in DIM_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table.split(".")[1]] = cur.fetchone()[0]
        conn.commit()
        result = {"seconds": round(time.monotonic() - start, 1), **counts}
        logger.info("load_dimension_tables %s", result)
        return result
    finally:
        if own:
            conn.close()


def run_refresh_fact(conn=None) -> dict:
    own = conn is None
    if own:
        conn = get_conn()
    try:
        start = time.monotonic()
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW analytics.fact_sales")
        cur.execute("SELECT COUNT(*) FROM analytics.fact_sales")
        rows = cur.fetchone()[0]
        conn.commit()
        result = {"fact_rows": rows, "seconds": round(time.monotonic() - start, 1)}
        logger.info("refresh_fact_materialized_view %s", result)
        return result
    finally:
        if own:
            conn.close()
