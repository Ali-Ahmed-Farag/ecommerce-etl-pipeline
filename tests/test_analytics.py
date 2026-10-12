import os

from load import load_analytics
from quality import data_quality_checks as dq


class _FakeCursor:
    def __init__(self, values):
        self._values = list(values)
        self.statements = []

    def execute(self, sql, *args):
        self.statements.append(sql)

    def fetchone(self):
        return [self._values.pop(0)]


class _FakeConn:
    def __init__(self, values):
        self.cursor_obj = _FakeCursor(values)

    def cursor(self):
        return self.cursor_obj


CLEAN_VALUES = [0, 112650, 112650, 0, 0, 0, 0, 0, 0, 0, 0]


def test_analytics_sql_dir_default(monkeypatch):
    monkeypatch.delenv("ANALYTICS_SQL_DIR", raising=False)
    assert load_analytics.analytics_sql_dir() == "/opt/airflow/analytics_sql"


def test_analytics_sql_dir_env_override(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SQL_DIR", "/tmp/custom_sql")
    assert load_analytics.analytics_sql_dir() == "/tmp/custom_sql"


def test_analytics_checks_clean():
    assert dq.run_analytics_checks(_FakeConn(CLEAN_VALUES)) == []


def test_analytics_checks_uses_schema_param():
    conn = _FakeConn(CLEAN_VALUES)
    dq.run_analytics_checks(conn, "scratch_p3")
    assert any("scratch_p3.fact_sales" in s for s in conn.cursor_obj.statements)
    assert any("production.order_items" in s for s in conn.cursor_obj.statements)


def test_analytics_delivery_consistency_fails():
    values = [2, 112650, 112650, 0, 0, 0, 0, 0, 0, 0, 0]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_delivery_consistency:2"
    ]


def test_analytics_fact_count_mismatch_fails():
    values = [0, 112649, 112650, 0, 0, 0, 0, 0, 0, 0, 0]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_fact_vs_order_items:112649!=112650"
    ]


def test_analytics_orphan_keys_fail():
    values = [0, 112650, 112650, 1, 2, 3, 0, 0, 0, 0, 0]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_orphan_fact_customer_key:1",
        "analytics_orphan_fact_product_key:2",
        "analytics_orphan_fact_seller_key:3",
    ]


def test_analytics_null_date_keys_fail():
    values = [0, 112650, 112650, 0, 0, 0, 4, 5, 0, 0, 0]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_null_purchase_date_key:4",
        "analytics_null_estimated_date_key:5",
    ]


def test_analytics_orphan_date_keys_fail():
    values = [0, 112650, 112650, 0, 0, 0, 0, 0, 6, 7, 8]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_orphan_fact_order_purchase_date_key:6",
        "analytics_orphan_fact_order_delivered_date_key:7",
        "analytics_orphan_fact_order_estimated_date_key:8",
    ]


def test_analytics_error_order_and_count():
    values = [1, 10, 20, 1, 0, 0, 1, 0, 0, 0, 1]
    assert dq.run_analytics_checks(_FakeConn(values)) == [
        "analytics_delivery_consistency:1",
        "analytics_fact_vs_order_items:10!=20",
        "analytics_orphan_fact_customer_key:1",
        "analytics_null_purchase_date_key:1",
        "analytics_orphan_fact_order_estimated_date_key:1",
    ]


def test_run_functions_expose_expected_keys():
    assert set(load_analytics.DIM_TABLES) == {
        "analytics.dim_date",
        "analytics.dim_customers",
        "analytics.dim_products",
        "analytics.dim_sellers",
    }
    assert os.path.basename(load_analytics.__file__) == "load_analytics.py"
