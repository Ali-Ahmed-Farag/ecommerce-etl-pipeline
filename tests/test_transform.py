import pandas as pd

from extract.extract_holidays import extract_holidays_api
from quality import data_quality_checks as dq
from transform.clean_and_merge import (
    clean_holidays,
    clean_sales,
    merge_holiday_flags,
    validate_sales,
)

RAW_COLUMNS = [
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


def _raw_frame(rows):
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def _clean_frame(rows, load_date="2017-10-01"):
    return clean_sales(_raw_frame(rows), load_date)


CLEAN_ROWS = [
    [
        "o1",
        "c1",
        "delivered",
        "2017-10-01 10:00:00",
        "",
        "2017-10-20",
        "1",
        "p1",
        "s1",
        "39.99",
        "5.00",
    ],
    [
        "o2",
        "c2",
        "shipped",
        "2017-10-02 11:00:00",
        "",
        "2017-10-25",
        "",
        "p2",
        "s2",
        "10.00",
        "2.00",
    ],
]


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, query, params=None):
        self.query = query
        self.params = params

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)


class _CountCursor:
    def __init__(self, counts):
        self._counts = list(counts)

    def execute(self, query, params=None):
        self.query = query
        self.params = params

    def fetchone(self):
        return (self._counts.pop(0),)


class _CountConn:
    def __init__(self, counts):
        self._counts = counts

    def cursor(self):
        return _CountCursor(self._counts)


def test_clean_sales_renames_item_column_and_sets_load_date():
    df = _clean_frame(CLEAN_ROWS)
    assert "order_item_seq" in df.columns
    assert "order_item_id" not in df.columns
    assert set(df["load_date"]) == {"2017-10-01"}
    assert df["price"].dtype.kind == "f"
    assert str(df["order_item_seq"].dtype) == "Int64"


def test_clean_sales_maps_empty_item_id_to_null():
    rows = [["o9", "c9", "shipped", "", "", "", "", "p9", "s9", "1.00", "1.00"]]
    df = _clean_frame(rows)
    row = df.iloc[0]
    assert pd.isna(row["order_item_seq"])
    assert pd.isna(row["order_purchase_timestamp"])
    assert pd.isna(row["order_delivered_customer_date"])


def test_clean_sales_drops_duplicate_business_keys():
    rows = CLEAN_ROWS + [list(CLEAN_ROWS[0])]
    df = _clean_frame(rows)
    assert len(df) == 2


def test_clean_sales_empty_input_returns_empty_frame():
    df = _clean_frame([])
    assert len(df) == 0
    assert "order_item_seq" in df.columns
    assert "load_date" in df.columns


def test_validate_sales_accepts_clean_frame():
    df = _clean_frame(CLEAN_ROWS)
    assert validate_sales(df) == []


def test_validate_sales_accepts_empty_frame():
    assert validate_sales(_clean_frame([])) == []


def test_validate_sales_flags_null_required_customer():
    df = _clean_frame(CLEAN_ROWS)
    df.loc[0, "customer_id"] = pd.NA
    assert "null_required:customer_id:1" in validate_sales(df)


def test_validate_sales_flags_invalid_status():
    df = _clean_frame(CLEAN_ROWS)
    df.loc[0, "order_status"] = "teleported"
    assert "invalid_order_status:1" in validate_sales(df)


def test_validate_sales_flags_non_positive_price():
    df = _clean_frame(CLEAN_ROWS)
    df.loc[0, "price"] = -5.0
    assert "price_not_positive:1" in validate_sales(df)


def test_validate_sales_ignores_price_on_no_item_rows():
    df = _clean_frame(CLEAN_ROWS)
    df.loc[1, "price"] = pd.NA
    assert validate_sales(df) == []


def test_validate_sales_flags_duplicate_rows():
    rows = CLEAN_ROWS + [list(CLEAN_ROWS[0])]
    df = clean_sales(_raw_frame(rows), "2017-10-01")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    assert "duplicate_rows:1" in validate_sales(df)


def test_validate_sales_flags_incomplete_item_row():
    df = _clean_frame(CLEAN_ROWS)
    df.loc[0, "product_id"] = pd.NA
    assert "incomplete_item_row:1" in validate_sales(df)


def test_merge_holiday_flags_marks_exact_day_and_week():
    df = _clean_frame(CLEAN_ROWS)
    df["order_purchase_timestamp"] = pd.to_datetime(
        ["2017-10-12", "2017-10-16"]
    )
    out = merge_holiday_flags(df, {"2017-10-12"})
    assert bool(out.loc[0, "is_national_holiday"]) is True
    assert bool(out.loc[0, "is_holiday_week"]) is True
    assert bool(out.loc[1, "is_national_holiday"]) is False
    assert bool(out.loc[1, "is_holiday_week"]) is False


def test_merge_holiday_flags_marks_whole_iso_week():
    df = _clean_frame(
        CLEAN_ROWS
        + [["o3", "c3", "created", "2017-10-16 09:00:00", "", "2017-10-30", "1", "p3", "s3", "5.00", "1.00"]]
    )
    df["order_purchase_timestamp"] = pd.to_datetime(
        ["2017-10-09", "2017-10-15", "2017-10-16"]
    )
    out = merge_holiday_flags(df, {"2017-10-12"})
    assert bool(out.loc[0, "is_holiday_week"]) is True
    assert bool(out.loc[0, "is_national_holiday"]) is False
    assert bool(out.loc[1, "is_holiday_week"]) is True
    assert bool(out.loc[2, "is_holiday_week"]) is False


def test_merge_holiday_flags_null_timestamp_yields_null_flags():
    df = _clean_frame(CLEAN_ROWS)
    df["order_purchase_timestamp"] = pd.NaT
    out = merge_holiday_flags(df, {"2017-10-12"})
    assert pd.isna(out.loc[0, "is_national_holiday"])
    assert pd.isna(out.loc[0, "is_holiday_week"])


def test_clean_holidays_parses_dates_and_drops_invalid():
    raw = pd.DataFrame(
        {
            "holiday_date": ["2016-01-01", "not-a-date", "2016-01-01"],
            "holiday_name": ["New Year", "bad", "New Year"],
            "holiday_type": ["national", "national", "national"],
            "weekday": ["x", "x", "x"],
            "load_year": [2016, 2016, 2016],
        }
    )
    out = clean_holidays(raw)
    assert len(out) == 1
    assert str(out.iloc[0]["holiday_date"]) == "2016-01-01"
    assert out["state"].isna().all()


def test_holiday_extract_skips_api_when_year_loaded(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("api must not be called for a loaded year")

    monkeypatch.setattr("extract.extract_holidays.requests.get", _fail)
    assert extract_holidays_api(_FakeConn((1,)), 2017) is None


def test_holiday_extract_calls_api_for_missing_year(monkeypatch):
    calls = {}

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "date": "2016-01-01",
                    "name": "Confraternizacao mundial",
                    "type": "national",
                    "weekday": "sexta-feira",
                }
            ]

    def _get(url, timeout=None):
        calls["url"] = url
        calls["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr("extract.extract_holidays.requests.get", _get)
    rows = extract_holidays_api(_FakeConn(None), 2016)
    assert calls["url"] == "https://brasilapi.com.br/api/feriados/v1/2016"
    assert len(rows) == 1


def test_incoming_csv_header_only_has_no_data(tmp_path, monkeypatch):
    (tmp_path / "2016-01-01.csv").write_text("order_id,customer_id\n", encoding="utf-8")
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    assert dq.incoming_csv_has_data("2016-01-01") is False


def test_incoming_csv_with_data_rows_is_true(tmp_path, monkeypatch):
    (tmp_path / "2017-10-01.csv").write_text(
        "order_id,customer_id\no1,c1\n", encoding="utf-8"
    )
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    assert dq.incoming_csv_has_data("2017-10-01") is True


def test_incoming_csv_missing_file_is_false(tmp_path, monkeypatch):
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    assert dq.incoming_csv_has_data("2018-12-31") is False


def _empty_staging_df():
    return pd.DataFrame({"order_id": [], "order_item_seq": []})


def test_post_checks_pass_for_header_only_day(tmp_path, monkeypatch):
    (tmp_path / "2016-01-01.csv").write_text("order_id,customer_id\n", encoding="utf-8")
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    errors = dq.run_post_production_checks(_CountConn([0] * 7), _empty_staging_df(), "2016-01-01")
    assert errors == []


def test_post_checks_fail_when_incoming_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    errors = dq.run_post_production_checks(_CountConn([0] * 7), _empty_staging_df(), "2016-01-01")
    assert errors == ["freshness_missing_incoming_file"]


def test_post_checks_fail_when_data_file_not_loaded(tmp_path, monkeypatch):
    (tmp_path / "2017-10-01.csv").write_text(
        "order_id,customer_id\no1,c1\n", encoding="utf-8"
    )
    monkeypatch.setattr(dq, "INCOMING_DIR", str(tmp_path))
    errors = dq.run_post_production_checks(_CountConn([0] * 7), _empty_staging_df(), "2017-10-01")
    assert errors == ["freshness_raw_missing"]


def test_staging_checks_flags_negative_price():
    df = pd.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_status": ["delivered"],
            "order_purchase_timestamp": [pd.Timestamp("2017-10-01")],
            "order_item_seq": [1],
            "price": [-1.0],
            "load_date": ["2017-10-01"],
        }
    )
    assert dq.run_staging_checks(df) == ["staging_price_not_positive:1"]


def test_staging_checks_accepts_clean_day():
    df = _clean_frame(CLEAN_ROWS)
    df = merge_holiday_flags(df, set())
    assert dq.run_staging_checks(df) == []
