# Deviations and Additions vs FINAL_IMPLEMENTATION_BLUEPRINT.md

Each item below is a deliberate deviation or addition relative to the Blueprint. Evidence comes from real command output in the stage reports of this repository.

## 1. Staging logical row identity: (order_id, order_item_seq, load_date)

**Blueprint said:** `raw.sales_daily` and `staging.sales_cleaned` are keyed by `(order_id, load_date)` (Project 2 §9).

**What we do:** Keep `load_date` alone as the raw delete-then-insert partition key (Blueprint §16). In staging, the logical row identity for dedup is `(order_id, order_item_seq, load_date)`. No UNIQUE index was added on either pair.

**Reason:** The Blueprint key is not unique for line-item rows: one order can have multiple items on the same purchase date. `(order_id, order_item_id, load_date)` is the approved minimal fix; `order_item_id` is renamed to `order_item_seq` in staging to match Project 1 column naming.

**Evidence (Stage 2, sample day 2017-10-02):**
```
 total_rows | distinct_order_date_pairs
------------+---------------------------
        167 |                       143
```

## 2. staging.sales_cleaned NOT NULL dropped on order_item_seq, product_id, seller_id

**Blueprint said:** `staging.sales_cleaned` has properly typed sales columns plus holiday flags (Project 2 §8); orders without items are not given an explicit staging rule.

**What we do:** `order_item_seq`, `product_id`, and `seller_id` are nullable in `staging.sales_cleaned`. Rows with NULL item fields are upserted into `production.orders` only and never into `production.order_items`. Referential product/seller checks skip those rows.

**Reason:** 775 Olist orders have zero line items. Without nullable item columns those orders cannot land in staging at all, and would be silently dropped from the pipeline even though Project 1 `production.orders` already holds them.

**Evidence (Stage 2 coverage query):**
```
 orders_total | distinct_orders_with_items | orders_without_items
--------------+----------------------------+----------------------
        99441 |                      98666 |                  775
```

**Evidence (Stage 3, no-item order path on 2017-10-02):**
```
             order_id             | order_item_seq | is_national_holiday | is_holiday_week
----------------------------------+----------------+---------------------+-----------------
 8d6342c5d498f0042ad00193ce3038c8 |                | f                   | f

             order_id             | order_status
----------------------------------+--------------
 8d6342c5d498f0042ad00193ce3038c8 | unavailable
```
`noitem_orders_accidentally_in_items` returned 0.

## 3. Holiday lookup: production.holidays (state IS NULL) UNION raw.holidays for the logical year

**Blueprint said:** Staging enrichment looks up the full persisted `production.holidays` table, not only the current run's raw holiday partition (Project 2 §5).

**What we do:** `is_national_holiday` and `is_holiday_week` are computed from `production.holidays WHERE state IS NULL` UNION `raw.holidays` for the DAG run's logical year where `holiday_type = 'national'`.

**Reason:** On the first run of a new logical year, `extract_holidays_api` has loaded that year into `raw.holidays`, but `upsert_production` (which writes `production.holidays`) runs after staging. Without the union, the first day of each year would get FALSE holiday flags. The union keeps persisted `production.holidays` as the primary source and only backfills the current year from raw when production does not yet have it. BrasilAPI returns national holidays only; `state` is always NULL (verified live: bare JSON array of `{date,name,type,weekday}`, `type="national"`).

**Evidence (Stage 3 standalone run, 2017-10-02):**
```
holiday_dates_available [...] count 13
staging_holidays_rows 13
upserted holidays 13
holidays_non_null_state 0
holidays_not_national 0
```

**Evidence (Stage 3 ISO-week proof, holiday 2017-10-12):**
```
order_purchase_timestamp  is_national_holiday  is_holiday_week
              2017-10-09                False             True
              2017-10-12                 True             True
              2017-10-16                False            False
```

## 4. Image tag apache/airflow:2.9.3-python3.11

**Blueprint said:** Pin `apache/airflow:2.9.3` (Project 2 §18), not `latest`.

**What we do:** Build FROM `apache/airflow:2.9.3-python3.11` (same Airflow version, Python 3.11 variant).

**Reason:** The default `apache/airflow:2.9.3` image ships Python 3.8; pinned `pandas==2.2.2` requires Python >= 3.9. The python3.11 variant keeps Airflow exactly 2.9.3 (backfill CLI syntax per Blueprint §13 still applies) while allowing the pinned dependency set. Build log confirmed base image digest for `apache/airflow:2.9.3-python3.11` and successful pip install.

**Evidence (Stage 1 build):**
```
#7 [1/3] FROM docker.io/apache/airflow:2.9.3-python3.11@sha256:cc5fcb91e93e4dfe4fd8b1b53a9155dfa2670fb829891a9658a0f36ac55f67ef
...
Successfully installed ... pandas-2.2.2 ... pytest-8.2.2 ... requests-2.32.3
```

## 5. DAG intentionally truncated until Project 3 (SUPERSEDED)

**Blueprint said:** Full chain ends with `load_dimension_tables` → `refresh_fact_materialized_view` → `data_quality_check` → `notify_success`, where `data_quality_check` includes delivery-consistency and `fact_sales`-vs-`production.order_items` checks (Project 2 §13, Project 3 §14).

**What we do:** The DAG contains only: `extract_sales_csv` and `extract_holidays_api` (parallel) → `load_raw` → `clean_and_validate_staging` → `upsert_production` → `data_quality_check` → `notify_success`. `data_quality_check` is limited to Project 2 checks (staging nulls/price/dupes; freshness; staging-vs-production row counts; referential checks against customers/products/sellers). No `load_dimension_tables`, no `refresh_fact_materialized_view`, no Project 3 quality checks. No EmptyOperator placeholders.

**Reason:** Project 3 (analytics schema, dimensions, `fact_sales` materialized view) is out of scope for this repository's current milestone. The README will state this truncation honestly.

**Evidence:** Stage decision fixed before Stage 4 DAG authoring; Project 3 objects do not exist in this database (`analytics` schema not created in Stage 1 DDL).

**Status:** SUPERSEDED by Project 3 integration — the DAG now runs the full 9-task chain (see items 8–10). The text above is kept as the historical record.

## 6. Holiday logical year derived from `ds`, not `data_interval_start`

**Blueprint said:** `extract_holidays_api` calls BrasilAPI only if `{{ data_interval_start.year }}` (the DAG run's logical year) is not already present in `raw.holidays` (Project 2 §13).

**What we do:** Every year derivation in the DAG uses the logical date `ds` (`year = int(ds[:4])` / `int(context["ds"][:4])`) instead of `context["data_interval_start"].year`.

**Reason:** In Airflow 2.9.3 the two values are identical for scheduled/backfill runs but **not** for manual runs (`airflow dags test`): the metadata database shows `manual__2016-01-01T00:00:00+00:00` has `execution_date = 2016-01-01` but `data_interval_start = 2015-12-31`, while every `backfill__2017-10-*` run has `data_interval_start = execution_date`. Using `data_interval_start.year` on a manual run for Jan 1 therefore fetched and stored **2015** holidays for a 2016 logical date, and `ds`-driven tasks (extract, staging, upsert) would then look up a different year than the guard used. Deriving the year from `ds` keeps one consistent source across all four places that need it (extract guard, raw load, staging rebuild, production upsert). The wall-clock prohibition is still honored: `ds` is the run's logical date, never `datetime.now()`.

**Evidence (Stage 4, bug found before the fix, manual run for ds=2016-01-01):**
```
Done. Returned value was: {'sales_rows': 0, 'holiday_rows': 0, 'year': 2015}
raw.holidays load_year: 2015 (13 rows)  <- wrong, deleted after the fix
```

**Evidence (metadata, interval alignment):**
```
               run_id                | execution_date | data_interval_start
-------------------------------------+----------------+---------------------
 manual__2016-01-01T00:00:00+00:00   | 2016-01-01     | 2015-12-31
 backfill__2017-10-01T00:00:00+00:00 | 2017-10-01     | 2017-10-01
```

**Evidence (Stage 4, after the fix, same manual run):**
```
Done. Returned value was: {'sales_rows': 0, 'holiday_rows': 0, 'year': 2016}
Done. Returned value was: {'status': 'passed', 'errors': 0}
```

## 7. Empty-day rule: header-only incoming CSV is SUCCESS, missing incoming CSV is a failure

**Blueprint said:** The freshness check verifies that the day's `load_date` is actually present (Project 2 §14), and `extract_sales_csv` fails when the incoming file has not arrived (Project 2 §13).

**What we do:** The freshness check now distinguishes three cases:
- incoming CSV for the logical date **missing entirely** → `freshness_missing_incoming_file` (gate fails);
- incoming CSV **exists with data rows** but `raw.sales_daily` for that date has 0 rows → `freshness_raw_missing` (gate fails, the load is broken);
- incoming CSV **exists but is header-only** (legitimate empty day, 0 orders) and raw/staging are 0 rows → **no error, the run succeeds end to end**.

**Reason:** 462 of the 1,096 generated CSVs are header-only. Under the original `raw_count == 0` rule every legitimate zero-order day would have failed the gate, making a full 2016–2018 backfill impossible. The file itself remains the source of truth for "did the day arrive": a missing file is still a hard failure (both at `extract_sales_csv`, which raises `FileNotFoundError`, and at the gate).

**Evidence (Stage 4, empty day runs end to end, ds=2016-01-01):**
```
Done. Returned value was: {'sales_rows': 0, 'holiday_rows': 0, 'year': 2016}
Done. Returned value was: {'staging_sales_rows': 0, 'staging_holiday_rows': 13}
Done. Returned value was: {'orders_upserted': 0, 'items_upserted': 0, 'holidays_upserted': 13}
Done. Returned value was: {'status': 'passed', 'errors': 0}
Done. Returned value was: success
7 of 7 tasks marked SUCCESS, DagRun state=success
```

**Evidence (Stage 4, missing file still fails, file temporarily renamed and restored):**
```
DQ_EXIT=1
EXTRACT_EXIT=1
airflow.exceptions.AirflowException: data quality gate failed: ['freshness_missing_incoming_file']
FileNotFoundError: missing incoming CSV: .../data/incoming/sales/2016-09-05.csv
```

**Evidence (Stage 4, backfill window 2017-01-01 to 2017-01-07: 4 empty days + 3 data days):**
```
finished run 7 of 7 | succeeded: 49 | failed: 0

 load_date  | raw_rows | stg_rows
------------+----------+----------
 2017-01-01 |        0 |        0
 2017-01-02 |        0 |        0
 2017-01-03 |        0 |        0
 2017-01-04 |        0 |        0
 2017-01-05 |       32 |       32
 2017-01-06 |        4 |        4
 2017-01-07 |        5 |        5

 orders | order_items | payments | reviews | holidays
  99441 |      112650 |   103886 |   99224 |       26   (all unchanged)
```

## 8. Analytics SQL mounted read-only from the Project 3 repo

**Blueprint said:** The analytics SQL lives in the Project 3 repo; the DAG runs in the Project 2 container (implementation decision: mount, not copy).

**What we do:** `docker-compose.yml` mounts `../ecommerce-sales-dashboard/sql` at `/opt/airflow/analytics_sql:ro` with path override `ANALYTICS_SQL_DIR` (default `/opt/airflow/analytics_sql`, also in `.env.example` as a placeholder). The Project 3 repo must sit next to this one.

**Evidence (Stage 3, recreate of only the airflow service):**
```
BEFORE: airflow Up 27 hours | metadata Up 27 hours | postgres Up 2 weeks ; runs 1096 ; paused t
AFTER: airflow Up <1s (recreated) | metadata Up 27 hours | postgres Up 2 weeks ; runs 1096 ; paused t
mount lists all 4 SQL files ; printenv → /opt/airflow/analytics_sql
```
A missing mount fails exactly like a missing SQL file: `FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/analytics_sql/load_dimension_tables.sql'` (proven by test, Stage 4 ordering proof).

## 9. Dimension load and fact refresh tasks in the DAG

**Blueprint said:** `load_dimension_tables` → `refresh_fact_materialized_view` between `upsert_production` and `data_quality_check`, two tasks, retries=2 / 5 min each (Project 2 §13, Project 3 §4/§13).

**What we do:** Exactly that, in TaskFlow style (`src/load/load_analytics.py`: whole-file single transaction, counts + durations logged and returned; SQL directory from the environment).

**Evidence (Stage 4, DagBag inspect):**
```
import_errors: {} ; catchup False ; paused_at_creation True ; task_count: 9
all 9 tasks: retries 2, delay 0:05:00 ; edges extracts→load_raw→staging→upsert→dims→fact→dq→notify
dags test 2017-01-05: dims {'seconds': 0.7, counts 860/99441/32951/3095} ; refresh {'fact_rows': 112650, 'seconds': 1.0} ; 9/9 success
window 2017-01-01…07 with 9-task DAG: finished run 7 of 7 | succeeded: 63 | failed: 0 (twice; checksums identical)
```

## 10. Project 3 quality checks in data_quality_check

**Blueprint said:** Delivery-consistency, absolute `fact_sales` = `production.order_items`, surrogate referential checks (Project 3 §14).

**What we do:** `run_analytics_checks(conn, schema="analytics")`: (a) `order_delivered_date_key IS NULL` implies NULL delivery columns and NULL `is_late`; (b) absolute row-count equality; (c) every customer/product/seller key and every non-NULL date key exists in its dimension, plus purchase/estimated keys never NULL (only delivered may be). The relative "stable or growing" check is NOT implemented (needs persisted state the Blueprint does not define). The schema parameter targets scratch copies for failure proofs.

**Evidence (Stage 3–4):**
```
real analytics → [] ; 26 pre-existing unit tests still pass (37 total with new tests)
scratch corruptions → ['analytics_delivery_consistency:2', 'analytics_fact_vs_order_items:112649!=112650', 'analytics_orphan_fact_customer_key:1', 'analytics_null_purchase_date_key:1', 'analytics_null_estimated_date_key:1'] plus the 3 date-key orphan errors on a second copy
git diff: 70 insertions(+), 0 deletions (pre-existing checks byte-untouched)
standalone gate call → AirflowException: data quality gate failed: ['analytics_orphan_fact_customer_key:1']
```

## 11. dags test and backfill run-handling findings (Airflow 2.9.3)

**Found, not designed:** three behaviors with query evidence, no guessing on causes.

(a) `dags test` never increments `try_number`, but retries still fire on schedule: `load_dimension_tables` shows 3 `task_fail` rows at 07:03:56 / 07:08:57 / 07:13:57 (exactly the 5-minute `retry_delay`) followed by success, while its task_instance reads `try_number 0, max_tries 2` with zero `task_reschedule` rows. The limit the counter enforces is therefore never reached under `dags test`.

(b) `dags test <date>` deletes any existing same-date DagRun before creating `manual__`: dates 2017-01-05/06/07 now hold only their `manual__` rows (no `backfill__` rows).

(c) `--reset-dagruns -y` deletes in-range backfill runs, recreates them, and adopts same-date manual runs (type flips to `backfill`, tasks re-executed): `manual__2017-01-05` now reads run_type `backfill`, success. `--help` documents `-y/--yes` ("Do not prompt to confirm") and the deletion behavior. A reset refused against a RUNNING manual run writes nothing (9 ms job, error only): `Backfill cannot be created … as there's already manual in a RUNNING state`.

**Evidence (Stage 4):**
```
run_type/state: backfill success 1094, manual success 2 ; prefixes backfill__ 1091 + manual__ 5 ; distinct dates 1096
window 2017-01-01…07: 6× backfill__ success + manual__2017-01-05 (backfill) + manual__2017-01-06/07 (manual)
evidence run backfill__2016-09-05 success, extract_sales_csv try_number 2 (untouched)
```

## 12. FMDay, IS DISTINCT FROM, and sequence consumption

**Blueprint said:** `TO_CHAR(d, 'Day')`; `ON CONFLICT … DO UPDATE` (Project 3 §10–11).

**What we do (Project 3 SQL, depended on by this repo's tasks):** `FMDay` (no padding; `padded_day_names 0`) and `WHERE … IS DISTINCT FROM …` (tampered row alone rewritten: `INSERT 0 1`, attribute reverted, key kept, checksum unchanged). `ON CONFLICT` consumes sequence values even for conflicting rows: +860/+99,441/+32,951/+3,095 per load run, measured.

## 13. Window re-execution method and dry-run limit

**What we do:** Re-execute dates with `airflow dags backfill -s <start> -e <end> --reset-dagruns -y ecommerce_etl_pipeline` (exact command; `-y` per `--help`). `backfill -n/--dry-run` renders the date and XCom-free tasks but aborts at the first XCom-dependent task (`ValueError: Exactly one of run_id or execution_date must be passed`) — a dry-run limitation, not a DAG defect.

## 14. Relative row-count check not implemented

Carried over from Project 3 scope: the "stable or growing versus previous run" check needs persisted state the Blueprint does not define. The absolute check covers the grain guarantee (112650 = 112650).

## 15. Read-only analyst role powerbi_reader

The Project 3 repo's `create_powerbi_role.sql` creates the role this pipeline's objects are read through: LOGIN, USAGE on `analytics`, SELECT on its tables, default privileges, nothing elsewhere. Detail and evidence live in that repo's decisions; the grant proof here:

```
SELECT on analytics.dim_customers/dim_date/dim_products/dim_sellers (+ fact_sales after Stage 2 re-run)
usage: analytics t, production f, raw f, staging f ; production SELECT as reader → permission denied
```
