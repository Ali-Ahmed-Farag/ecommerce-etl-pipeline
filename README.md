# Ecommerce ETL Pipeline — Project 2 of 3

A daily Apache Airflow pipeline that lands two **different source types** (bulk historical CSV files and a live public holiday API) into a layered PostgreSQL warehouse (raw → staging → production), with idempotent re-runs, a hard data-quality gate before success, and a deliberate logical-year guard so historical dates are replayed correctly.

Part of a three-repo portfolio:

| # | Repo | Scope |
|---|------|-------|
| 1 | [ecommerce-database-design](https://github.com/Ali-Ahmed-Farag/ecommerce-database-design) | PostgreSQL design + one-time historical seed for Olist (orders, items, payments, reviews, customers…) |
| 2 | **this repo** | Airflow ETL pipeline (CSV + API) that upserts into Project 1's `production` schema |
| 3 | *analytics / Power BI* | Star schema, `fact_sales` materialized view, dashboard — **not built yet** |

## Scope boundary (read this first)

This repository's DAG currently ends at:

```
extract_sales_csv ─┐
                   ├─> load_raw ─> clean_and_validate_staging ─> upsert_production ─> data_quality_check ─> notify_success
extract_holidays_api ┘
```

It does **not** yet contain `load_dimension_tables` or `refresh_fact_materialized_view` (Project 3's tasks), and `data_quality_check` only runs Project 2 checks (staging validation, freshness, staging-vs-production row counts, referential checks). The blueprint's full chain will be completed by Project 3. This truncation is deliberate and documented in [`docs/decisions.md`](docs/decisions.md) (item 5).

## Architecture

```text
             SOURCES                                LAYERS (PostgreSQL: ecommerce)
┌──────────────────────────────┐        ┌──────────────────────────────────────────────┐
│ data/incoming/sales/*.csv    │        │ raw.sales_daily      (load_date partition)   │
│  1,096 daily Olist files     │───┐    │ raw.holidays         (load_year partition)   │
│  (634 with data, 462 empty)  │   │    ├──────────────────────────────────────────────┤
└──────────────────────────────┘   │    │ staging.sales_cleaned (+ holiday flags)      │
┌──────────────────────────────┐   ├───>│ staging.holidays_validated                   │
│ BrasilAPI /feriados/v1/{year}│───┘    ├──────────────────────────────────────────────┤
│  national holidays, cached   │        │ production.orders / order_items / holidays   │
│  per logical year            │        │ (Project 1 tables, UPSERTed)                 │
└──────────────────────────────┘        └──────────────────────────────────────────────┘
                                                    ▲
                       Apache Airflow 2.9.3 (single container, LocalExecutor)
                       own metadata DB: airflow_metadata (never the business DB)
                       UI: http://localhost:8080
```

**Layering rationale:** `raw` is a faithful, immutable-per-partition landing copy (delete-then-insert per `load_date`) so a bad transform never loses source data; `staging` is recomputed from raw every run (cleaning, typing, dedup, holiday flags) so logic changes can be replayed; `production` receives only UPSERTs of already-validated rows keyed on Project 1's natural keys.

## Why holidays (and not currency) as the second source

A second source only earns its place if it **changes the meaning of the primary data**. Brazilian national holidays genuinely do: they explain delivery delays, order-purchase spikes and quiet days in an ecommerce dataset, and the flags land on every sales row (`is_national_holiday`, `is_holiday_week`) where an analyst can actually use them. Exchange rates, by contrast, are decorative for this dataset — every order is denominated in a single currency (BRL), so a FX series would attach to nothing and answer no question the data raises. Holidays are also **time-varying per historical year**, which forces (and demonstrates) the harder engineering problem: a guard keyed on the run's *logical* year rather than the wall clock, so replaying 2016 fetches 2016 holidays even though "today" is 2026.

## Tech stack

Python 3.11, pandas 2.2.2, requests, psycopg2, python-dotenv, Apache Airflow **2.9.3** (pinned, never `latest`), PostgreSQL 18 (Docker, host port 5433), Docker Compose, pytest.

## Repository layout

```text
ecommerce-etl-pipeline/
├── dags/ecommerce_etl_pipeline.py     # TaskFlow DAG: 7 tasks, retries=2 / 5 min
├── src/
│   ├── extract/   extract_csv.py, extract_holidays.py
│   ├── transform/ clean_and_merge.py
│   ├── load/      load_raw.py, load_staging.py, upsert_production.py
│   └── quality/   data_quality_checks.py
├── tests/         test_transform.py (26 unit tests)
├── sql/           create_schemas_and_tables.sql
├── scripts/       generate_incoming_csvs.py   (one-time CSV generation)
├── data/incoming/sales/                         (gitignored)
├── docs/decisions.md   # every deviation from the blueprint, with evidence
├── Dockerfile, docker-compose.yml, requirements.txt, .env.example
└── README.md
```

## How to run

```bash
# 1. generate the daily CSVs (one time, on the host)
python scripts/generate_incoming_csvs.py

# 2. apply the pipeline DDL (raw/staging/production.holidays)
Get-Content sql\create_schemas_and_tables.sql | docker exec -i ecommerce_postgres psql -U postgres -d ecommerce

# 3. build and start (UI on http://localhost:8080)
docker compose up -d --build

# 4. the DAG is created PAUSED on purpose. Replay history with a backfill window:
docker exec ecommerce_airflow airflow dags backfill -s 2017-01-01 -e 2017-01-07 ecommerce_etl_pipeline

# 5. single-day interactive run (does not unpause the DAG)
docker exec ecommerce_airflow airflow dags test ecommerce_etl_pipeline 2016-09-04

# 6. unit tests
docker exec ecommerce_airflow bash -c "cd /opt/airflow/tests && python -m pytest -q"
```

`catchup=False` and `is_paused_upon_creation=True` are set in the DAG; the scheduler never fires runs on its own. Business logic uses `{{ ds }}` / the run's logical date, never the wall clock.

## Data quality gate

`data_quality_check` runs **after** the upsert and before `notify_success` — if it fails, the run fails; no partial or silent success is possible.

- **Staging:** required-field nulls, allowed `order_status` values, `price > 0` on item rows, duplicate detection on `(order_id, order_item_seq, load_date)`, complete item fields.
- **Post-production:** staging-vs-production row counts (orders and items), referential checks (customer/product/seller must exist in Project 1's reference tables), and freshness.
- **Freshness distinguishes three cases** (this is a documented deviation, see [`docs/decisions.md`](docs/decisions.md) item 7):

| Incoming CSV for the logical date | raw rows | Result |
|---|---|---|
| missing entirely | — | **FAIL** `freshness_missing_incoming_file` (extract also raises `FileNotFoundError`) |
| present with data rows | 0 | **FAIL** `freshness_raw_missing` (the load is broken) |
| present but header-only (legitimate empty day) | 0 | **SUCCESS**, 0 rows end to end |

## Idempotency (what happens if you re-run a day)

| Layer | Mechanism | Re-run effect |
|---|---|---|
| raw | delete-then-insert per `load_date` | no duplicate partitions |
| staging | rebuilt from that day's raw partition every run | no accumulation |
| production | `INSERT … ON CONFLICT DO UPDATE` on natural keys | converges to the same end state |
| holidays | logical-year guard + unique index `(holiday_date, COALESCE(state,'__NATIONAL__'))` | no repeat API calls, no duplicate rows |

Verified: the 7-day window `2017-10-01…2017-10-07` was executed **twice**; all per-day staging checksums were byte-identical between runs, and `production` counts did not move.

## Verified end-to-end results

- Backfill `2017-10-01…2017-10-07`: 7/7 runs, **49/49 tasks succeeded, 0 failed** (twice — idempotency proof).
- Backfill `2017-01-01…2017-01-07` (4 empty days + 3 data days): 7/7 runs, **49/49 succeeded, 0 failed**; empty days land 0 rows, data days land 32/4/5 rows.
- Empty day `2016-01-01` via `dags test`: **7/7 tasks success, DagRun success** (quality gate passed with `errors: 0`).
- API path: first 2016 run fetched **13 national holidays** from BrasilAPI → `raw.holidays` (load_year 2016) → staging → `production.holidays`; 2017 was already cached, so those 7 runs returned `None` without calling the API.
- Protected Project 1 counts unchanged after every run: `orders 99441`, `order_items 112650`, `payments 103886`, `reviews 99224`; `production.holidays = 26` (2016 + 2017, both legitimately loaded).
- The DAG's metadata lives in a **separate** database (`airflow_metadata` container), never in the business database.

## Deliberate failure demonstration (blueprint §13)

Two failures were forced deliberately:

1. **Incoming CSV temporarily renamed** (`2016-09-05.csv` → `.bak`): `extract_sales_csv` raised `FileNotFoundError` (exit 1) and, at gate level, `data_quality_check` failed with `['freshness_missing_incoming_file']`. The file was then restored and verified.
2. **Empty day before the freshness fix**: `data_quality_check` raised `AirflowException: data quality gate failed: ['freshness_raw_missing']`, the task went **`up_for_retry`** (retries = 2, delay = 5 min), `notify_success` never executed and the DagRun did not succeed.

Airflow's retry settings (`retries=2`, `retry_delay=5min`) apply to every task, so a transient file/API failure is retried before the run is finally marked failed.

> **Screenshots needed for the README:** (a) Airflow UI grid/log of a successful run, (b) the retry history of the forced failure above. They are taken in the UI at `http://localhost:8080` and dropped into this section.

## Decisions and deviations

Every deviation from the blueprint — staging row identity, nullable item columns, holiday source union, image tag, DAG truncation, the `ds`-vs-`data_interval_start` year bug, and the empty-day rule — is written down with its evidence in [`docs/decisions.md`](docs/decisions.md).
