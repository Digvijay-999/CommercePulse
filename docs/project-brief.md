# CommercePulse — Project Brief

> Working brief for the CommercePulse repo. Reconstructed from the build plan (Nov 2026).
> If an original brief exists, it wins wherever the two disagree.

## 1. Summary

CommercePulse is a retail event analytics platform. It turns raw e-commerce events
(page views, add-to-carts, checkouts, purchases) into tested, trustworthy business
metrics, served through a read-only API and shown on a dashboard.

Every number on the dashboard must be traceable back to raw rows.

## 2. Goals

- Generate realistic, **deterministic** synthetic e-commerce data (same seed → same data).
- Model it with dbt on DuckDB into facts and marts with a **documented grain**.
- Separate real pipeline failures (fail loudly) from tolerated source quirks (report, don't crash).
- Serve metrics through a **read-only**, validated FastAPI API.
- Show them on a React dashboard that always says which data source it is showing.
- Demonstrate an A/B test on a separate, clearly labelled synthetic experiment dataset.
- Optionally import the public GA4 e-commerce sample from BigQuery into the same contract.

## 3. Non-goals

- No live business data, no user accounts or login, no write endpoints.
- No arbitrary SQL from the API or the UI.
- No causal claims from observational data (only the randomized experiment supports cause and effect).
- No requirement for a cloud account. Everything must run locally on synthetic data.

## 4. Architecture (data only moves downward)

```
generator / GA4 importer
        ↓  Parquet files + manifest.json      (data/raw/<source>/)
DuckDB + dbt
        ↓  staging → intermediate → facts → marts   (data/warehouse/<source>.duckdb)
quality checks + reconciliation
        ↓
FastAPI (read_only=True)   /api/v1/...
        ↓
React dashboard
```

- `COMMERCEPULSE_SOURCE` = `synthetic` (default) or `ga4`.
- One DuckDB file per source. Sources are never mixed in one table.

## 5. Canonical raw data contract

Every source (synthetic generator or GA4 importer) must write exactly these two tables
as Parquet, plus a `manifest.json`.

### 5.1 `raw_events` — grain: one row per event

| Column | Type | Nullable | Meaning / rules |
|---|---|---|---|
| `event_id` | string | no | Unique ID for the event. Deterministic for synthetic data. |
| `source` | string | no | `synthetic` or `ga4`. |
| `event_timestamp` | timestamp (UTC) | no | When the event happened. |
| `event_date` | date | no | UTC date of `event_timestamp`. |
| `event_name` | string | no | One of: `session_start`, `page_view`, `view_item`, `add_to_cart`, `begin_checkout`, `purchase`. |
| `user_id` | string | no* | Pseudonymous user ID (GA4: `user_pseudo_id`). |
| `session_id` | string | no* | Session ID. A session = unique `(user_id, session_id)` pair. |
| `device_category` | string | no | `desktop`, `mobile`, `tablet`. |
| `country` | string | yes | Country name. |
| `traffic_source` | string | yes | e.g. `google`, `(direct)`, `newsletter`. |
| `traffic_medium` | string | yes | e.g. `organic`, `cpc`, `email`, `referral`, `(none)`. |
| `traffic_campaign` | string | yes | Campaign name, if any. |
| `transaction_id` | string | yes | Order ID. Required on `purchase` events, null otherwise. |
| `purchase_revenue` | decimal(12,2) | yes | Order revenue. Purchase events only. Must be ≥ 0. |
| `currency` | string | yes | ISO code, e.g. `USD`. Purchase events only. |

\* Required by the contract. Rows that break this are counted and reported as quality issues, not silently dropped.

### 5.2 `raw_event_items` — grain: one row per item inside an event

| Column | Type | Nullable | Meaning / rules |
|---|---|---|---|
| `event_id` | string | no | Links to `raw_events.event_id`. |
| `item_index` | integer | no | Position of the item within the event (0, 1, 2…). |
| `item_id` | string | no | Product ID. |
| `item_name` | string | no | Product name. |
| `item_category` | string | no | Product category. |
| `price` | decimal(12,2) | no | Unit price. Must be ≥ 0. |
| `quantity` | integer | no | Must be > 0. |
| `item_revenue` | decimal(12,2) | no | `price × quantity`. |

Items appear on `view_item`, `add_to_cart`, `begin_checkout` and `purchase` events.
`(event_id, item_index)` is unique.

### 5.3 `manifest.json`

Written next to the Parquet files:

- `source`, `seed` (synthetic only), `start_date`, `end_date`
- `generated_at` (or `imported_at`), the only field allowed to differ between identical runs
- row counts per file
- counts of deliberately invalid rows by category (synthetic only)

### 5.4 Synthetic data rules

- Default seed `20261124`; ~20k users, ~60k sessions, 90 days; all tunable.
- Journeys follow view → cart → checkout → purchase with realistic drop-off at each step.
- For a purchase, `purchase_revenue` = sum of that event's `item_revenue`.
- Includes a small, known, documented set of invalid rows, e.g. missing `session_id`,
  negative price, unknown `event_name`, duplicate `event_id`, purchase without `transaction_id`.
- Same seed → byte-identical Parquet files.

## 6. Metric definitions

All metrics live in `docs/metric-definitions.md` with example SQL. The API returns a
`metric_version` with every response.

| Metric | Definition |
|---|---|
| Sessions | Count of distinct valid `(user_id, session_id)` pairs. |
| Users | Count of distinct `user_id`. |
| Orders | Count of distinct orders (by `transaction_id`; documented fallback if missing). |
| Revenue | Sum of order-level `purchase_revenue`, one value per order. |
| Average order value (AOV) | Revenue ÷ Orders. |
| Session conversion rate | Sessions with ≥ 1 purchase ÷ Sessions. |
| Funnel step rates | Share of sessions reaching each step **in strict timestamp order** within the same session: view_item → add_to_cart → begin_checkout → purchase. |
| Purchase cohorts | Users grouped by month of first purchase; % who purchase again in month 1, 2, 3… Months not fully observable yet are flagged, never shown as 0%. |
| Product revenue | Sum of `item_revenue` on purchase events, per product / category. |
| Channel performance | Sessions, orders, revenue and conversion by source/medium, using a documented, simplified channel mapping. |

**Rule:** order-level revenue and item-level revenue are never summed together.

## 7. Data quality

- **Structural failures** (pipeline bugs) fail the build: e.g. duplicate primary keys in facts,
  orphan items, manifest vs Parquet count mismatch.
- **Tolerated source quirks** are `warn` tests and land in a `quality_summary` table with counts.
- A reconciliation script checks counts and revenue from manifest → raw → staging → marts.
- Every deliberately invalid synthetic row must show up in the quality report.

## 8. API (read-only, `/api/v1`)

| Endpoint | Returns |
|---|---|
| `GET /overview` | KPI cards + daily trend |
| `GET /funnel` | Funnel steps and step rates |
| `GET /cohorts` | Cohort matrix with completeness flags |
| `GET /products` | Product table, paginated, whitelisted sort fields |
| `GET /channels` | Channel table |
| `GET /experiments/{id}` | Experiment results (stub until the experiment exists) |
| `GET /metadata` | Source, date coverage, last refresh, metric version |
| `GET /health/ready` | Readiness check |

- Shared filters: date range, device, channel, category, each validated against allowed values.
- Invalid input returns 422. DuckDB opened with `read_only=True`. Parameterized SQL only.
- Every response includes the source name and `metric_version`.

## 9. Dashboard

React + TypeScript + Vite, Recharts, TanStack Query. Data comes only from the API.

- Always-visible header: source badge, date coverage, last refresh.
- Global filters (date, device, channel) stored in the URL.
- Pages: **Overview** (KPIs, daily trend, definition tooltips), **Funnel**, **Cohorts**
  (incomplete cells visibly hatched), **Products & Channels** (sortable tables),
  **Data Quality & Source**, **Experiment**.
- Every chart has a title, units, an empty state and an `aria-label`.

## 10. Experiment (synthetic A/B test)

- Separate generator with its own seed, user-level random assignment, and a known positive effect.
- Stored separately; never appears in observational marts.
- Reports conversion per arm, absolute and relative lift, and a 95% two-proportion
  confidence interval (normal approximation), with the method stated.
- The Experiment page shows a permanent "Synthetic demonstration" banner.

## 11. Optional: GA4 importer

- Imports the public GA4 obfuscated e-commerce sample (Nov 2020 – Jan 2021) from BigQuery.
- Needs `GCP_PROJECT_ID`; credentials stay in `.env` and are never logged.
- Filters tables by date to control cost; flattens nested params and items into section 5.
- UI banner: "Obfuscated sample, Nov 2020 – Jan 2021, not a live business".

## 12. Acceptance criteria

1. Same seed twice → identical synthetic files.
2. From a fresh clone, one documented command regenerates data and builds all models.
3. Manifest row counts equal Parquet row counts.
4. Every model documents its grain.
5. Order count and revenue match a hand-written SQL query on raw data.
6. A session with checkout before add-to-cart does not advance in the funnel.
7. Order revenue and item revenue are never added together; product revenue reconciles to raw item revenue.
8. Incomplete cohort months are flagged, not shown as 0%.
9. API KPIs match independent SQL for at least 3 filter combinations; injection-style input returns 422.
10. Every dashboard page shows which source is selected.
11. The experiment shows treatment beating control, with an interval reported.
12. The whole stack runs locally without a cloud account.

## 13. Tech stack

Python 3.12 (uv) · NumPy · PyArrow · DuckDB · dbt-duckdb · FastAPI · Pydantic · pytest ·
React · TypeScript · Vite · Recharts · TanStack Query · Vitest · Docker Compose (Phase 6) ·
GitHub Actions (Phase 7)