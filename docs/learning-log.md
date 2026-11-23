# Learning log

Short, plain-language notes on the ideas behind each phase. Under each one, write the idea
again in your own words. If you can explain it simply, you understand it.

## Phase 1: Foundation

### Random seed

A computer can't really "roll dice". It uses a formula that produces numbers that *look* random.
The **seed** is the starting number for that formula. Same seed → exactly the same "random"
numbers, in the same order, every time.

Our generator takes every random decision (who visits, when, what they buy) from one random
number generator started with the seed (`20261124` by default). So the same seed gives the
same data, down to the last byte. That's what makes our data **reproducible**: anyone can
regenerate exactly the data you tested, and a bug you find today will still be there tomorrow.
A different seed gives a different but equally realistic shop.

My explanation:

### Parquet (and why not CSV)

Parquet is a file format for tables, like CSV, but built for analytics:

- **It stores types.** A CSV is just text: `19.99`, `2026-08-01` and `00042` are all strings
  until someone guesses what they are. Parquet records that `price` is a decimal(12,2) and
  `event_date` is a date, so every tool reads the same values.
- **It is stored by column**, not by row. A query like "total revenue" only reads the
  revenue column and skips the rest. That makes it much faster.
- **It is compressed.** Our 276k events are about 5 MB instead of tens of MB as CSV.
- **"Missing" is clear.** Parquet has a real null. In CSV, an empty cell could be null, an
  empty string or a mistake.

My explanation:

### DuckDB

DuckDB is a database that runs **inside your program**. There's no server to install or start.
The whole database is one file (`data/warehouse/synthetic.duckdb`). It's built for analytics:
it can scan millions of rows quickly, and it can read Parquet files directly with SQL
(`select * from 'data/raw/synthetic/raw_events.parquet'`).

Think of it as "SQLite for analytics". It's great for a project like this, where one person or
one API reads a lot of data but nobody writes to it at the same time.

My explanation:

### dbt: `source()`, `ref()` and staging models

**dbt** turns SQL `select` statements (called *models*) into tables in the database, in the right
order, and tests them. You write the `select`. dbt writes the `create table` for you.

- **`source()`** points at data that dbt did *not* build: our raw Parquet files.
  `{{ source('raw', 'raw_events') }}` means "the raw_events file listed in `_sources.yml`".
  This is where dbt's world starts.
- **`ref()`** points at another dbt model: `{{ ref('stg_events') }}`. dbt sees these links,
  builds models in the right order (staging before facts), and draws the dependency graph.
  We don't use `ref()` yet. Phase 2 models will use `ref('stg_events')`.
- **A staging model** is the first, thin layer on top of a source: one staging model per
  source table. It only cleans the shape: casts to the right types, trims text, renames if
  needed. **No business logic** (no "what counts as an order"). Everything later builds on
  staging, never on the raw files directly. So if the raw format changes, only staging changes.

My explanation:

### What each test proves

**dbt tests** (in `data_pipeline/dbt/models/staging/_staging.yml`, run by `dbt build`):

- **`not_null`** on a column proves that column never has an empty value. For example, every
  event has an `event_id`, a timestamp and a `user_id`.
- **`accepted_values`** proves a column only contains values from a fixed list. For example,
  `device_category` is only `desktop`, `mobile` or `tablet`. `source` may only be the source
  you're building (`COMMERCEPULSE_SOURCE`), which proves sources never mix in one warehouse.
- **`unique`** on `event_id` checks that no event appears twice.
- **`severity: warn`** tests *expect* to find problems: the rows we planted on purpose
  (20 missing session ids, 30 unknown event names, 25 duplicates, 10 purchases without an order
  id). A warning with exactly those counts proves the test can *see* bad data. Phase 3 will
  handle those rows properly.

**pytest tests** (in `data_pipeline/tests/test_generator.py`, run by `uv run pytest`):

- **Same seed twice → identical files.** Proves the generator is deterministic (compares
  SHA-256 fingerprints of the files).
- **Different seed → different data.** Proves the seed really controls the data. It isn't
  ignored, and the data isn't hard-coded.
- **Manifest row counts = Parquet row counts.** Proves the manifest tells the truth about
  the files it describes. Later pipeline steps can trust it for reconciliation.
- **Tiny run works.** Proves the generator doesn't crash on the smallest possible input
  (1 user, 1 session, 1 day, and no products viewed at all).
- **Files follow the contract.** Proves the columns, their order and their Parquet types match
  the data dictionary, every date is inside the manifest's date range, `event_date` is the UTC
  date of `event_timestamp`, every item belongs to an existing event, and order fields
  (transaction id, revenue, currency) appear only on purchases.
- **Planted invalid rows match the manifest.** Proves the "known" bad rows really are known:
  counting them in the files gives exactly the numbers the manifest and the docs promise.
- **Purchase revenue = sum of its items.** Proves the money is consistent: every order's
  revenue equals price × quantity added up over its items. It deals with the planted bad rows
  on purpose (duplicates compared once, negative prices checked to be off purchases), so it
  can't pass or fail by luck.

My explanation:
