# CommercePulse

Retail event analytics portfolio project. It turns raw e-commerce events (page views,
add-to-carts, checkouts, purchases) into tested, trustworthy business metrics.

```
synthetic events (Python + NumPy) → Parquet + manifest.json → DuckDB + dbt models and tests
→ (later phases) read-only FastAPI → React dashboard
```

The full plan is in [docs/project-brief.md](docs/project-brief.md). Column meanings are in
[docs/data-dictionary.md](docs/data-dictionary.md).

**Status:** Phase 1 (data foundation): generator, raw data contract, dbt staging models, tests.

## Run Phase 1

You need [uv](https://docs.astral.sh/uv/). It installs Python 3.12 and every package for you.
Run all commands from the repo root.

```sh
uv sync                                                        # install Python 3.12 + packages (from uv.lock)
uv run python data_pipeline/bootstrap/generate_synthetic.py    # write data/raw/synthetic/*.parquet + manifest.json
uv run dbt build --project-dir data_pipeline/dbt --profiles-dir data_pipeline/dbt   # build + test data/warehouse/synthetic.duckdb
uv run pytest                                                  # generator tests
```

Generator options: `--seed` (default 20261124), `--users` (20000), `--sessions` (60000),
`--days` (90), `--start-date` (2026-08-01), `--out` (data/raw/synthetic).

Short dbt command: `cp .env.example .env` once, then `uv run --env-file .env dbt build`.

`COMMERCEPULSE_SOURCE` (`synthetic` or `ga4`, default `synthetic`) picks which source dbt builds.
Each source has its own raw folder and its own DuckDB file. They never mix.

## Layout

| Folder | What's in it |
|---|---|
| `data_pipeline/bootstrap/` | Synthetic data generator |
| `data_pipeline/dbt/` | dbt project (DuckDB) |
| `data_pipeline/tests/` | pytest tests for the generator |
| `docs/` | Brief, data dictionary, learning log |
| `backend/`, `frontend/` | Empty until later phases |
| `data/` | Generated files (not committed) |
