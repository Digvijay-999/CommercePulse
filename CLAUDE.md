# CommercePulse: instructions for Claude

## How to talk to me
- **Explain everything simply. I'm a beginner** with Python data tooling, DuckDB and dbt.
  After each step, say in plain language what you did and why.
- Only stop to ask when something is broken or the decision is genuinely mine.

## Project
Retail event analytics portfolio project:
synthetic e-commerce events → Parquet → DuckDB + dbt models and tests → (later) read-only FastAPI → React dashboard.
The plan and the data contract are in `docs/project-brief.md`. Section 5 is the raw data
contract. Never invent or change the contract without asking.

## Fixed decisions (ask before changing any)
- Monorepo: `data_pipeline/`, `backend/`, `frontend/`, `docs/`.
- Python 3.12 managed with uv (`uv add`, commit `uv.lock`). Run everything via `uv run`.
- Generator: pure Python + NumPy, writes Parquet with PyArrow, default seed `20261124`.
  Same seed → byte-identical Parquet (no wall-clock time inside the data; rows sorted before writing).
- Default size ~20k users, ~60k sessions, 90 days, all tunable via CLI args.
- `COMMERCEPULSE_SOURCE` = `synthetic` (default) or `ga4`.
  Raw data → `data/raw/<source>/`, warehouse → `data/warehouse/<source>.duckdb`.
  One DuckDB file per source. Sources never mix.
- Deliberately invalid synthetic rows are kept in staging. dbt tests that they would break use
  `severity: warn` until Phase 3 handles them properly.
- Not yet: Docker, Makefile, API, frontend.

## Folder map
| Path | What |
|---|---|
| `data_pipeline/bootstrap/generate_synthetic.py` | Synthetic data generator (writes Parquet + `manifest.json`) |
| `data_pipeline/dbt/` | dbt project; `profiles.yml` lives here (not in `~/.dbt`) |
| `data_pipeline/dbt/models/staging/` | `_sources.yml`, staging models + their tests |
| `data_pipeline/tests/` | pytest tests |
| `docs/` | `project-brief.md`, `data-dictionary.md`, `learning-log.md` |
| `data/` | Generated, gitignored: `raw/<source>/`, `warehouse/<source>.duckdb` |

## Common commands (run from the repo root)
```sh
uv sync                                                        # install everything from uv.lock
uv run python data_pipeline/bootstrap/generate_synthetic.py    # regenerate synthetic data
uv run dbt build --project-dir data_pipeline/dbt --profiles-dir data_pipeline/dbt   # build + test models
uv run pytest                                                  # Python tests
```
dbt must run from the repo root: the paths in `profiles.yml` and `_sources.yml` are relative to it.

## Do not upgrade without checking
- **`dbt-duckdb==1.11.1` + `duckdb==1.5.5`** (pinned in `pyproject.toml`, locked in `uv.lock`).
  Why: dbt-duckdb 1.11.1 is the newest release (26 Oct 2026). Its package only says `duckdb>=1.0.0`,
  but its own test setup (`tox.ini`) and its MotherDuck extra pin **duckdb 1.5.5**.
  DuckDB 2.0 (21 Oct 2026) is only covered by their nightly "pre-release" CI job.
  So 1.5.5 is the version dbt-duckdb actually stands behind.
- Before upgrading either one, check dbt-duckdb's release notes and `tox.ini` for the DuckDB version
  they test against. Upgrade both together, then rerun the generator, `dbt build` and `pytest`.
- **numpy and pyarrow** (locked in `uv.lock`): "same seed → byte-identical Parquet" only holds with
  the locked versions. NumPy doesn't promise the same random numbers across versions, and every
  Parquet file records the PyArrow version that wrote it. After upgrading either one, expect
  different files for the same seed. That's fine, but say so in the commit.
- dbt-core (1.12.x, dbt Core v1) comes in through dbt-duckdb and is locked by `uv.lock`.
  dbt prints "dbt v2 is available". Ignore that until dbt-duckdb officially supports v2.
