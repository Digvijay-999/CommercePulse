"""Tests for the synthetic data generator (data_pipeline/bootstrap/generate_synthetic.py).

They run the real command-line script on a small dataset in a temporary folder, the same way you do.
"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

GENERATOR = Path(__file__).resolve().parents[1] / "bootstrap" / "generate_synthetic.py"
SMALL = ["--users", "400", "--sessions", "1200", "--days", "30"]  # big enough to plant every invalid row
PARQUET_FILES = ["raw_events.parquet", "raw_event_items.parquet"]
VALID_EVENT_NAMES = ("session_start", "page_view", "view_item", "add_to_cart", "begin_checkout", "purchase")
# The documented planted rows (docs/data-dictionary.md).
EXPECTED_INVALID_ROWS = {
    "missing_session_id": 20,
    "unknown_event_name": 30,
    "duplicate_event_id": 25,
    "purchase_missing_transaction_id": 10,
    "negative_price": 15,
}
EXPECTED_DUPLICATE_PURCHASES = 5


def generate(out, seed):
    subprocess.run([sys.executable, str(GENERATOR), "--seed", str(seed), "--out", str(out), *SMALL], check=True)
    return out


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_manifest(folder):
    return json.loads((folder / "manifest.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """One small dataset made with seed 1, shared by the tests below."""
    return generate(tmp_path_factory.mktemp("seed1"), seed=1)


@pytest.fixture(scope="module")
def con(run):
    """DuckDB connection with views `ev` (raw_events) and `it` (raw_event_items) over the run's files."""
    con = duckdb.connect()
    con.execute(f"create view ev as select * from read_parquet('{(run / 'raw_events.parquet').as_posix()}')")
    con.execute(f"create view it as select * from read_parquet('{(run / 'raw_event_items.parquet').as_posix()}')")
    yield con
    con.close()


def test_same_seed_gives_identical_files(run, tmp_path):
    again = generate(tmp_path / "again", seed=1)
    for name in PARQUET_FILES:
        assert sha256(run / name) == sha256(again / name), f"{name} differs between two runs with seed 1"


def test_different_seed_gives_different_data(run, tmp_path):
    other = generate(tmp_path / "seed2", seed=2)
    for name in PARQUET_FILES:
        # Compare the data itself, not just the file bytes.
        assert not pq.read_table(run / name).equals(pq.read_table(other / name)), f"{name}: seed 1 == seed 2"


def test_manifest_row_counts_match_parquet(run):
    row_counts = read_manifest(run)["row_counts"]
    assert sorted(row_counts) == sorted(PARQUET_FILES)
    for name in PARQUET_FILES:
        assert row_counts[name] == pq.read_metadata(run / name).num_rows, name


def test_planted_invalid_rows_match_manifest(run, con):
    """The manifest's invalid-row counts are true: counting them in the files gives the same numbers."""
    session_missing, unknown_name, duplicates, txn_missing = con.execute(
        """
        select
            count(*) filter (where session_id is null),
            count(*) filter (where event_name not in ?),
            count(*) - count(distinct event_id),
            count(*) filter (where event_name = 'purchase' and transaction_id is null)
        from ev
        """,
        [list(VALID_EVENT_NAMES)],
    ).fetchone()
    negative_price = con.sql("select count(*) from it where price < 0").fetchone()[0]
    found = {
        "missing_session_id": session_missing,
        "unknown_event_name": unknown_name,
        "duplicate_event_id": duplicates,
        "purchase_missing_transaction_id": txn_missing,
        "negative_price": negative_price,
    }
    manifest = {category: info["rows"] for category, info in read_manifest(run)["invalid_rows"].items()}
    assert found == manifest == EXPECTED_INVALID_ROWS

    # Duplicates copy only the event row, never its items: item keys stay unique.
    assert con.sql("select count(*) = count(distinct (event_id, item_index)) from it").fetchone()[0]


def test_purchase_revenue_equals_sum_of_items(run, con):
    """Every purchase's purchase_revenue equals the sum of price x quantity over its items.

    Planted rows that touch purchases are handled on purpose, so they can't make this test
    pass or fail by accident:
    - duplicate_event_id: exactly 5 purchase rows are exact copies. We assert that, then compare
      once per distinct event_id (otherwise revenue would be counted twice).
    - purchase_missing_transaction_id: these keep their revenue and items. We join on event_id,
      not transaction_id, and assert all 10 of them are among the purchases checked.
    - negative_price: planted only on view_item items. We assert none sit on a purchase.
    """
    # Every item: item_revenue = price x quantity (planted negative prices included).
    assert con.sql("select count(*) from it where item_revenue <> price * quantity").fetchone()[0] == 0

    rows, distinct_ids, distinct_rows = con.sql(
        """
        select count(*), count(distinct event_id), (select count(*) from (select distinct * from ev where event_name = 'purchase'))
        from ev where event_name = 'purchase'
        """
    ).fetchone()
    assert rows - distinct_ids == EXPECTED_DUPLICATE_PURCHASES
    assert read_manifest(run)["invalid_rows"]["duplicate_event_id"]["purchase_rows"] == EXPECTED_DUPLICATE_PURCHASES
    assert distinct_rows == distinct_ids, "duplicate purchase rows must be exact copies"

    negative_on_purchases = con.sql(
        """
        select count(*) from it
        where price < 0 and event_id in (select event_id from ev where event_name = 'purchase')
        """
    ).fetchone()[0]
    assert negative_on_purchases == 0

    checked, mismatches, missing_txn_checked = con.sql(
        """
        with purchases as (
            select distinct event_id, transaction_id, purchase_revenue from ev where event_name = 'purchase'
        ),
        item_totals as (
            select event_id, sum(price * quantity) as items_revenue from it group by event_id
        )
        select
            count(*),
            count(*) filter (where items_revenue is distinct from purchase_revenue),
            count(*) filter (where transaction_id is null)
        from purchases left join item_totals using (event_id)
        """
    ).fetchone()
    assert checked == distinct_ids > 0
    assert missing_txn_checked == EXPECTED_INVALID_ROWS["purchase_missing_transaction_id"]
    assert mismatches == 0
