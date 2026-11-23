"""Generate deterministic synthetic e-commerce events for CommercePulse.

Writes raw_events.parquet, raw_event_items.parquet and manifest.json, following the raw data
contract in docs/project-brief.md section 5 (column details: docs/data-dictionary.md).

Same --seed and options -> byte-identical Parquet files. Only manifest.json's generated_at changes.

Run from the repo root:
    uv run python data_pipeline/bootstrap/generate_synthetic.py --help
"""

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = "synthetic"
DEFAULT_SEED = 20261124

US_PER_SECOND = 1_000_000  # timestamps are stored as microseconds since 1970-01-01 UTC
US_PER_HOUR = 3600 * US_PER_SECOND
US_PER_DAY = 24 * US_PER_HOUR

VALID_EVENT_NAMES = ("session_start", "page_view", "view_item", "add_to_cart", "begin_checkout", "purchase")

# --- Catalog and traffic ------------------------------------------------------------------
# (category, lowest price USD, highest price USD, product nouns): one product per noun.
CATEGORIES = [
    ("Apparel", 15, 90, ["T-Shirt", "Hoodie", "Jeans", "Jacket", "Sweater", "Shorts", "Dress", "Cap",
                         "Scarf", "Polo Shirt", "Chinos", "Raincoat", "Cardigan", "Vest", "Pajama Set"]),
    ("Footwear", 40, 160, ["Running Shoes", "Sneakers", "Boots", "Sandals", "Loafers", "Slippers",
                           "Hiking Boots", "Trail Runners", "High Tops", "Clogs"]),
    ("Electronics", 25, 600, ["Headphones", "Earbuds", "Smartwatch", "Bluetooth Speaker", "Tablet",
                              "E-Reader", "Webcam", "Keyboard", "Mouse", "Power Bank", "Monitor", "Charger"]),
    ("Home & Kitchen", 10, 200, ["Coffee Maker", "Blender", "Knife Set", "Frying Pan", "Mug Set",
                                 "Toaster", "Cutting Board", "Bedsheet Set", "Table Lamp", "Throw Pillow",
                                 "Storage Box", "Kettle"]),
    ("Beauty", 8, 60, ["Face Cream", "Shampoo", "Lip Balm", "Perfume", "Sunscreen", "Hair Dryer",
                       "Nail Kit", "Face Mask", "Body Lotion", "Conditioner"]),
    ("Sports & Outdoors", 15, 250, ["Yoga Mat", "Dumbbell Set", "Water Bottle", "Backpack", "Tent",
                                    "Sleeping Bag", "Bike Helmet", "Jump Rope", "Camping Stove", "Fitness Band"]),
    ("Toys & Games", 10, 80, ["Puzzle", "Board Game", "Building Blocks", "Plush Bear", "Card Game",
                              "RC Car", "Art Set", "Kite", "Model Kit", "Dollhouse"]),
    ("Books", 8, 40, ["Cookbook", "Mystery Novel", "Travel Guide", "Biography", "Sci-Fi Novel",
                      "Children's Book", "History Book", "Poetry Collection", "Self-Help Book", "Comic Book"]),
]
ADJECTIVES = ["Classic", "Everyday", "Premium", "Urban", "Essential", "Pro", "Lightweight", "Vintage"]

# (source, medium, campaign, share of sessions, how much more/less likely to add to cart)
CHANNELS = [
    ("google", "organic", None, 0.30, 1.0),
    ("(direct)", "(none)", None, 0.20, 1.2),
    ("google", "cpc", "brand_search", 0.10, 1.4),
    ("google", "cpc", "generic_search", 0.08, 0.8),
    ("bing", "organic", None, 0.05, 1.0),
    ("newsletter", "email", "weekly_newsletter", 0.10, 1.5),
    ("facebook", "cpc", "autumn_sale", 0.12, 0.7),
    ("partner-blog.example", "referral", None, 0.05, 0.9),
]
# (device, share of users, add-to-cart multiplier): mobile shoppers convert less, like in real shops.
DEVICES = [("desktop", 0.42, 1.25), ("mobile", 0.50, 0.80), ("tablet", 0.08, 1.0)]
COUNTRIES = [("United States", 0.38), ("United Kingdom", 0.12), ("Canada", 0.08), ("Germany", 0.09),
             ("France", 0.07), ("Australia", 0.06), ("India", 0.08), ("Netherlands", 0.04),
             ("Spain", 0.04), ("Brazil", 0.04)]
# Relative traffic per UTC hour (quiet at night, busiest in the evening) and per weekday (Mon..Sun).
HOUR_WEIGHTS = np.array([2, 1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 6, 6, 6, 6, 6, 7, 8, 9, 10, 10, 9, 6, 4], float)
WEEKDAY_WEIGHTS = np.array([1.0, 0.97, 0.97, 1.0, 1.05, 1.15, 1.12])

# --- Funnel: chance of reaching each step, given the previous step was reached --------------
P_BROWSE = 0.62     # views at least one product (the rest leave after the landing page)
P_CART = 0.17       # adds to cart (times the device and channel multipliers above)
P_CHECKOUT = 0.55   # starts checkout
P_PURCHASE = 0.62   # completes the purchase

# --- Deliberately invalid rows: fixed counts, documented in docs/data-dictionary.md ---------
# Each planted row has exactly one problem. Proper handling comes in Phase 3.
UNKNOWN_EVENT_NAME = "add_to_wishlist"
INVALID_ROWS = {
    # category: (file, rows to plant, description written to manifest.json)
    "missing_session_id": (
        "raw_events.parquet", 20, "page_view events whose session_id was set to null."),
    "unknown_event_name": (
        "raw_events.parquet", 30,
        f"Extra events named '{UNKNOWN_EVENT_NAME}' (not in the contract), inside real sessions, with no items."),
    "duplicate_event_id": (
        "raw_events.parquet", 25,
        "Exact copies of existing raw_events rows (same event_id); 'purchase_rows' of them copy purchase "
        "events. Only the raw_events row is duplicated, never its items in raw_event_items."),
    "purchase_missing_transaction_id": (
        "raw_events.parquet", 10, "purchase events whose transaction_id was set to null (revenue and items kept)."),
    "negative_price": (
        "raw_event_items.parquet", 15,
        "Items on view_item events whose price was made negative (item_revenue = price x quantity, so also "
        "negative). Never on purchase events, so order revenue is not affected."),
}
DUPLICATE_PURCHASES = 5  # how many of the duplicate_event_id rows copy purchase events

# Row layouts while generating. Prices and revenue are whole cents (ints) to avoid float rounding.
# `source` and `event_date` are added when writing.
EVENT_COLUMNS = ["event_id", "event_timestamp", "event_name", "user_id", "session_id", "device_category",
                 "country", "traffic_source", "traffic_medium", "traffic_campaign", "transaction_id",
                 "purchase_revenue", "currency"]
ITEM_COLUMNS = ["event_id", "item_index", "item_id", "item_name", "item_category", "price", "quantity",
                "item_revenue"]
EV = {name: i for i, name in enumerate(EVENT_COLUMNS)}  # e.g. row[EV["session_id"]]
IT = {name: i for i, name in enumerate(ITEM_COLUMNS)}


def to_us(day):
    """Microseconds from 1970-01-01 UTC to midnight UTC of `day`."""
    return (day - date(1970, 1, 1)).days * US_PER_DAY


def make_products(rng):
    """Return the catalog [(item_id, name, category, price_cents)] and each product's popularity."""
    products = []
    for category, low, high, nouns in CATEGORIES:
        for noun in nouns:
            dollars = np.exp(rng.uniform(np.log(low), np.log(high)))  # more cheap items than expensive ones
            price_cents = max(int(round(dollars)), 1) * 100 - 1        # shop prices end in .99
            name = f"{ADJECTIVES[rng.integers(len(ADJECTIVES))]} {noun}"
            products.append((f"prd_{len(products) + 1:04d}", name, category, price_cents))
    # A few best sellers and a long tail of slow sellers, like a real shop.
    popularity = 1.0 / rng.permutation(np.arange(1, len(products) + 1)) ** 0.9
    return products, popularity / popularity.sum()


def make_sessions(rng, n_users, n_sessions, start, days):
    """Decide who visits, when, from where and how far down the funnel they get.

    Returns a dict of NumPy arrays with one entry per session, sorted by start time.
    """
    shares = lambda table, col: np.array([row[col] for row in table])
    user_country = rng.choice(len(COUNTRIES), n_users, p=shares(COUNTRIES, 1))
    user_device = rng.choice(len(DEVICES), n_users, p=shares(DEVICES, 1))

    # Every user gets one session. The rest mostly go to a few very active users.
    activity = rng.gamma(0.7, size=n_users)
    user = np.concatenate([np.arange(n_users),
                           rng.choice(n_users, n_sessions - n_users, p=activity / activity.sum())])

    # Start time: busier weekends, slow growth over the period, busier evenings. Millisecond precision.
    weekday = np.array([(start + timedelta(days=d)).weekday() for d in range(days)])
    day_weights = WEEKDAY_WEIGHTS[weekday] * np.linspace(1.0, 1.25, days)
    day = rng.choice(days, n_sessions, p=day_weights / day_weights.sum())
    hour = rng.choice(24, n_sessions, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    start_us = to_us(start) + day * US_PER_DAY + hour * US_PER_HOUR + rng.integers(0, 3_600_000, n_sessions) * 1000

    # People mostly use their usual device.
    device = np.where(rng.random(n_sessions) < 0.85, user_device[user],
                      rng.choice(len(DEVICES), n_sessions, p=shares(DEVICES, 1)))
    channel = rng.choice(len(CHANNELS), n_sessions, p=shares(CHANNELS, 3))

    # Funnel: each step can only happen if the previous one did.
    p_cart = np.minimum(P_CART * shares(DEVICES, 2)[device] * shares(CHANNELS, 4)[channel], 0.95)
    browses = rng.random(n_sessions) < P_BROWSE
    carts = browses & (rng.random(n_sessions) < p_cart)
    checkouts = carts & (rng.random(n_sessions) < P_CHECKOUT)
    purchases = checkouts & (rng.random(n_sessions) < P_PURCHASE)

    sessions = {
        "user": user, "country": user_country[user], "device": device, "channel": channel,
        "start_us": start_us, "extra_pages": rng.integers(0, 3, n_sessions),
        "browses": browses, "carts": carts, "checkouts": checkouts, "purchases": purchases,
    }
    order = np.argsort(start_us, kind="stable")  # session IDs follow start time
    return {key: values[order] for key, values in sessions.items()}


def make_events(rng, products, popularity, sessions, end_us):
    """Turn sessions into event rows and item rows (lists, columns as in EVENT_COLUMNS / ITEM_COLUMNS)."""
    events, items = [], []
    n_orders = 0
    for i in range(len(sessions["user"])):
        names = ["session_start", "page_view"] + ["page_view"] * int(sessions["extra_pages"][i])
        viewed, cart, quantities = [], [], []
        if sessions["browses"][i]:
            viewed = list(rng.choice(len(products), size=min(1 + rng.poisson(1.2), 6), replace=False, p=popularity))
            names += ["view_item"] * len(viewed)
            if sessions["carts"][i]:
                cart = viewed[:min(rng.choice([1, 2, 3], p=[0.7, 0.25, 0.05]), len(viewed))]
                quantities = [int(q) for q in rng.choice([1, 2, 3], size=len(cart), p=[0.8, 0.15, 0.05])]
                names += ["add_to_cart"] * len(cart)
                if sessions["checkouts"][i]:
                    names.append("begin_checkout")
                if sessions["purchases"][i]:
                    names.append("purchase")

        # Time between events: at least 1 s, about 26 s on average, millisecond precision.
        # session_start and the landing page_view happen at the same moment.
        gaps = np.round(1000 + rng.exponential(25_000, len(names))).astype(np.int64) * 1000
        gaps[:2] = 0
        offsets = np.cumsum(gaps)
        # Keep the whole session inside the date range (only matters late on the last day).
        t0 = min(int(sessions["start_us"][i]), end_us - 1000 - int(offsets[-1]))  # 1000 us = 1 ms

        source, medium, campaign = CHANNELS[sessions["channel"][i]][:3]
        context = [f"usr_{sessions['user'][i] + 1:06d}", f"ses_{i + 1:07d}", DEVICES[sessions["device"][i]][0],
                   COUNTRIES[sessions["country"][i]][0], source, medium, campaign]
        viewed_left = iter(viewed)
        added_left = iter(zip(cart, quantities))
        for name, offset in zip(names, offsets):
            event_id = f"evt_{len(events) + 1:09d}"
            if name == "view_item":
                event_items = [(next(viewed_left), 1)]
            elif name == "add_to_cart":
                event_items = [next(added_left)]
            elif name in ("begin_checkout", "purchase"):
                event_items = list(zip(cart, quantities))
            else:
                event_items = []
            for index, (product, quantity) in enumerate(event_items):
                item_id, item_name, category, price = products[product]
                items.append([event_id, index, item_id, item_name, category, price, quantity, price * quantity])

            transaction_id = revenue = currency = None
            if name == "purchase":
                n_orders += 1
                transaction_id = f"txn_{n_orders:06d}"
                revenue = sum(products[p][3] * q for p, q in event_items)  # order revenue = sum of item revenue
                currency = "USD"
            events.append([event_id, t0 + int(offset), name, *context, transaction_id, revenue, currency])
    return events, items


def plant_invalid_rows(rng, events, items):
    """Break a few rows on purpose, in the documented ways (INVALID_ROWS). Returns what was planted.

    Each planted row has exactly one problem: rows already planted are never picked again.
    If the data is too small, fewer rows are planted. The manifest records the real counts.
    """
    planted = set()  # event_ids already used for a planted problem

    def pick(rows, n):
        chosen = rng.choice(len(rows), size=min(n, len(rows)), replace=False)
        return [rows[i] for i in np.sort(chosen)]

    def unplanted(event_name):
        return [e for e in events if e[EV["event_name"]] == event_name and e[EV["event_id"]] not in planted]

    def target(category):
        return INVALID_ROWS[category][1]

    counts = {}
    rows = pick(unplanted("page_view"), target("missing_session_id"))
    for e in rows:
        e[EV["session_id"]] = None
    planted.update(e[EV["event_id"]] for e in rows)
    counts["missing_session_id"] = len(rows)

    rows = pick(unplanted("purchase"), target("purchase_missing_transaction_id"))
    for e in rows:
        e[EV["transaction_id"]] = None
    planted.update(e[EV["event_id"]] for e in rows)
    counts["purchase_missing_transaction_id"] = len(rows)

    view_item_ids = {e[EV["event_id"]] for e in unplanted("view_item")}
    rows = pick([it for it in items if it[IT["event_id"]] in view_item_ids], target("negative_price"))
    for it in rows:
        it[IT["price"]] = -it[IT["price"]]
        it[IT["item_revenue"]] = -it[IT["item_revenue"]]
    planted.update(it[IT["event_id"]] for it in rows)  # keep their events out of the other categories
    counts["negative_price"] = len(rows)

    # New events in real sessions, half a second after a product view, with an unknown name.
    rows = []
    for e in pick(unplanted("view_item"), target("unknown_event_name")):
        extra = list(e)
        extra[EV["event_id"]] = f"evt_{len(events) + len(rows) + 1:09d}"
        extra[EV["event_timestamp"]] += 500_000
        extra[EV["event_name"]] = UNKNOWN_EVENT_NAME
        rows.append(extra)
    events.extend(rows)
    planted.update(e[EV["event_id"]] for e in rows)
    counts["unknown_event_name"] = len(rows)

    # Exact copies of events, like a tracker sending the same hit twice. Items are NOT copied.
    rows = pick(unplanted("purchase"), DUPLICATE_PURCHASES)
    duplicate_purchases = len(rows)
    others = [e for e in events if e[EV["event_name"]] != "purchase" and e[EV["event_id"]] not in planted]
    rows += pick(others, target("duplicate_event_id") - duplicate_purchases)
    events.extend(list(e) for e in rows)
    counts["duplicate_event_id"] = len(rows)

    result = {}
    for category, (file, _, description) in INVALID_ROWS.items():
        result[category] = {"file": file, "rows": counts[category], "description": description}
    result["duplicate_event_id"]["purchase_rows"] = duplicate_purchases
    return result


def cents_to_decimal(values):
    """Whole cents (int) -> exact decimal(12,2) Arrow array, e.g. 1999 -> 19.99. None stays null."""
    return pa.array([None if c is None else Decimal(c).scaleb(-2) for c in values], pa.decimal128(12, 2))


def write_parquet(events, items, out):
    """Write both tables with explicit Arrow types, so the files match the contract exactly."""
    ev = dict(zip(EVENT_COLUMNS, zip(*events)))  # list of rows -> one tuple per column
    it = dict(zip(ITEM_COLUMNS, zip(*items)))
    string = lambda values: pa.array(values, pa.string())
    events_table = pa.table({
        "event_id": string(ev["event_id"]),
        "source": string([SOURCE] * len(events)),
        "event_timestamp": pa.array(ev["event_timestamp"], pa.timestamp("us", tz="UTC")),
        "event_date": pa.array([t // US_PER_DAY for t in ev["event_timestamp"]], pa.date32()),
        **{name: string(ev[name]) for name in EVENT_COLUMNS[2:11]},  # event_name .. transaction_id
        "purchase_revenue": cents_to_decimal(ev["purchase_revenue"]),
        "currency": string(ev["currency"]),
    })
    items_table = pa.table({
        "event_id": string(it["event_id"]),
        "item_index": pa.array(it["item_index"], pa.int32()),
        "item_id": string(it["item_id"]),
        "item_name": string(it["item_name"]),
        "item_category": string(it["item_category"]),
        "price": cents_to_decimal(it["price"]),
        "quantity": pa.array(it["quantity"], pa.int32()),
        "item_revenue": cents_to_decimal(it["item_revenue"]),
    })
    pq.write_table(events_table, out / "raw_events.parquet")
    pq.write_table(items_table, out / "raw_event_items.parquet")
    return {"raw_events.parquet": events_table.num_rows, "raw_event_items.parquet": items_table.num_rows}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate deterministic synthetic e-commerce events.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="random seed (default: %(default)s)")
    parser.add_argument("--users", type=int, default=20_000, help="number of users (default: %(default)s)")
    parser.add_argument("--sessions", type=int, default=60_000, help="number of sessions (default: %(default)s)")
    parser.add_argument("--days", type=int, default=90, help="length of the date range (default: %(default)s)")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 8, 1),
                        help="first day, YYYY-MM-DD (default: %(default)s)")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "data" / "raw" / SOURCE,
                        help="output folder (default: data/raw/synthetic)")
    args = parser.parse_args(argv)
    if args.seed < 0 or args.users < 1 or args.days < 1:
        parser.error("--seed must be >= 0, --users and --days must be >= 1")
    if args.sessions < args.users:
        parser.error("--sessions must be >= --users (every user has at least one session)")
    return args


def main(argv=None):
    args = parse_args(argv)
    rng = np.random.default_rng(args.seed)  # the ONLY source of randomness, so the seed decides everything

    products, popularity = make_products(rng)
    sessions = make_sessions(rng, args.users, args.sessions, args.start_date, args.days)
    end_us = to_us(args.start_date + timedelta(days=args.days))  # exclusive: midnight after the last day
    events, items = make_events(rng, products, popularity, sessions, end_us)
    invalid_rows = plant_invalid_rows(rng, events, items)

    # Sort so the same data is always written in the same order (needed for byte-identical files).
    events.sort(key=lambda e: (e[EV["event_timestamp"]], e[EV["event_id"]]))
    items.sort(key=lambda it: (it[IT["event_id"]], it[IT["item_index"]]))

    args.out.mkdir(parents=True, exist_ok=True)
    row_counts = write_parquet(events, items, args.out)
    manifest = {
        "source": SOURCE,
        "seed": args.seed,
        "start_date": args.start_date.isoformat(),
        "end_date": (args.start_date + timedelta(days=args.days - 1)).isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),  # only field that varies
        "parameters": {"users": args.users, "sessions": args.sessions, "days": args.days},
        "row_counts": row_counts,
        "invalid_rows": invalid_rows,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote {args.out}")
    for file, rows in row_counts.items():
        print(f"  {file}: {rows:,} rows")
    print("  planted invalid rows: " + ", ".join(f"{k}={v['rows']}" for k, v in invalid_rows.items()))


if __name__ == "__main__":
    main()
