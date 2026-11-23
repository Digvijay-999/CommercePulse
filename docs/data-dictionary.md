# Data dictionary: raw data contract

Source of truth: [project-brief.md](project-brief.md), section 5. This file adds examples,
valid values and the exact Parquet types. If the two ever disagree, the brief wins.

Every source (the synthetic generator now, the GA4 importer later) writes these files to
`data/raw/<source>/`:

| File | Grain (what one row is) |
|---|---|
| `raw_events.parquet` | One event |
| `raw_event_items.parquet` | One item inside an event |
| `manifest.json` | Run summary: source, seed, date range, row counts, planted invalid rows |

"Nullable = no*" means the contract requires a value, but a source can still break that rule.
Such rows are **counted and reported** as quality issues, never silently dropped.

## `raw_events`

Grain: one row per event. `event_id` should be unique (see the planted duplicates below).

| Column | Contract type (Parquet type) | Nullable | Meaning | Example | Valid values |
|---|---|---|---|---|---|
| `event_id` | string (`string`) | no | Unique ID of the event. Deterministic for synthetic data. | `evt_000004211` | Synthetic: `evt_` + 9 digits |
| `source` | string (`string`) | no | Which source produced the row. | `synthetic` | `synthetic`, `ga4` |
| `event_timestamp` | timestamp UTC (`timestamp[us, tz=UTC]`) | no | When the event happened, in UTC, microsecond precision. | `2026-08-14 19:03:27.512000+00` | Inside the manifest's `start_date`…`end_date` |
| `event_date` | date (`date32`) | no | UTC calendar date of `event_timestamp`. | `2026-08-14` | Always equals `event_timestamp`'s UTC date |
| `event_name` | string (`string`) | no | What happened. | `add_to_cart` | `session_start`, `page_view`, `view_item`, `add_to_cart`, `begin_checkout`, `purchase` |
| `user_id` | string (`string`) | no* | Pseudonymous user ID (GA4: `user_pseudo_id`). | `usr_000042` | Synthetic: `usr_` + 6 digits |
| `session_id` | string (`string`) | no* | Session ID. A session is a unique `(user_id, session_id)` pair. | `ses_0000137` | Synthetic: `ses_` + 7 digits |
| `device_category` | string (`string`) | no | Device type. | `mobile` | `desktop`, `mobile`, `tablet` |
| `country` | string (`string`) | yes | Country name. | `United States` | Synthetic: United States, United Kingdom, Canada, Germany, France, Australia, India, Netherlands, Spain, Brazil |
| `traffic_source` | string (`string`) | yes | Where the visit came from. | `google` | Synthetic: `google`, `(direct)`, `bing`, `newsletter`, `facebook`, `partner-blog.example` |
| `traffic_medium` | string (`string`) | yes | How it came. | `cpc` | Synthetic: `organic`, `(none)`, `cpc`, `email`, `referral` |
| `traffic_campaign` | string (`string`) | yes | Campaign name, if any. Null when there is no campaign. | `brand_search` | Synthetic: `brand_search`, `generic_search`, `weekly_newsletter`, `autumn_sale`, or null |
| `transaction_id` | string (`string`) | yes | Order ID. Required on `purchase` events, null on all others. | `txn_000017` | Synthetic: `txn_` + 6 digits |
| `purchase_revenue` | decimal(12,2) (`decimal128(12, 2)`) | yes | Order revenue = sum of that event's `item_revenue`. Purchase events only. | `84.97` | ≥ 0; null on non-purchase events |
| `currency` | string (`string`) | yes | ISO currency code. Purchase events only. | `USD` | Synthetic: `USD`; null on non-purchase events |

Synthetic traffic channels (source / medium / campaign), as a set:
`google / organic`, `(direct) / (none)`, `google / cpc / brand_search`, `google / cpc / generic_search`,
`bing / organic`, `newsletter / email / weekly_newsletter`, `facebook / cpc / autumn_sale`,
`partner-blog.example / referral`.

## `raw_event_items`

Grain: one row per item inside an event. `(event_id, item_index)` is unique.
Items appear on `view_item`, `add_to_cart`, `begin_checkout` and `purchase` events only.

| Column | Contract type (Parquet type) | Nullable | Meaning | Example | Valid values |
|---|---|---|---|---|---|
| `event_id` | string (`string`) | no | The event this item belongs to. Links to `raw_events.event_id`. | `evt_000004211` | Must exist in `raw_events` |
| `item_index` | integer (`int32`) | no | Position of the item inside its event. | `0` | 0, 1, 2… |
| `item_id` | string (`string`) | no | Product ID. | `prd_0042` | Synthetic: `prd_` + 4 digits |
| `item_name` | string (`string`) | no | Product name. | `Classic Hoodie` | Any text |
| `item_category` | string (`string`) | no | Product category. | `Apparel` | Synthetic: Apparel, Footwear, Electronics, Home & Kitchen, Beauty, Sports & Outdoors, Toys & Games, Books |
| `price` | decimal(12,2) (`decimal128(12, 2)`) | no | Unit price in the event's currency. | `39.99` | ≥ 0 |
| `quantity` | integer (`int32`) | no | How many units. | `2` | > 0 (synthetic: 1–3; always 1 on `view_item`) |
| `item_revenue` | decimal(12,2) (`decimal128(12, 2)`) | no | `price × quantity`. | `79.98` | Equals `price × quantity` |

Which items each event type carries (synthetic):
- `view_item`: the one product viewed, quantity 1.
- `add_to_cart`: the one product added, with its quantity.
- `begin_checkout` and `purchase`: every product in the cart, with quantities.

## Deliberately invalid rows (synthetic data only)

The generator breaks a small, fixed number of rows **on purpose**, so later quality checks
have something real to find. Phase 1 keeps them in staging and only *warns* about them.
Phase 3 handles them properly. `manifest.json` → `invalid_rows` records the real count per
category, with the same descriptions as below.

| Category | File | Rows | What was done | Phase 1 dbt test that sees it (warn) |
|---|---|---|---|---|
| `missing_session_id` | `raw_events` | 20 | `page_view` events whose `session_id` was set to null | `not_null` on `stg_events.session_id` |
| `unknown_event_name` | `raw_events` | 30 | Extra events named `add_to_wishlist` (not in the contract), inside real sessions, with no items | `accepted_values` on `stg_events.event_name` |
| `duplicate_event_id` | `raw_events` | 25 | Exact copies of existing event rows (same `event_id`, same everything). 5 of them copy `purchase` events | `unique` on `stg_events.event_id` |
| `purchase_missing_transaction_id` | `raw_events` | 10 | `purchase` events whose `transaction_id` was set to null. Revenue and items are kept | `not_null` on `stg_events.transaction_id` (purchases only) |
| `negative_price` | `raw_event_items` | 15 | Items on `view_item` events whose `price` was made negative (`item_revenue` = price × quantity, so it is negative too) | none yet (Phase 3 adds range checks) |

Rules that make these rows "known":

- **Each planted row has exactly one problem.** A row picked for one category is never picked
  for another. This includes the events whose items got a negative price.
- **Duplicates copy only the `raw_events` row, never its items.** `raw_event_items` has no
  duplicate rows, so `(event_id, item_index)` stays unique. To find a duplicated event's items,
  join on `event_id` *after* removing the duplicate event rows, or revenue gets counted twice.
- **Negative prices are only on `view_item` items**, never on purchases, so order revenue
  (`purchase_revenue` = sum of the purchase's `item_revenue`) is never affected.
- The counts are fixed constants in `generate_synthetic.py` (`INVALID_ROWS`). With very small
  `--users`/`--sessions`, there may not be enough candidate rows. Then fewer are planted, and
  `manifest.json` records the real numbers.
