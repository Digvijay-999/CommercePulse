-- Staging for raw_events. Grain: one row per raw event row.
-- Only casts and light cleanup (trim, lower-case, empty text -> null). No business logic.
-- Column names already match the contract, so nothing is renamed.
-- Deliberately invalid rows (duplicates, unknown event names, missing ids) are kept on purpose:
-- the tests in _staging.yml warn about them, and Phase 3 handles them.
select
    cast(event_id as varchar)                as event_id,
    cast(source as varchar)                  as source,
    cast(event_timestamp as timestamptz)     as event_timestamp,
    cast(event_date as date)                 as event_date,
    lower(trim(event_name))                  as event_name,
    cast(user_id as varchar)                 as user_id,
    cast(session_id as varchar)              as session_id,
    lower(trim(device_category))             as device_category,
    nullif(trim(country), '')                as country,
    nullif(trim(traffic_source), '')         as traffic_source,
    nullif(trim(traffic_medium), '')         as traffic_medium,
    nullif(trim(traffic_campaign), '')       as traffic_campaign,
    cast(transaction_id as varchar)          as transaction_id,
    cast(purchase_revenue as decimal(12, 2)) as purchase_revenue,
    upper(trim(currency))                    as currency
from {{ source('raw', 'raw_events') }}
