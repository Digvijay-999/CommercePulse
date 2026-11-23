-- Staging for raw_event_items. Grain: one row per item inside an event; (event_id, item_index) is unique.
-- Only casts and light cleanup. No business logic. Column names already match the contract.
-- Deliberately invalid rows (negative prices) are kept on purpose; Phase 3 handles them.
select
    cast(event_id as varchar)            as event_id,
    cast(item_index as integer)          as item_index,
    cast(item_id as varchar)             as item_id,
    trim(item_name)                      as item_name,
    trim(item_category)                  as item_category,
    cast(price as decimal(12, 2))        as price,
    cast(quantity as integer)            as quantity,
    cast(item_revenue as decimal(12, 2)) as item_revenue
from {{ source('raw', 'raw_event_items') }}
