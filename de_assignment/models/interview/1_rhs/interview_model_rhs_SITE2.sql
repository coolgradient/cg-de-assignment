{{ config(tags=['interview']) }}

-- TODO for DE assignment: Implement incremental MERGE with composite unique key at the event grain
-- TODO for DE assignment: Add configurable lookback window (e.g., 48h) for late-arriving data

with src as (
  select
    tenant_code,
    site_code,
    device_id,
    try_cast(ts as timestamp) as ts,
    datapoint,
    try_cast(value as float) as value
  from {{ ref('generate_lnd_interview_data_SITE2') }}
)
select * from src


