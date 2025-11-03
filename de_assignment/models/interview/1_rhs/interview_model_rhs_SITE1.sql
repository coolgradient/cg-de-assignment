{{ config(tags=['interview']) }}

-- See Section B in models/interview/interview_questions.md for the hands-on requirements

-- TODO for DE assignment: Implement incremental MERGE with composite unique key at the event grain
-- TODO for DE assignment: Add configurable lookback window (e.g., 48h) for late-arriving data

with src as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    try_cast(event_dts as timestamp) as event_dts,
    datapoint,
    try_cast(metric_value as float) as metric_value
  from {{ ref('generate_lnd_interview_data_SITE1') }}
)
select
 *
from src
