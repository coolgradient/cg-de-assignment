{{ config(tags=['interview']) }}

-- TODO (Task 1 - RHS): Replace this stub with a call to {{ rhs_historize(...) }}.
-- Source: {{ ref('generate_lnd_interview_data_SITE1') }}
-- Source columns: customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value
-- Output: same raw columns (historized incrementally via MERGE + lookback window).

-- Proof (paste row-count check results here after running twice + 2-day increment):
-- 1) Idempotency check:
-- 2) 2-day increment check:

select
  customer_short_code,
  dc_site_code,
  asset_id,
  try_cast(event_dts as timestamp) as event_dts,
  datapoint,
  try_cast(metric_value as float) as metric_value
from {{ ref('generate_lnd_interview_data_SITE1') }}
limit 0
