{{ config(tags=['interview']) }}

{#
  TODO (Task 1 - RHS):
  - Mid/Senior (DE_ASSIGNMENT.md): Replace this stub with a call to rhs_historize(...).
  - Junior (JUNIOR_DE_ASSIGNMENT.md): Write the incremental MERGE SQL directly in this file.
  Source: ref('generate_lnd_interview_data_SITE1')
  Source columns: customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value
  Output: same raw columns (historized incrementally via MERGE + lookback window).
  Keep tags=['interview'] in config(...).

  Proof (paste results here):
  1) Idempotency check (row count after run 1 vs run 2):
  2) [Mid/Senior only] 2-day increment check (MERGE rows_inserted / rows_updated + final row count):
#}

select
  customer_short_code,
  dc_site_code,
  asset_id,
  try_cast(event_dts as timestamp) as event_dts,
  datapoint,
  try_cast(metric_value as float) as metric_value
from {{ ref('generate_lnd_interview_data_SITE1') }}
limit 0
