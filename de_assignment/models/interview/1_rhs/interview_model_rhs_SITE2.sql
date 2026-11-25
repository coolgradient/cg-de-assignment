{{ config(tags=['interview']) }}

{#
  TODO (Task 1 - RHS): Replace this stub with a call to rhs_historize(...).
  Must share the exact same macro as interview_model_rhs_SITE1 (no duplicated logic).
  Source: ref('generate_lnd_interview_data_SITE2')
  Source columns: tenant_code, site_code, device_id, ts, datapoint, value
  Output: same raw columns (historized incrementally via MERGE + lookback window).
#}

select
  tenant_code,
  site_code,
  device_id,
  try_cast(ts as timestamp) as ts,
  datapoint,
  try_cast(value as float) as value
from {{ ref('generate_lnd_interview_data_SITE2') }}
limit 0
