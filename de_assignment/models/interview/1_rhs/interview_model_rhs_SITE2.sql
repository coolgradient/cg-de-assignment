{{ config(tags=['interview']) }}

{#
  TODO (Task 1 - RHS):
  - Mid/Senior (DE_ASSIGNMENT.md): Replace this stub with a call to rhs_historize(...) (no duplicated logic).
  - Junior (JUNIOR_DE_ASSIGNMENT.md): Write the incremental MERGE SQL directly in this file.
  Source: ref('generate_lnd_interview_data_SITE2')
  Source columns: tenant_code, site_code, device_id, ts, datapoint, value
  Output: same raw columns (historized incrementally via MERGE + lookback window).
  Keep tags=['interview'] in config(...).
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
