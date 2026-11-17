{{ config(materialized='table', tags=['interview']) }}

select
  customer_short_code,
  dc_site_code,
  asset_id,
  asset_type,
  manufacturer,
  model,
  try_cast(commissioning_date as date) as commissioning_date,
  try_cast(capacity_kw as float) as capacity_kw
from {{ ref('interview_asset_metadata') }}
where asset_type = 'CHLR'
