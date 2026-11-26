{{ config(tags=['interview']) }}

{#
  TODO (Task 3 - INT EV):
  - Mid/Senior (DE_ASSIGNMENT.md): Replace this stub with ev_hourly_kpis('CHLR') (incremental).
  - Junior (JUNIOR_DE_ASSIGNMENT.md): Write the hourly CHLR aggregation SQL directly here (table).
  Output contract (exact columns):
    customer_short_code, dc_site_code, asset_id, event_dth,
    temperature_avg, temperature_stddev, humidity_avg, humidity_stddev, imputed_ratio
  Keep tags=['interview'] in config(...).
#}

select
  cast(null as varchar) as customer_short_code,
  cast(null as varchar) as dc_site_code,
  cast(null as varchar) as asset_id,
  cast(null as timestamp) as event_dth,
  cast(null as float) as temperature_avg,
  cast(null as float) as temperature_stddev,
  cast(null as float) as humidity_avg,
  cast(null as float) as humidity_stddev,
  cast(null as float) as imputed_ratio
limit 0
