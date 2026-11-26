{{ config(tags=['interview']) }}

{#
  TODO (Task 2 - EHS_IN):
  - Mid/Senior (DE_ASSIGNMENT.md): Replace this stub with a call to ehs_standardize(...) (no duplicated logic).
  - Junior (JUNIOR_DE_ASSIGNMENT.md): Write the standardization SQL directly here (false as is_imputed; no gap-fill).
  Source: ref('interview_model_rhs_SITE2')
  Output contract (exact columns):
    customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value, is_imputed
  Keep tags=['interview'] in config(...).
#}

select
  cast(null as varchar) as customer_short_code,
  cast(null as varchar) as dc_site_code,
  cast(null as varchar) as asset_id,
  cast(null as timestamp) as event_dts,
  cast(null as varchar) as datapoint,
  cast(null as float) as metric_value,
  cast(null as boolean) as is_imputed
limit 0
