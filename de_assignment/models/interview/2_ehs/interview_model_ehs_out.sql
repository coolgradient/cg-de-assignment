{{ config(materialized='view', tags=['interview']) }}

with site1 as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_dts,
    datapoint,
    metric_value,
    is_imputed
  from {{ ref('interview_model_ehs_in_SITE1') }}
),

site2 as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_dts,
    datapoint,
    metric_value,
    is_imputed
  from {{ ref('interview_model_ehs_in_SITE2') }}
)

select * from site1
union all
select * from site2
