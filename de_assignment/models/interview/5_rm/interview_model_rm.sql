{{ config(materialized='view', tags=['interview']) }}

with fact as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_date,
    datapoint,
    metric_avg,
    metric_stddev
  from {{ ref('interview_model_fact_measurement') }}
),

dim_asset as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    asset_type,
    manufacturer,
    model,
    commissioning_date,
    capacity_kw
  from {{ ref('interview_model_dm_dim_asset') }}
),

dim_date as (
  select
    event_dt,
    year,
    month,
    day,
    week
  from {{ ref('interview_model_dm_dim_date') }}
)

select
  f.customer_short_code,
  f.dc_site_code,
  f.asset_id,
  d.event_dt,
  f.datapoint,
  f.metric_avg,
  f.metric_stddev,
  case
    when f.datapoint = 'CG_TEMPERATURE' and f.metric_avg > 23.0 then true
    else false
  end as is_overheating,
  object_construct(
    'asset_type', a.asset_type,
    'manufacturer', a.manufacturer,
    'model', a.model,
    'capacity_kw', a.capacity_kw,
    'datapoint', f.datapoint,
    'metric_avg', f.metric_avg,
    'metric_stddev', f.metric_stddev,
    'year', d.year,
    'month', d.month,
    'day', d.day
  ) as json_payload
from fact f
inner join dim_asset a
  on f.customer_short_code = a.customer_short_code
  and f.dc_site_code = a.dc_site_code
  and f.asset_id = a.asset_id
inner join dim_date d
  on f.event_date = d.event_dt
