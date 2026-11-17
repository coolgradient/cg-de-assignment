{{ config(materialized='table', tags=['interview']) }}

with ev_union as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_dth,
    temperature_avg,
    temperature_stddev,
    humidity_avg,
    humidity_stddev
  from {{ ref('interview_ev_dcasset_chlr_base') }}

  union all

  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_dth,
    temperature_avg,
    temperature_stddev,
    humidity_avg,
    humidity_stddev
  from {{ ref('interview_ev_dcasset_crah_base') }}
),

daily as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_dth::date as event_date,
    avg(temperature_avg) as temperature_avg,
    avg(temperature_stddev) as temperature_stddev,
    avg(humidity_avg) as humidity_avg,
    avg(humidity_stddev) as humidity_stddev
  from ev_union
  group by 1, 2, 3, 4
),

unpivoted as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_date,
    'CG_TEMPERATURE' as datapoint,
    temperature_avg as metric_avg,
    temperature_stddev as metric_stddev
  from daily

  union all

  select
    customer_short_code,
    dc_site_code,
    asset_id,
    event_date,
    'CG_HUMIDITY' as datapoint,
    humidity_avg as metric_avg,
    humidity_stddev as metric_stddev
  from daily
)

select
  customer_short_code,
  dc_site_code,
  asset_id,
  event_date,
  datapoint,
  metric_avg,
  metric_stddev
from unpivoted
