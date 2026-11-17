{{ config(materialized='table', tags=['interview']) }}

with bounds as (
  select
    min(event_dts::date) as min_dt,
    max(event_dts::date) as max_dt
  from {{ ref('interview_model_ehs_out') }}
),

spine as (
  select
    dateadd(day, seq4(), bounds.min_dt) as event_dt
  from bounds,
    table(generator(rowcount => 3650))
  qualify event_dt <= bounds.max_dt
)

select
  event_dt,
  year(event_dt) as year,
  month(event_dt) as month,
  day(event_dt) as day,
  weekofyear(event_dt) as week
from spine
