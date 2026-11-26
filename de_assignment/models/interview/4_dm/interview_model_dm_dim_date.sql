{{ config(materialized='table', tags=['interview']) }}

with bounds as (
  select
    min(event_dts::date) as min_dt,
    max(event_dts::date) as max_dt
  from {{ ref('interview_model_ehs_out') }}
),

seq as (
  select
    row_number() over (order by seq4()) - 1 as day_offset
  from table(generator(rowcount => 3650))
),

spine as (
  select
    dateadd(day, seq.day_offset, bounds.min_dt) as event_dt
  from bounds
  cross join seq
  where dateadd(day, seq.day_offset, bounds.min_dt) <= bounds.max_dt
)

select
  event_dt,
  year(event_dt) as year,
  month(event_dt) as month,
  day(event_dt) as day,
  weekofyear(event_dt) as week
from spine
