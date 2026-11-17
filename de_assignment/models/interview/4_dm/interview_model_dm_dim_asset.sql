{{ config(materialized='table', tags=['interview']) }}

with chlr as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    asset_type,
    manufacturer,
    model,
    commissioning_date,
    capacity_kw
  from {{ ref('interview_bc_dcasset_chlr') }}
),

crah as (
  select
    customer_short_code,
    dc_site_code,
    asset_id,
    asset_type,
    manufacturer,
    model,
    commissioning_date,
    capacity_kw
  from {{ ref('interview_bc_dcasset_crah') }}
)

select * from chlr
union all
select * from crah
