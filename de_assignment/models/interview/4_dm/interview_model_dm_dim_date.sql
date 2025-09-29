{{ config(tags=['interview']) }}

-- TODO: Create a date dimension from the min/max event_dts in EHS_OUT.
-- Guidance:
-- 1) Determine date range from {{ ref('interview_model_ehs_out') }}.
-- 2) Generate rows per date with fields using a data range generator function : event_dt, year, month, day, week
-- 3) Add tests in interview_dm.yml: not_null + unique on event_dt.
-- 4) What do you choos as a materialisation and why?
select 1
