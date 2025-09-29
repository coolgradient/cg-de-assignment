{{ config(tags=['interview']) }}

-- TODO: Build fact_measurement by unpivoting EV CHLR and CRAH models back to long format and unioning.
-- Output columns:
--   customer_short_code, dc_site_code, asset_id, event_date, datapoint, metric_value
-- Guidance:
-- 1) Start from {{ ref('interview_ev_dcasset_chlr_base') }} and {{ ref('interview_ev_dcasset_crah_base') }}.
-- 2) UNPIVOT the KPIs from the EV models
-- 3) UNION ALL both EV sources
-- 4) Add tests in interview_dm.yml: not_null on all columns, unique grain on keys + datapoint and foreign key tests from the facst to the dims
-- 5) What do you choos as a materialisation and why?
select * from {{ ref('interview_ev_dcasset_chlr_base') }} limit 0
