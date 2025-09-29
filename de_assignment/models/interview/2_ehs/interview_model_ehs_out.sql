{{ config(tags=['interview']) }}

-- TODO: Union  glue together the two EHS_IN models into a canonical EHS_OUT stream.
-- Output columns (explicit only):
--   customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value
-- Guidance:
-- 1) Select the explicit columns from {{ ref('interview_model_ehs_in_SITE1') }} and {{ ref('interview_model_ehs_in_SITE2') }} and UNION ALL.
-- 2) Enforce uniqueness at the canonical grain (customer_short_code, dc_site_code, asset_id, event_dts, datapoint).
-- 3) Keep the final explicit select to the 6 standard columns only.
-- 4) Add/verify schema tests in interview_model_ehs_out.yml (not_null + unique grain).

-- placeholder: replace with your explicit union per the guidance above
select * from {{ ref('interview_model_ehs_in_SITE1') }} limit 0


