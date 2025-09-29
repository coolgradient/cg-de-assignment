{{ config(tags=['interview']) }}

-- TODO: Flatten DM facts and dims and add a new column with a JSON payload.
-- Input: fact {{ ref('interview_model_fact_measurement') }}, dims {{ ref('interview_model_dm_dim_asset') }}, {{ ref('interview_model_dm_dim_date') }}.
-- Output columns (explicit only):
--   customer_short_code, dc_site_code, asset_id, event_dt, datapoint, metric_value, json_payload
-- Guidance:
-- 1) Join fact -> dim_asset (on keys) and fact -> dim_date on the daily event_dt grain.
-- 2) Build json_payload with OBJECT_CONSTRUCT including measurement and asset fields. Why would we do this?
-- 3) Keep presentation-only logic here (no new business logic).
-- 4) Add tests in interview_model_rm.yml for not_null on keys and json_payload.
-- 5) What do you choose as a materialisation and why?
-- 6) Would it make sense to add dbt contract to this model and why?
select 1
