{{ config(tags=['interview']) }}

-- TODO: Build CHLR EV (event) model as an incremental table with hourly pivoted KPIs from EHS_OUT.
-- Guidance:
-- 1) Source minute-grain `event_dts` and derive `event_dth = date_trunc('hour', event_dts)`.
-- 2) Compute hourly aggregates: AVG and STDDEV per datapoint (temperature/humidity) via conditional aggregation.
-- 3) Keys: (customer_short_code, dc_site_code, asset_id, event_dth).
-- 4) Add tests in interview_int.yml: not_null + unique_combination on the keys above.
select * from {{ ref('interview_model_ehs_out') }} limit 0


