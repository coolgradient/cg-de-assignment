{{ config(tags=['interview']) }}

-- TODO: Map RHS SITE 1 to the standard EHS_IN schema.
-- Output columns (explicit only):
--   customer_short_code, dc_site_code, asset_id, event_dts (TIMESTAMP), datapoint, metric_value (FLOAT)
-- Guidance:
-- 1) Select only the required columns from {{ ref('interview_model_rhs_SITE1') }} (no SELECT *).
-- 2) Impute / calculate the missing minute level data points from rhs (there are holes in the data) and rationalize your missing value calucation approach
-- 3) Standardize datapoint names by joining the seed {{ ref('interview_datapoint_map') }} on
--    (customer_short_code, dc_site_code, source_datapoint) and output the "standard_datapoint" as "datapoint".
-- 4) Ensure safe typing with TRY_CAST for timestamps and numeric values where appropriate.
-- 5) Add/verify schema tests in interview_model_ehs_in.yml (not_null + unique grain).
select
  *
from {{ ref('interview_model_rhs_SITE1') }}


