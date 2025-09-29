{{ config(tags=['interview']) }}

-- TODO: Create a Business Concept (BC) model for CHLR assets.
-- Guidance:
-- 1) Use {{ ref('interview_model_ehs_out') }} as the measurement source and the seed interview_asset_metadata for attributes.
-- 2) Output one row per asset (distinct keys) with stable attributes (manufacturer, model, commissioning_date, capacity_kw).
-- 3) Add tests in interview_int.yml to validate unique key: (customer_short_code, dc_site_code, asset_id).
-- 4) How would we deeel with interview_asset_metadata changing over time / attributes being overwritten?
select * from {{ ref('interview_model_ehs_out') }} limit 0


