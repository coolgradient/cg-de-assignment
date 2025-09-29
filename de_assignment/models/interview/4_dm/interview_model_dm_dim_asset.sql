{{ config(tags=['interview']) }}

-- TODO: Create an asset dimension by unioning CHLR and CRAH BC models.
-- Guidance:
-- 1) Union {{ ref('interview_bc_dcasset_chlr') }} and {{ ref('interview_bc_dcasset_crah') }}.
-- 2) Keys: (customer_short_code, dc_site_code, asset_id).
-- 3) Include asset attributes: asset_type, manufacturer, model, commissioning_date, capacity_kw.
-- 4) Add tests in interview_dm.yml: not_null + unique on the keys above.
-- 5) What do you choos as a materialisation and why?
select * from {{ ref('interview_bc_dcasset_chlr') }} limit 0
