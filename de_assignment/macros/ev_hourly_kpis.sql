{#
  TODO (Task 3 - INT EV): Implement `ev_hourly_kpis(asset_type)` macro.

  Contract:
  - Used by both `interview_ev_dcasset_chlr_base` ('CHLR') and `interview_ev_dcasset_crah_base` ('CRAH').
  - Incremental model at hourly grain: `event_dth = date_trunc('hour', event_dts)`.
  - Sources from {{ ref('interview_model_ehs_out') }}, filtered to the given `asset_type`.
  - Incremental window must re-process the last partial hour(s).
  - Output columns (exact names):
      customer_short_code
      dc_site_code
      asset_id
      event_dth
      temperature_avg
      temperature_stddev
      humidity_avg
      humidity_stddev
      imputed_ratio        -- fraction of rows in the hour where is_imputed = true
#}
{% macro ev_hourly_kpis(asset_type) %}
  -- TODO: implement
{% endmacro %}
