{#
  TODO (Task 2 - EHS_IN): Implement `ehs_standardize` macro.

  Contract:
  - Used by both `interview_model_ehs_in_SITE1` and `interview_model_ehs_in_SITE2` (you design the signature).
  - Output columns (exact names & types):
      customer_short_code  (VARCHAR)
      dc_site_code         (VARCHAR)
      asset_id             (VARCHAR)
      event_dts            (TIMESTAMP)
      datapoint            (VARCHAR)   -- standardized via {{ ref('interview_datapoint_map') }}
      metric_value         (FLOAT)
      is_imputed           (BOOLEAN)   -- false for observed rows, true for forward-filled rows
  - Gap-fill on the native 10-minute grid using forward-fill, capped at `max_fill_intervals`
    (default 6 = 1 hour). Multi-hour outages beyond the cap must NOT be filled.
#}
{% macro ehs_standardize(max_fill_intervals=6) %}
  -- TODO: implement
{% endmacro %}
