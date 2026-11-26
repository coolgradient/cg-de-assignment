{#
  TODO (Task 1 - RHS): Implement `rhs_historize` macro.

  Contract:
  - Used by both `interview_model_rhs_SITE1` and `interview_model_rhs_SITE2` (you design the signature).
  - Preserves raw source schema/fidelity (minimal casting only, no business renaming).
  - Supports incremental MERGE with a composite unique key at the event grain.
  - Applies a configurable lookback window in hours (parameter or dbt var) when `is_incremental()` is true
    to capture late-arriving rows idempotently.
#}
{% macro rhs_historize() %}
  -- TODO: implement
{% endmacro %}
