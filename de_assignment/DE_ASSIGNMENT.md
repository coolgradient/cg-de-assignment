# Coolgradient Data Engineering Assignment

**Target time:** ~75–90 minutes of engineering time (excluding initial Snowflake/dbt setup), plus an optional 15-minute review task.

We encourage you to use AI (e.g. Cursor) to move fast — we use it every day. During the follow-up technical interview, you will walk us through your implementation and explain the choices and trade-offs you made.

## Prerequisites & Setup (outside the 90-min clock)

1. Install [Cursor](https://cursor.com/) and [dbt](https://docs.getdbt.com/docs/core/installation-overview) (`dbt-snowflake`).
2. Create a [Snowflake trial account](https://signup.snowflake.com/) and configure `profiles.yml` (see `de_assignment/profiles.yml` for the template; note that new Snowflake accounts may require key-pair auth or a Programmatic Access Token instead of password-only auth).
3. From the `de_assignment/` directory, install packages, load seeds, and generate the raw landing tables (~1 month of 10-minute telemetry per site):
   ```bash
   cd de_assignment
   dbt deps
   dbt seed
   dbt run --select generate_lnd_interview_data_SITE1 generate_lnd_interview_data_SITE2
   ```

## DWH Layers Overview

Our architecture extends the medallion pattern into 6 layers:

- **0_lnd (Landing):** Raw ingestion, source fidelity preserved. *(Provided)*
- **1_rhs (Raw History):** Incremental historization (`MERGE` + lookback window). **You build this (Task 1).**
- **2_1_ehs_in (Standardization):** Schema mapping, datapoint harmonization, and bounded gap-fill. **You build this (Task 2).**
- **2_2_ehs_out (Canonical Stream):** Union of standardized feeds. *(Prebuilt)*
- **3_int (Integration):** Business Concepts (`BC`, *prebuilt*) and hourly Events (`EV`, **Task 3**).
- **4_dm (Data Marts):** Daily facts and dimensions. *(Prebuilt — do not build)*
- **5_rm (Reporting):** Flattened presentation + JSON payload. *(Prebuilt — do not build)*

> **Scope note:** Do **not** build `EHS_OUT`, `BC`, `DM`, or `RM` — they are already provided so the full pipeline runs end-to-end once your 3 tasks are complete.

---

## Your Tasks

Each of the 3 build tasks must be implemented as a **reusable dbt macro** in `macros/` so that duplicated site/asset models share a single implementation.

### Task 1 — RHS: `macros/rhs_historize.sql` (~20 min)

Implement `rhs_historize` and call it from both `1_rhs/interview_model_rhs_SITE1.sql` and `1_rhs/interview_model_rhs_SITE2.sql` (zero duplicated logic).

- Preserves raw source column names and fidelity (minimal casting only).
- `materialized='incremental'` with `merge` and a composite unique key at the event grain.
- Configurable lookback window in hours (parameter or dbt `var`) under `is_incremental()` to capture late-arriving rows idempotently.
- **Required proof** (paste the results in the header comment of `interview_model_rhs_SITE1.sql` — no written essay):
  1. Run RHS twice in a row and paste the row counts after run 1 vs run 2 (confirming idempotency).
  2. Bump `end` in `0_lnd/generate_lnd_interview_data_SITE1.yml` forward by 2 days, rebuild `generate_lnd_interview_data_SITE1`, and run RHS incrementally — paste the Snowflake `MERGE` `rows_inserted` / `rows_updated` counts (or incremental source row count) plus final RHS row count. *(Note: because the synthetic generator re-randomizes gaps on rebuild, RHS may slightly exceed the new landing count due to backfilled gaps inside the lookback window.)*

### Task 2 — EHS_IN: `macros/ehs_standardize.sql` (~35 min)

Implement `ehs_standardize(max_fill_intervals=6)` and call it from both `2_ehs/interview_model_ehs_in_SITE1.sql` and `2_ehs/interview_model_ehs_in_SITE2.sql`.

- **Output contract (exact columns & types):**
  `customer_short_code` (`VARCHAR`), `dc_site_code` (`VARCHAR`), `asset_id` (`VARCHAR`), `event_dts` (`TIMESTAMP`), `datapoint` (`VARCHAR`), `metric_value` (`FLOAT`), `is_imputed` (`BOOLEAN`)
- Translate `datapoint` labels via `{{ ref('interview_datapoint_map') }}` (`CG_TEMPERATURE`, `CG_HUMIDITY`).
- Forward-fill missing readings on the native 10-minute grid, capped at `max_fill_intervals` (default `6`, i.e. 1 hour). Multi-hour sensor outages beyond the cap must **not** be filled — omit those unfilled timestamps so `metric_value` stays non-null.
- Set `is_imputed = true` for forward-filled rows and `false` for observed rows.
- Add `not_null` and grain uniqueness (`dbt_utils.unique_combination_of_columns`) tests in `2_ehs/interview_model_ehs_in.yml`.

### Task 3 — INT EV: `macros/ev_hourly_kpis.sql` (~20 min)

Implement `ev_hourly_kpis(asset_type)` and call it from `3_int/interview_ev_dcasset_chlr_base.sql` (`'CHLR'`) and `3_int/interview_ev_dcasset_crah_base.sql` (`'CRAH'`).

- `materialized='incremental'` at hourly grain (`event_dth = date_trunc('hour', event_dts)`), sourcing from `{{ ref('interview_model_ehs_out') }}`.
- Incremental filter must re-process the last partial hour(s) so late/updated 10-minute readings are reflected accurately.
- **Output contract (exact columns):**
  `customer_short_code`, `dc_site_code`, `asset_id`, `event_dth`, `temperature_avg`, `temperature_stddev`, `humidity_avg`, `humidity_stddev`, `imputed_ratio`
- Ensure all models and tests pass with `dbt build --select tag:interview`.

### Optional Task — Architecture & Code Review (max 15 min)

The prebuilt `4_dm/` and `5_rm/` models contain deliberate design issues. If you have time left, create a `REVIEW.md` at the repo root with **3–5 short bullets** naming what you would change and why.

---

## Submission

- Zip your repository (including models, macros, tests, and optional `REVIEW.md`) and email it to `rogier.werschkull@coolgradient.com`.

## Be Ready to Discuss (Follow-up Interview)

1. **RHS:** Your composite key choice, lookback window sizing, and idempotency guarantees.
2. **EHS_IN:** How your bounded forward-fill works (including across incremental batch boundaries) and view vs. incremental trade-offs.
3. **INT EV:** Re-aggregating partial hours incrementally and why we separate Business Concepts (`BC`) from Events (`EV`).
4. **Architecture:** Where logic belongs across layers (e.g. `EHS_OUT` materialization, handling changing metadata in `BC`).
5. **Review (if completed):** The issues you spotted in `DM`/`RM` and how you'd fix them.
