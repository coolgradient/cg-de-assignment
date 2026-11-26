# Coolgradient Junior Data Engineering Assignment

**Target time:** ~60–90 minutes of engineering time (excluding initial Snowflake/dbt setup).

We encourage you to use AI (e.g. Cursor) to move fast — we use it every day. During the follow-up technical interview, you will walk us through your SQL and dbt models and explain what each part does and why.

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
- **2_1_ehs_in (Standardization):** Schema mapping and datapoint harmonization. **You build this (Task 2).**
- **2_2_ehs_out (Canonical Stream):** Union of standardized feeds. *(Prebuilt)*
- **3_int (Integration):** Business Concepts (`BC`, *prebuilt*) and hourly Events (`EV`, **Task 3**).
- **4_dm (Data Marts):** Daily facts and dimensions. *(Prebuilt — do not build)*
- **5_rm (Reporting):** Flattened presentation + JSON payload. *(Prebuilt — do not build)*

> **Scope note:**
> - Do **not** build `EHS_OUT`, `BC`, `DM`, or `RM` — they are already provided so the full pipeline runs end-to-end once your 3 tasks are complete.
> - You do **not** need to implement the macro stubs in `macros/` — writing clean, readable SQL directly in each model file is completely fine (reusing a macro is optional). If you use Cursor, point it to `JUNIOR_DE_ASSIGNMENT.md` so it doesn't build the Mid/Senior macro or gap-fill tasks.

---

## Your Tasks

### Task 1 — RHS: Incremental Historization (~20 min)

Update `1_rhs/interview_model_rhs_SITE1.sql` and `1_rhs/interview_model_rhs_SITE2.sql`.

- Keep raw source column names intact (only safe type casting with `try_cast` — note that `0_lnd` stores timestamps and values as strings).
  - `SITE1`: `customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value`
  - `SITE2`: `tenant_code, site_code, device_id, ts, datapoint, value`
- Configure `materialized='incremental'`, `incremental_strategy='merge'`, and a composite `unique_key` at the event grain. **Keep `tags=['interview']` in your `config(...)` block** so `dbt build --select tag:interview` includes these models.
- Add an `{% if is_incremental() %}` filter with a 48-hour lookback window (e.g. `try_cast(event_dts as timestamp) >= (select dateadd(hour, -48, max(event_dts)) from {{ this }})`) so late-arriving rows are picked up.
- **Quick check:** Run RHS twice in a row and paste the `SITE1` row counts after run 1 and run 2 in a comment at the top of `interview_model_rhs_SITE1.sql` to confirm it is idempotent (row count stays the same).

### Task 2 — EHS_IN: Schema & Datapoint Standardization (~25 min)

Update `2_ehs/interview_model_ehs_in_SITE1.sql` and `2_ehs/interview_model_ehs_in_SITE2.sql` so both feeds share one canonical schema.

- **Output contract (exact columns & types):**
  `customer_short_code` (`VARCHAR`), `dc_site_code` (`VARCHAR`), `asset_id` (`VARCHAR`), `event_dts` (`TIMESTAMP`), `datapoint` (`VARCHAR`), `metric_value` (`FLOAT`), `is_imputed` (`BOOLEAN`)
- Map `SITE2`'s raw column names (`tenant_code`, `site_code`, `device_id`, `ts`, `value`) to the standard column names above.
- Translate `datapoint` labels via `{{ ref('interview_datapoint_map') }}` so both sites output `CG_TEMPERATURE` and `CG_HUMIDITY`.
- Set `false as is_imputed` (you do **not** need to implement gap-filling for this assignment — we will discuss how you'd handle missing sensor data in the interview).
- Add `not_null` and grain uniqueness (`dbt_utils.unique_combination_of_columns`) tests in `2_ehs/interview_model_ehs_in.yml` *(see `3_int/interview_int.yml` for the exact syntax)*.

### Task 3 — INT EV: Hourly Asset KPIs (~25 min)

Update `3_int/interview_ev_dcasset_chlr_base.sql` (for `'CHLR'` assets) and `3_int/interview_ev_dcasset_crah_base.sql` (for `'CRAH'` assets).

- Source from `{{ ref('interview_model_ehs_out') }}` and join `{{ ref('interview_asset_metadata') }}` to filter to the right `asset_type`.
- Materialize as a `table` (incremental is not required here).
- Aggregate 10-minute readings to hourly grain (`date_trunc('hour', event_dts) as event_dth`), pivoting `CG_TEMPERATURE` and `CG_HUMIDITY` with `avg` and `stddev`.
- **Output contract (exact columns):**
  `customer_short_code`, `dc_site_code`, `asset_id`, `event_dth`, `temperature_avg`, `temperature_stddev`, `humidity_avg`, `humidity_stddev`, `imputed_ratio` *(you can compute `avg(case when is_imputed then 1.0 else 0.0 end)`)*
- Run `dbt build --select tag:interview` and verify that all models (including the prebuilt `EHS_OUT`, `BC`, `DM`, and `RM` layers) and all tests pass.

### Optional Bonus (max 15 min)

If you have time left, pick **one**:
- **DRY it up:** Factor repeated SQL between `SITE1`/`SITE2` or `CHLR`/`CRAH` into a simple dbt macro in `macros/`.
- **Spot the issues:** Read the prebuilt models in `4_dm/` and `5_rm/` and write 2–3 short bullets in a `REVIEW.md` at the repo root naming anything you'd question or change.

---

## Submission

- Zip your repository (including models, tests, and any optional macros or `REVIEW.md`) and email it to `rogier.werschkull@coolgradient.com`.

## Be Ready to Discuss (Follow-up Interview)

1. **dbt & SQL fundamentals:** How `ref()`, `table` vs `view` vs `incremental`, and dbt tests work in your solution.
2. **RHS:** Why we use a composite `unique_key`, what idempotency means, and why a lookback window matters.
3. **EHS_IN:** Why map datapoints via a seed (`interview_datapoint_map`) instead of hardcoded `CASE` statements, and how you would detect or fill missing sensor readings.
4. **Architecture:** Why split transformations into layers (`RHS` → `EHS` → `INT` → `DM` → `RM`) and why separate Business Concepts (`BC`) from Events (`EV`).
5. **Your code:** Walk through the SQL you wrote (including any AI-assisted parts) and explain what it does.
