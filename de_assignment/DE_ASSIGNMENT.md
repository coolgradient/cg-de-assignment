# Coolgradient Data Engineering Assignment

## Prerequisites

- dbt installed on your local machine or use dbt cloud, we prefer you to install and use https://cursor.com/
  - for installing dbt see: https://docs.getdbt.com/docs/core/installation-overview
  - when working with dbt we suggest you to use the dbt power user extension: https://open-vsx.org/extension/innoverio/vscode-dbt-power-user

- sign up for snowflake trial account and write down your login credentials: https://signup.snowflake.com/

- dbt configured, fill in your snowflake connection credentials profiles.yml and place it at the right location

## Repo / DWH Layers Overview (What & Why)

Note that our architecture is an EXTENSION, more layered approach of the so called 'medallion architecture'

### The DWH Layers we use are:

- **0_lnd** — Landing: Ingest-only raw data; preserve source fidelity, minimal coercion. Why: auditability and decoupled ingest.
- **1_rhs** — Raw History: Historize raw with incremental MERGE and lookback; clustering allowed. Why: durable, reproducible history.
- **2_ehs** — Enterprise History Standardization:
  - 2_1_ehs_in: Map per-customer/site schemas to CG standard columns.
  - 2_2_ehs_out: Union standardized inputs into one canonical enterprise stream.
  - Why: harmonize semantics across tenants and provide a single, tested source of truth.
- **3_int** — Integration and reducing data amount: Business Concepts (BC) and Events on the hourly level (EV); pivot/wide KPIs for reuse. Why: clean business entities, reusable event structures.
- **4_dm** — Data Marts: Dimensional models and daily aggregations for analytics; presentation-friendly schemas. Why: consumption-oriented performance and clarity.
- **5_rm** — Reporting: Final presentation/API flattening; no new business logic; JSON payloads for downstream use. Why: decouple delivery from modeling.

### Repo Folder structure (by DWH layer)
- 0_lnd/: synthetic data generators and configs
- 1_rhs/: RHS models per source/site
- 2_ehs/: EHS_IN (x2) standardization and EHS_OUT union scaffold
- 3_int/: BC and EV scaffolds
- 4_dm/: dim_date, dim_asset, fact_measurement scaffolds (and daily DM example)
- 5_rm/: RM flattening scaffold with JSON payload

## Assignment Goal

Build a layered data warehouse pipeline (RHS → EHS_IN (x2) → EHS_OUT → INT → DM → RM) following Coolgradient's architectural patterns. You will implement incremental processing, schema standardization, and dimensional modeling across multiple warehouse layers.

- At least develop up to and including the INT layer
- You are free to use AI to complete the assignment - we even EXPECT you to! (we use it every day). But you will obviously fail if you cannot explain WHAT you have done and WHY in our review

## What You Will Deliver

1. **0_lnd — Generate landing data (run first)**
   - Run the Snowpark Python generators for both data centers:
     - `dbt run --select models/interview/assignment_models/0_lnd/generate_lnd_interview_data_SITE1`
     - `dbt run --select models/interview/assignment_models/0_lnd/generate_lnd_interview_data_SITE2`
   - These create raw landing tables per site based on the YAML parameters (assets × datapoints × time).

2. **RHS — Raw historization only**
   - Update `1_rhs/interview_model_rhs_SITE1.sql` and `1_rhs/interview_model_rhs_SITE2.sql`.
   - Focus: load raw and historize incrementally correctly. Nothing else.
   - Implement `materialized: incremental` with `merge` and a composite unique key
   - Add a lookback window (e.g., 48h) via `is_incremental()` to capture late-arriving rows.
   - Justify key choice, idempotency, and lookback trade-offs

3. **EHS_IN (two sources)**
   - Update `2_ehs/interview_model_ehs_in_SITE1.sql` and `2_ehs/interview_model_ehs_in_SITE2.sql` mapping both feeds into the standard schema:
     `customer_short_code, dc_site_code, asset_id, event_dts, datapoint, metric_value`.
   - Standardize datapoint names (`temp_c→temperature`, `rel_humidity→humidity`) using the interview_datapoint_map seed
   - Impute missing hourly datapoints where appropriate (e.g., forward-fill); document your choice
   - Add schema tests for grain uniqueness and required fields.
   - Build this model as incremental table or view? Rationalize!

4. **EHS_OUT**
   - Union the two EHS_IN models into a single canonical stream.
   - Ensure uniqueness on the standard grain, minimize scans, explicit columns only.
   - Build this model as incremental table or view? Rationalize!

5. **INT — BC and EV**
   - Adjust the existing BC and one EV models for the two asset types following repo patterns (clean keys, readable transforms).
   - EVs should aggregate minute-level measurements to hourly grain using AVG and also include STDDEV variants per datapoint (e.g., temperature and humidity), producing a wide format from `EHS_OUT`.
   - Explain why this separation improves reuse further downstream and testability.

6. **DM — Daily aggregation and create facts / dimensions**
   - Aggregate to day level grain from INT with explicit columns.

7. **RM — Presentation**
   - Flatten DM and construct a JSON payload with daily datapoint metrics.

8. **Data quality validation**
   - Add schema tests at each layer for grain and required columns.

## Evaluation Criteria

- **Correctness**: Models produce accurate results and handle edge cases
- **Incremental Logic**: Proper merge keys, idempotency, and lookback windows
- **Layer Separation**: Appropriate logic placement per warehouse layer
- **Materialization Choices**: Justified decisions on table vs view, incremental vs full refresh
- **Code Quality**: Clean SQL, reusable macros, clear naming conventions
- **Testing**: Schema tests for grain uniqueness and required fields
- **Documentation**: Ability to explain your decisions and trade-offs

## Constraints

- Do not alter the semantics of the provided seed data
- Keep logic layer-appropriate and avoid embedding presentation-only concerns
- Follow the repo's naming conventions and folder structure

## Submission

- Zip your repository (including all models, tests, and macros) and email it to rogier.werschkull@coolgradient.com
- Be prepared to walk through your approach during the follow-up interview

## Potential Discussion Topics (be ready to discuss)

### General Topics
- Your unique key choices and alternatives you considered
- Usage of macros or not for repeating logic
- Materialization choices (table vs view, incremental vs full refresh)
- Missing data imputation strategy
- Your lookback window strategy when materializing incrementally and how you would tune it for larger datasets
- Where you placed logic by layer and why
- Code cleanliness and naming conventions

### Layer-Specific Questions

**0_lnd — Landing Layer**
- What's allowed and not allowed in this layer?
- Why preserve source fidelity here?

**1_rhs — Raw History**
- How do you handle historization?
- What makes a good composite unique key?
- How do you ensure idempotency?

**2_1_ehs_in — Enterprise History Standardization (Input)**
- How do you standardize per-customer schemas?
- When would you use a seed file vs hardcoded mappings?

**2_2_ehs_out — Enterprise History Standardization (Output)**
- How do you union multi-customer data into enterprise artifacts?
- What are the trade-offs of view vs table here?

**3_int — Integration Layer**
- What belongs here vs 4_dm?
- Why separate Business Concepts (BC) from Events (EV)?
- How does this layer improve reuse and testability?

**4_dm — Data Marts**
- What's the data modeling focus here?
- How do dimensional models differ from INT layer models?

**5_rm — Reporting Layer**
- What's the data modeling focus here?
- What is not allowed in this layer and why?

