# Coolgradient Data Quality Engineering Assignment

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
This assignment evaluates your ability to design and implement a comprehensive data quality assurance strategy for Coolgradient's multi-layered Snowflake data platform. You will work in the same repository used for the data engineering exercise, but with a focus on data quality observability, automated testing, and analytical SQL capabilities. Expect to reason about which tests belong in which warehouse layer, how to operationalize them, and how to communicate findings.

> **Important:** Before you start, also read `README.md`.
You will reuse the synthetic data generators in `0_lnd` as the foundation for all downstream quality checks.


## Scenario
You are joining Coolgradient as the first dedicated **Data Quality Engineer**. The Gradient AI platform relies on a layered warehouse where the `1_rhs` layer stores incrementally historized raw data. Your mandate is to ensure that the RHS layer provides trustworthy, timely, and analyzable data that can flow through the rest of the stack without surprises. This assignment simulates how you would bootstrap a data quality program centered on RHS, while designing a roadmap for scaling controls across all layers.

## What You Will Deliver
1. **Executable setup**
   - Run the landing data generators to create fresh input tables. For example:
     ```bash
     dbt run -s generate_lnd_interview_data_SITE1
     dbt run -s generate_lnd_interview_data_SITE2
     ```
   - Confirm row counts and date ranges in the newly created landing tables. Document the checks you performed.

2. **RHS Data Quality Test Suite (focus of the assignment)**
   - Configure **standard dbt schema tests** (YAML-based) for built-in assertions such as:
     - **Uniqueness**: Ensure composite keys (e.g., `site`, `asset_id`, `event_ts`, `datapoint`) remain unique over the historized table.
     - **Not Null / Required Fields**: Validate that critical columns (timestamps, asset identifiers, sensor readings) are always populated.
   - Develop **custom data tests or macros** under `tests/` for controls that exceed the built-in capabilities. Cover at least the following dimensions:
     - **Completeness / Holes Detection**: Detect gaps in hourly measurements per asset and datapoint. Implement a window-based query that flags missing intervals within a rolling 48h horizon.
     - **Validity / Range Anomalies**: Build a sliding-window statistical test (e.g., 7-day rolling mean ± 3σ) that raises failures when temperature or humidity values drift beyond expected bounds.
   - When evaluating anomalies, explain how you would distinguish legitimate shifts (e.g., asset set point changes) from sensor malfunctions or breakages so that alerts remain actionable.
   - Demonstrate how each test can run via `dbt test`, and document expected failure modes versus passing criteria.

3. **Analytical SQL Investigations**
   - Write at least three investigative SQL queries (save them in `analyses/` or as dbt models) that help stakeholders debug data issues uncovered by the tests. Examples:
     - Trend of failed uniqueness records by day and site
     - Distribution of measurement gaps per asset type
     - Comparison of sliding-window z-scores before/after an anomaly period
   - Summarize insights from your queries in a short README snippet or comments.

4. **Additional dbt Tests to Recommend/Implement**
   - Propose (and optionally implement) at least three additional dbt tests, macros, or packages that would improve RHS observability. Examples include:
     - Freshness tests with custom SLA thresholds
     - Tests validating schema drift or field type changes
     - Outlier detection macros leveraging percentile caps
   - For each proposed test, explain why it belongs in RHS and how it could be parameterized for other sites.

5. **Scaling Strategy Across Layers**
   - Produce a short design note (`analyses/` or `docs/`) answering:
     - How would you scale data quality controls from RHS to EHS, INT, DM, and RM?
     - Which DMBOK data quality dimensions are most critical per layer, and what specific checks would you implement? (e.g., conformity in EHS, integrity between INT and DM, accuracy in RM)
     - Tooling or automation you would introduce (dbt exposures, CI pipelines, observability platforms, alerting).

6. **Communication & Handoff**
   - Prepare a `SUMMARY.md` (or update an existing README section) documenting:
     - Tests implemented and how to run them (`dbt test --select ...`)
     - Key findings or anomalies detected during your analysis
     - Recommendations for ongoing monitoring and owner assignment

## Evaluation Criteria
- **Coverage**: Breadth and depth of data quality dimensions addressed in RHS.
- **SQL Proficiency**: Ability to write analytical queries that surface actionable insights.
- **dbt Mastery**: Appropriate use of built-in tests, custom macros, and test configuration.
- **Scalability Thinking**: Quality of the plan for extending controls across the warehouse layers.
- **Communication**: Clarity of documentation, naming conventions, and reproducibility.
- **Pragmatism**: Reasoned trade-offs (e.g., cost vs. rigor, incremental vs. full refresh tests).

## Stretch Goals (Optional)
- Integrate a data quality library (e.g., dbt-expectations, elementary, Great Expectations via dbt) and show how it complements native tests.

## Submission
- Zip your repository (including new tests, analyses, and documentation) and email it to rogier.werschkull@coolgradient.com.
- Include a short note describing your experience, challenges faced, and priorities for next iterations.
- Be prepared to walk through your approach during the follow-up interview.

Good luck, and thanks for helping us keep Coolgradient's data reliable!