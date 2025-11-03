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

Design and implement a comprehensive data quality assurance strategy for Coolgradient's multi-layered Snowflake data platform. You will focus on the RHS layer, building automated tests, analytical investigations, and a roadmap for scaling quality controls across all warehouse layers.

You are joining as the first dedicated **Data Quality Engineer**. Your mandate is to ensure the RHS layer provides trustworthy, timely, and analyzable data that can flow through the rest of the stack without surprises. This assignment simulates bootstrapping a data quality program centered on RHS.

- You are free to use AI to complete the assignment - we even EXPECT you to! (we use it every day). But you will obviously fail if you cannot explain WHAT you have done and WHY in our review



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

4. **Scaling Strategy Across Layers**
   - Produce a short design note (`analyses/` or `docs/`) answering:
     - How would you scale data quality controls from RHS to EHS, INT, DM, and RM?
     - Which DMBOK data quality dimensions are most critical per layer, and what specific checks would you implement? (e.g., conformity in EHS, integrity between INT and DM, accuracy in RM)
     - Tooling or automation you would introduce (dbt exposures, CI pipelines, observability platforms, alerting).

5. **Communication & Handoff**
   - Prepare a `SUMMARY.md` (or update an existing README section) documenting:
     - Tests implemented and how to run them (`dbt test --select ...`)
     - Key findings or anomalies detected during your analysis
     - Recommendations for ongoing monitoring and owner assignment

6. **OPTIONAL STRETCH GOALS / Additional dbt Tests to Recommend/Implement**
   - Propose (and optionally implement) at least three additional dbt tests, macros, or packages that would improve RHS observability. Examples include:
     - Freshness tests with custom SLA thresholds
     - Tests validating schema drift or field type changes
     - Outlier detection macros leveraging percentile caps
   - For each proposed test, explain why it belongs in RHS and how it could be parameterized for other sites.

## Evaluation Criteria
- **Coverage**: Breadth and depth of data quality dimensions addressed in RHS.
- **SQL Proficiency**: Ability to write analytical queries that surface actionable insights.
- **dbt Mastery**: Appropriate use of built-in tests, custom macros, and test configuration.
- **Scalability Thinking**: Quality of the plan for extending controls across the warehouse layers.
- **Communication**: Clarity of documentation, naming conventions, and reproducibility.
- **Pragmatism**: Reasoned trade-offs (e.g., cost vs. rigor, incremental vs. full refresh tests).

## Constraints

- Do not alter the semantics of the provided seed data
- Focus primarily on RHS layer quality, with a design plan for other layers
- Tests should be executable via `dbt test` commands

## Submission

- Zip your repository (including new tests, analyses, and documentation) and email it to rogier.werschkull@coolgradient.com
- Include a short note describing your experience, challenges faced, and priorities for next iterations
- Be prepared to walk through your approach during the follow-up interview

## Potential Discussion Topics (be ready to discuss)

### General Topics
- Your approach to distinguishing legitimate data patterns from quality issues
- How you prioritized which tests to implement first
- Trade-offs between test coverage and computational cost
- Your strategy for making tests actionable vs just informative
- How you would handle false positives in anomaly detection

### Data Quality Dimensions
- **Completeness**: How do you detect and handle missing data or gaps?
- **Validity**: What makes a value "valid" vs "invalid" for sensor data?
- **Accuracy**: How do you validate that measurements reflect reality?
- **Consistency**: How do you ensure data is consistent across layers?
- **Timeliness**: What SLAs would you set for data freshness?
- **Uniqueness**: What makes a good composite key for time-series data?

### Layer-Specific Questions

**RHS Layer**
- Why focus data quality efforts on RHS first?
- How do you test incremental models differently than full-refresh models?
- What's the role of lookback windows in quality testing?

**Scaling Across Layers**
- Which quality dimensions are most critical at each layer (EHS, INT, DM, RM)?
- How would you prevent quality issues from propagating downstream?
- What tests belong in which layer and why?

### Tooling & Automation
- When would you use dbt native tests vs custom macros vs external tools?
- How would you integrate quality checks into CI/CD pipelines?
- What observability or alerting platforms would you recommend?
- How do you make test results accessible to non-technical stakeholders?
