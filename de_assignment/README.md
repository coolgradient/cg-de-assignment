# CoolGradient Data Engineering Assignment

This repository contains hands-on dbt assignments for data engineering candidates. Choose the assignment that matches your role:

## Assignment Types

### 📊 Data Engineering (DE) Assignment
**Focus**: Building the core reusable dbt macros for a multi-layered Snowflake pipeline (~90 minutes).

**You will be tested on**:
- Incremental `MERGE` historization with composite keys, lookback windows, and idempotency (`1_rhs`)
- Schema standardization and bounded time-series gap-fill (`2_ehs`)
- Incremental hourly event aggregation with partial-hour reprocessing (`3_int`)
- Writing reusable dbt macros and grain/schema tests
- Optional: reviewing prebuilt downstream `DM`/`RM` models for architectural and statistical flaws

**See**: `DE_ASSIGNMENT.md` for detailed requirements

---

### 🔍 Data Quality (DQ) Assignment
**Focus**: Designing and implementing a comprehensive data quality testing framework.

**You will be tested on**:
- Writing dbt schema tests (uniqueness, not-null, referential integrity)
- Creating custom data quality tests and macros for complex validations
- Detecting data anomalies (gaps, outliers, range violations, sensor failures)
- Writing analytical SQL to investigate quality issues
- Distinguishing legitimate data patterns from actual quality problems
- Designing a scalable DQ strategy across all warehouse layers
- Understanding DMBOK data quality dimensions (completeness, validity, accuracy, etc.)
- Proposing monitoring and alerting strategies

**See**: `DQ_ASSIGNMENT.md` for detailed requirements 

## What's Inside

This repo contains a **6-layer data architecture** that extends the medallion architecture:

- **0_lnd** — Landing: Raw data ingestion with minimal transformation
- **1_rhs** — Raw History: Incremental historization with merge logic
- **2_ehs** — Enterprise History Standardization: Schema harmonization across sources
- **3_int** — Integration: Business Concepts (BC) and Events (EV) models
- **4_dm** — Data Marts: Dimensional models for analytics
- **5_rm** — Reporting: Presentation layer with JSON payloads



---

## Getting Started

1. **Clone this repository**: `git clone https://github.com/coolgradient/cg-de-assignment`
2. **Choose your assignment**: 
   - Data Engineering → Read `DE_ASSIGNMENT.md`
   - Data Quality → Read `DQ_ASSIGNMENT.md`
3. **Follow the step-by-step guide** to complete your chosen assignment
4. **Submit your work**: Email your completed repo to rogier.werschkull@coolgradient.com

## Prerequisites

- Install [Cursor IDE](https://cursor.com/)
- Install [dbt](https://docs.getdbt.com/docs/core/installation-overview)
- Get a [Snowflake trial account](https://signup.snowflake.com/)
- Configure your `profiles.yml` with Snowflake credentials

## Key Skills Tested (Both Assignments)

- **SQL proficiency** and analytical thinking
- **dbt best practices** (models, tests, macros, documentation)
- **Architecture understanding** and layer separation
- **Technical decision-making** and trade-offs
- **Communication** and documentation skills
- **AI-assisted development** (encouraged! We use it daily)

## Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
