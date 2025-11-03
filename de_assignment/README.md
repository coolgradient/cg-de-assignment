# CoolGradient Data Engineering Assignment

This repository contains a hands-on dbt assignment for data (quality) engineering candidates. The assignment tests your ability to:
1) For the DE test: build a layered data warehouse architecture using modern data engineering patterns.
2) For the DQ test: perform / describe various DQ tests on the synthetically generated data 

## What's Inside

This repo contains a **6-layer data architecture** that extends the medallion architecture:

- **0_lnd** — Landing: Raw data ingestion with minimal transformation
- **1_rhs** — Raw History: Incremental historization with merge logic
- **2_ehs** — Enterprise History Standardization: Schema harmonization across sources
- **3_int** — Integration: Business Concepts (BC) and Events (EV) models
- **4_dm** — Data Marts: Dimensional models for analytics
- **5_rm** — Reporting: Presentation layer with JSON payloads

## Getting Started

1. **Clone this repository**: `git clone https://github.com/coolgradient/cg-de-assignment`
2. **Read the assignment**: Open `models/interview/DE_ASSIGNMENT.md` (DE role) or `models/interview/DQ_ASSIGNMENT.md` (DQ role) 
3. **Follow the step-by-step guide** to complete the assignment
4. **Submit your work**: Email your completed repo to rogier.werschkull@coolgradient.com

## Prerequisites

- Install [Cursor IDE](https://cursor.com/)
- Install [dbt](https://docs.getdbt.com/docs/core/installation-overview)
- Get a [Snowflake trial account](https://signup.snowflake.com/)
- Configure your `profiles.yml` with Snowflake credentials

## Assignment Focus

This assignment tests:
- **Incremental data processing** and idempotency
- **Data quality** and testing practices
- **Architecture understanding** and layer separation
- **Technical decision-making** and trade-offs
- **AI-assisted development** (encouraged!)

## Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
