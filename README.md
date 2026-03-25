# GeekyBald Marketing Analytics Pipeline
> End-to-end data pipeline built with **Snowflake** and **dbt Cloud** to support product, marketing, and leadership teams at GeekyBald — an e-learning subscription platform.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Business Context](#business-context)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Key Design Decisions](#key-design-decisions)
- [Testing & Documentation](#testing--documentation)
- [How to Run](#how-to-run)
- [Author](#author)

---

## Project Overview

This project implements a production-grade analytics pipeline for GeekyBald, transforming raw operational data into clean, tested, and documented data marts that answer critical business questions across:

- **Customer acquisition & conversion funnel**
- **Subscription lifecycle & retention**
- **Email marketing performance**
- **Digital campaign ROI**
- **Website behavior & engagement**

---

## Business Context

GeekyBald is an e-learning platform offering users online courses through subscription packages:

| Package | Price |
|---|---|
| 3-month subscription | $89.99 |
| 12-month subscription | $299.99 |

Users follow a conversion funnel:
```
Signup → 7-day Free Trial → 3-month Subscription → 12-month Subscription
```

The data science team supports the **product**, **marketing**, and **leadership** teams by providing accurate, timely insights to improve customer engagement and maximize ROI during the platform's growth phase.

The dataset covers **43,898 unique users** over the period **January 2019 – March 2023**.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Snowflake** | Cloud data warehouse |
| **dbt Cloud** | Data transformation & orchestration |
| **SQL** | Data transformation logic |
| **Python** | Source data preparation & CSV cleaning |

---

## Architecture

The pipeline follows a **multi-layered architecture** that separates concerns at each stage:

```
┌─────────────────────────────────────────────────────────┐
│                     RAW DATABASE                        │
│  12 source tables loaded from CSV files                 │
│  USERS | WEB | EMAIL | MARKETING schemas                │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   STAGING LAYER                         │
│  11 models — 1:1 with source tables                     │
│  Clean, cast, rename, filter bots & internal users      │
│  Materialized as VIEWS                                  │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                 INTERMEDIATE LAYER                      │
│  1 model — int_email_funnel                             │
│  Unions 6 email event tables into unified funnel        │
│  Materialized as VIEW                                   │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                     MART LAYER                          │
│  5 models — analytics-ready, wide tables                │
│  Designed for direct consumption by BI tools & analysts │
│  Materialized as TABLES                                 │
└─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
marketing_analytics/
├── models/
│   ├── staging/
│   │   ├── users/
│   │   │   ├── sources.yml
│   │   │   └── stg_users.sql
│   │   ├── web/
│   │   │   ├── sources.yml
│   │   │   ├── stg_visits.sql
│   │   │   └── stg_conversions.sql
│   │   ├── email/
│   │   │   ├── sources.yml
│   │   │   ├── stg_email_sent.sql
│   │   │   ├── stg_email_delivered.sql
│   │   │   ├── stg_email_opened.sql
│   │   │   ├── stg_email_clicked.sql
│   │   │   ├── stg_email_bounced.sql
│   │   │   └── stg_email_spam.sql
│   │   ├── marketing/
│   │   │   ├── sources.yml
│   │   │   ├── stg_campaigns.sql
│   │   │   ├── stg_campaign_performance.sql
│   │   │   └── stg_campaign_cost.sql
│   │   └── schema.yml
│   ├── intermediate/
│   │   └── email/
│   │       └── int_email_funnel.sql
│   └── marts/
│       ├── mart_users.sql
│       ├── mart_subscription_funnel.sql
│       ├── mart_email_engagement.sql
│       ├── mart_campaign_analytics.sql
│       ├── mart_web_behavior.sql
│       └── schema.yml
├── macros/
├── seeds/
├── snapshots/
├── tests/
├── dbt_project.yml
└── README.md
```

---

## Data Models

### Staging Layer
| Model | Source | Description |
|---|---|---|
| `stg_users` | `df_users` | Cleaned user profiles, bots and internal users filtered out |
| `stg_visits` | `df_visit` | Cleaned website visit events |
| `stg_conversions` | `df_conversion` | Cleaned conversion events with type and campaign |
| `stg_email_sent` | `df_sent` | Emails sent events |
| `stg_email_delivered` | `df_delivered` | Emails delivered events |
| `stg_email_opened` | `df_open` | Emails opened events |
| `stg_email_clicked` | `df_clicked` | Emails clicked events |
| `stg_email_bounced` | `df_bounced` | Emails bounced events |
| `stg_email_spam` | `df_spam` | Emails marked as spam events |
| `stg_campaigns` | `df_campaigns` | Campaign dimension — id, name, status |
| `stg_campaign_performance` | `df_campaign` | Daily campaign performance metrics |
| `stg_campaign_cost` | `df_campaign_cost` | Daily campaign cost metrics |

### Intermediate Layer
| Model | Description |
|---|---|
| `int_email_funnel` | Unions all 6 email event tables into a single unified funnel model with a status column |

### Mart Layer
| Model | Grain | Description |
|---|---|---|
| `mart_users` | One row per user | Full user profile with conversion and visit metrics |
| `mart_subscription_funnel` | One row per user | Full subscription journey with time between each funnel stage |
| `mart_email_engagement` | One row per user per email campaign | Email funnel rates — delivery, open, click, bounce, spam |
| `mart_campaign_analytics` | One row per campaign per day | Campaign performance with CTR, CPC and cost per conversion |
| `mart_web_behavior` | One row per user | Website visit behavior, pages visited and conversion patterns |

---

## Key Design Decisions

### 1. Filtering bots and internal users at the staging layer
Bots and internal users distort conversion metrics. By filtering them in `stg_users`, all downstream models are clean by default without requiring repeated filter logic in every model.

### 2. Views for staging and intermediate, tables for marts
Staging and intermediate models are lightweight transformations that don't need to be stored. Using views avoids unnecessary Snowflake storage costs. Mart models are materialized as tables since they are queried frequently by analysts and BI tools — tables provide faster query performance at the consumption layer.

### 3. Single intermediate model for the email funnel
Six separate email event tables (sent, delivered, opened, clicked, bounced, spam) are unified into a single `int_email_funnel` model. This avoids repeating the same UNION ALL logic across multiple mart models and provides a single reusable building block for any email analysis.

### 4. Using QUALIFY for deduplication in Snowflake
Snowflake's `QUALIFY` clause with `ROW_NUMBER()` is used for deduplication instead of nested subqueries. This is more readable, more performant, and idiomatic Snowflake SQL.

### 5. Coalescing NULLs in mart aggregations
All COUNT and SUM aggregations in mart models use `COALESCE(..., 0)` to avoid NULL values propagating into downstream analysis. This makes the data safer to consume without requiring analysts to handle NULLs themselves.

### 6. Funnel stage as an ordered string
The `funnel_stage` column in `mart_subscription_funnel` uses prefixed strings (`1_signup`, `2_trial`, `3_monthly`, `4_yearly`) rather than integers. This makes the values self-documenting when viewed in a BI tool while still sorting correctly alphabetically.

### 7. Dedicated DBT_ROLE for Snowflake access
A dedicated `DBT_ROLE` with least-privilege access was created in Snowflake — read-only on the RAW database and read/write on the ANALYTICS database. This follows security best practices by separating dbt's permissions from admin roles.

---

## Testing & Documentation

The project includes **dbt tests** across all layers:

| Test Type | Coverage |
|---|---|
| `unique` | All primary keys in staging and mart models |
| `not_null` | All critical columns across all layers |
| `accepted_values` | Categorical columns — registration_type, conversion_type, campaign_status, funnel_stage, subscription_status, acquisition_type |
| `relationships` | Foreign key integrity between models |

To run all tests:
```bash
dbt test
```

To generate and view the documentation site:
```bash
dbt docs generate
dbt docs serve
```

---

## How to Run

### Prerequisites
- Snowflake account with RAW and ANALYTICS databases set up
- dbt Cloud account connected to Snowflake
- Source CSV files loaded into RAW database tables

### Run the full pipeline
```bash
# Run and test all models
dbt build

# Run only staging models
dbt build --select staging

# Run only mart models
dbt build --select marts

# Run a specific model and all its dependencies
dbt build --select +mart_subscription_funnel
```

### Development vs Production
| Environment | Schema Prefix | Purpose |
|---|---|---|
| Development | `DBT_<USERNAME>_` | Personal development sandbox |
| Production | None | Final analytics schemas consumed by BI tools |

---

## Author

Built as part of a hands-on data engineering portfolio project demonstrating:
- End-to-end pipeline design with Snowflake and dbt
- Dimensional modeling and layered architecture best practices
- Data quality testing and documentation
- Analytics engineering for product, marketing and leadership stakeholders

