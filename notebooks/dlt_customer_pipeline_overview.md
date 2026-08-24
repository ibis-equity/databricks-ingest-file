# DLT Customer Pipeline 2026-06-10 13:39 - Comprehensive Overview

This is a Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables) that implements a production-grade, multi-layered customer data processing system. The pipeline follows the Medallion Architecture pattern (Bronze → Silver → Gold) to transform raw customer data into business-ready analytics.

## Architecture Overview

The pipeline operates across three Unity Catalog schemas:

- Bronze: `workspace.etl-bronze-pipeline-data` — raw ingestion layer
- Silver: `workspace.etl-silver-pipeline-data` — cleaned and standardized data
- Gold: `workspace.etl-gold-pipeline-data` — business analytics and aggregations

## Key Components & Data Flow

### 1. Schema Discovery & Metadata Layer

`bronze_schema_catalog`: A metadata table that automatically discovers and documents the schema of all bronze source tables. This provides:

- Complete column inventory across source tables
- Data type tracking for each column
- Schema change monitoring over time
- Foundation for identifying schema inconsistencies

Purpose: Essential for understanding data structure, planning standardization strategies, and detecting schema drift.

### 2. Bronze Layer: Dynamic Source Discovery

`customers_bronze_all`: The entry point that automatically discovers and unifies all tables in the bronze schema. Key features:

- Auto-discovery: Uses Spark SQL to list all tables in the bronze schema dynamically
- Graceful failure handling: Skips problematic tables rather than failing the entire pipeline
- Column mapping: Standardizes inconsistent naming (`customer_id` → `cust_id`, `total_spend` → `amount_paid`)
- Source tracking: Adds `source_table` metadata to track data provenance
- Union logic: Combines all bronze tables using `unionByName` with missing column tolerance

Data quality: Applies expectations to warn on missing source tables or records without identifiers.

### 3. Supporting Infrastructure

`zip_lookup`: A reference data table that reads from `workspace.default.us_zip_lookup` (if it exists) to provide ZIP code → city/state mappings. Includes fallback logic to return an empty DataFrame if the lookup table is missing, preventing pipeline failures.

`pipeline_zero_record_alerts`: A monitoring table that tracks empty datasets across all pipeline layers, providing operational visibility into data flow issues.

### 4. Silver Layer: Comprehensive Data Cleaning

`customers_silver_all`: The heart of the pipeline's data quality engine. This dataset applies extensive transformations:

#### Data Standardization

- Trims all string columns
- Normalizes names to proper case (`initcap`)
- Standardizes addresses and state codes (uppercase)

#### Data Repair

- Auto-generates customer IDs for records missing them (`CUST-AUTO-<id>`)
- Cleans ZIP codes (removes non-numeric characters, pads to 5 digits)
- Validates and cleans email addresses using regex patterns
- Extracts email domains and categorizes as personal vs corporate
- Replaces invalid emails with structured placeholders (`noemail_<cust_id>@missing.email`)

#### Financial Data Cleaning

- Removes currency symbols and commas from `amount_paid`
- Handles invalid values (`pending`, `unknown`, `n/a`, `#REF!`, `void`)
- Uses `try_cast` for safe numeric conversion

#### Enrichment

- Left joins with ZIP lookup to enrich city/state from ZIP codes
- Adds derived fields: `email_domain`, `email_provider_type`

#### Deduplication

- Uses window functions to deduplicate by `cust_id` and `source_table`
- Keeps the record with the highest `amount_paid` per group

#### NULL Handling

- Fills all NULL strings with meaningful defaults (e.g., `"Unknown"`, `"Not Provided"`)
- Sets NULL amounts to `0.0`
- Ensures no NULLs propagate to gold layer

#### Data Quality

Drops records that fail critical validation:

- Valid customer ID (not null or empty)
- Amount paid is reasonable (≥ -100,000)
- ZIP codes are exactly 5 digits (if present)

### 5. Data Quality Reporting

`customers_silver_all_dqr`: A data quality report that calculates null counts per column per source table, providing visibility into data completeness and quality trends over time.

### 6. Gold Layer: Business Analytics (8 datasets)

The gold layer produces multiple purpose-built analytical tables:

#### a) `customers_gold_rfm_clv`: RFM (Recency, Frequency, Monetary) Analysis

- Calculates customer lifetime value proxy
- Segments customers by:
  - Value: High (≥ $1000), Medium (≥ $250), Low
  - Recency: Recent (≤ 30 days), Warm (≤ 90 days), Cold
  - Frequency: Frequent (≥ 10 txns), Occasional (≥ 3), Rare
- Tracks last transaction date and days since last purchase

#### b) `customers_gold_segments_summary`: Segment Distribution

- Aggregates customer counts by segment combination
- Calculates average monetary value and CLV by segment
- Provides executive-level view of customer portfolio composition

#### c) `customers_gold_top_spenders`: Top 100 Customers by Lifetime Spend

- Ranks customers by total spend
- Includes full customer profile with geographic details
- Tracks transaction count, average transaction value, and customer lifetime (days)
- Provides actionable list for VIP customer programs

#### d) `customers_gold_revenue_by_geography`: Geographic Revenue Analysis

- Aggregates revenue by state and city
- Calculates unique customers, transaction counts, and per-customer metrics
- Ranks cities within states and overall
- Identifies high-value geographic markets

#### e) `customers_gold_revenue_trends_daily`: Time-Series Revenue Analysis

- Daily revenue aggregation with transaction counts
- 7-day rolling averages for revenue and transactions
- Day-over-day revenue growth percentage
- Day-of-week analysis for seasonality detection

#### f) `customers_gold_state_kpis`: Comprehensive State-Level Dashboard

- Revenue, customer count, and transaction metrics by state
- Revenue per customer and transactions per customer
- High-value customer percentage by state
- Recent customer percentage (engagement metric)
- State ranking by revenue

#### g) `customers_gold_email_analysis`: Email Intelligence

- Email domain distribution with revenue metrics
- Personal vs corporate email split
- Domain ranking by customer count and revenue
- Average transaction value by email provider type
- Identifies potential data quality issues and customer preferences

#### h) `customers_gold_email_provider_summary`: Email Provider Overview

- High-level summary of personal vs corporate email usage
- Revenue and customer counts by provider type
- Transaction patterns by email category

#### i) `customers_gold_duplicate_emails`: Data Quality - Duplicate Detection

- Identifies email addresses shared across multiple customer IDs
- Severity classification (`HIGH`/`MEDIUM`/`LOW` based on duplication count)
- Lists all customer IDs and names associated with each duplicate email
- Tracks combined spend across duplicate accounts
- Provides merge candidates for customer deduplication efforts

## Technical Features

### Resilience

- Graceful handling of missing tables, schemas, and lookup data
- Try-catch blocks around table discovery and loading operations
- Empty DataFrame fallbacks prevent pipeline failures

### Observability

- Comprehensive logging at INFO level throughout execution
- Zero-record alert monitoring
- Data quality reports
- Source table tracking in all datasets

### Performance

- Window functions for efficient ranking and deduplication
- Pre-computed column lists to avoid repeated schema operations
- Strategic use of caching implicit in DLT materialized views

### Data Governance

- Expectations at Bronze, Silver, and Gold layers
- Source lineage tracking via `source_table` column
- Timestamped reports for audit trails

## Pipeline Execution Model

The pipeline uses the legacy `dlt` import (``import dlt``) rather than the modern `from pyspark import pipelines as dp` syntax. All datasets are defined as materialized tables using `@dlt.table()` decorators.

Flow: Bronze tables → Schema catalog + Unified bronze → Silver cleaning → Multiple gold analytics tables

Expectations: The pipeline uses a mix of:

- `@dlt.expect_all`: Warn on violations but keep records
- `@dlt.expect_all_or_drop`: Drop records that violate quality rules

## Business Value

This pipeline enables:

1. Customer Segmentation: RFM analysis for targeted marketing
2. Geographic Strategy: Revenue analysis by location for expansion planning
3. Revenue Forecasting: Daily trends with moving averages
4. Customer Intelligence: Email behavior, provider preferences, and duplicate detection
5. Data Quality Management: Comprehensive quality reporting and monitoring
6. Operational Visibility: Zero-record alerts and schema change tracking

## Notable Design Decisions

### Commented-Out Archival Function

Lines 1064-1173 contain a bronze table archival function that is disabled. The comments explain that DDL commands (`CREATE SCHEMA`, `DROP TABLE`) are not allowed within `@dlt.table()` decorators, so this functionality has been moved to a separate notebook called `Bronze_Table_Archival`.

### Legacy API Usage

The pipeline uses the deprecated `dlt` import and API (`dlt.table()`, `dlt.read()`) rather than the current `dp` syntax. While still functional, this could be modernized.

## Summary

This pipeline represents a production-ready, enterprise-grade data processing system that balances robustness, observability, and business value delivery.
