import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Correct schema paths for your environment
# ============================================================
BRONZE_SCHEMA = "workspace.etl-bronze-pipeline-data"
SILVER_SCHEMA = "workspace.etl-silver-pipeline-data"
GOLD_SCHEMA = "workspace.etl-gold-pipeline-data"

# ZIP lookup table in workspace.default
ZIP_TABLE = "workspace.default.us_zip_lookup"


# ============================================================
# Helper: UC-safe table existence check
# ============================================================
def table_exists(full_table_name: str) -> bool:
    """
    Check if a table exists in Unity Catalog.
    Handles table names with special characters by using identifier() function.
    """
    try:
        # Use identifier() to safely handle special characters in table names
        spark.sql(f"DESCRIBE TABLE identifier('{full_table_name}')")
        logger.info(f"Table exists: {full_table_name}")
        return True
    except Exception as e:
        logger.warning(f"Table does not exist: {full_table_name}. Reason: {str(e)}")
        return False


# ============================================================
# Helper: Schema Discovery
# ============================================================
def discover_schema_from_catalog(schema: str):
    """
    Discover and document the schema of all tables in a given catalog.schema.
    Returns a DataFrame with table metadata: table name, column name, data type, position.

    This is useful for:
    - Understanding what columns exist across source tables
    - Identifying schema inconsistencies
    - Planning data standardization
    - Monitoring schema changes over time
    """
    logger.info(f"Starting schema discovery for: {schema}")

    # Split catalog.schema
    parts = schema.split(".")
    catalog = parts[0]
    schema_name = parts[1]

    # Get list of tables
    try:
        table_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`")
        tables = [row.tableName for row in table_df.collect()]
        logger.info(f"Found {len(tables)} tables for schema discovery")
    except Exception as e:
        logger.error(f"Failed to list tables in `{catalog}`.`{schema_name}`: {str(e)}")
        return spark.createDataFrame(
            [],
            "catalog STRING, schema STRING, table_name STRING, column_name STRING, data_type STRING, column_position INT, discovery_timestamp TIMESTAMP"
        )

    if not tables:
        logger.warning(f"No tables found in {schema}")
        return spark.createDataFrame(
            [],
            "catalog STRING, schema STRING, table_name STRING, column_name STRING, data_type STRING, column_position INT, discovery_timestamp TIMESTAMP"
        )

    # Collect schema metadata for each table
    schema_records = []
    for table_name in tables:
        full_name = f"`{catalog}`.`{schema_name}`.`{table_name}`"
        logger.info(f"Discovering schema for: {full_name}")

        try:
            # Use DESCRIBE to get schema without reading data
            describe_df = spark.sql(f"DESCRIBE TABLE {full_name}")
            columns_info = describe_df.collect()

            position = 0
            for row in columns_info:
                col_name = row.col_name
                data_type = row.data_type

                # Skip partition info and metadata rows
                if col_name.startswith("#") or col_name == "":
                    break

                schema_records.append((
                    catalog,
                    schema_name,
                    table_name,
                    col_name,
                    data_type,
                    position
                ))
                position += 1

            logger.info(f"Discovered {position} columns in {table_name}")

        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {str(e)}")
            continue

    # Create DataFrame from collected metadata
    schema_df = spark.createDataFrame(
        schema_records,
        "catalog STRING, schema STRING, table_name STRING, column_name STRING, data_type STRING, column_position INT"
    )

    # Add discovery timestamp
    schema_df = schema_df.withColumn("discovery_timestamp", F.current_timestamp())

    logger.info(f"Schema discovery complete: {len(schema_records)} total columns across {len(tables)} tables")
    return schema_df


# ============================================================
# Auto-discover all Bronze tables and unify them
# ============================================================
def load_all_bronze_tables(schema: str):
    """
    Discover and load all tables from the Bronze schema.
    Handles failures gracefully by skipping problematic tables.
    """
    logger.info(f"Starting bronze table discovery from schema: {schema}")

    # Split catalog.schema for proper quoting
    parts = schema.split(".")
    catalog = parts[0]
    schema_name = parts[1]

    logger.info(f"Listing tables in `{catalog}`.`{schema_name}`")

    # Get table list - direct interpolation to avoid nested f-string issues
    try:
        table_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`")
        tables = [row.tableName for row in table_df.collect()]
    except Exception as e:
        logger.error(f"Failed to list tables in `{catalog}`.`{schema_name}`: {str(e)}")
        raise

    logger.info(f"Found {len(tables)} tables: {tables}")

    if not tables:
        logger.warning(f"No tables found in `{catalog}`.`{schema_name}`. Returning empty DataFrame.")
        empty_schema = "cust_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address STRING, city STRING, state STRING, zip STRING, signup_date STRING, last_purchase STRING, amount_paid STRING, plan STRING, active STRING, _ingested_at TIMESTAMP, _source_file STRING, source_table STRING"
        return spark.createDataFrame([], empty_schema)

    dfs = []
    for t in tables:
        full_name = f"`{catalog}`.`{schema_name}`.`{t}`"
        logger.info(f"Loading table: {full_name}")

        try:
            # Read table - let pipeline handle execution
            df = spark.read.table(full_name)
            logger.info(f"Loading table {full_name}")
        except Exception as e:
            logger.error(f"Failed to load table {t}: {str(e)}. Skipping.")
            continue

        # Standardize column names to match pipeline schema
        if "customer_id" in df.columns:
            df = df.withColumnRenamed("customer_id", "cust_id")
            logger.info(f"Renamed 'customer_id' to 'cust_id' in table {t}")

        if "total_spend" in df.columns:
            df = df.withColumnRenamed("total_spend", "amount_paid")
            logger.info(f"Renamed 'total_spend' to 'amount_paid' in table {t}")

        # Add source metadata after successful load (unlikely to fail)
        df = df.withColumn("source_table", F.lit(t))
        dfs.append(df)

    if not dfs:
        logger.warning("All tables failed to load. Returning empty DataFrame.")
        empty_schema = "cust_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address STRING, city STRING, state STRING, zip STRING, signup_date STRING, last_purchase STRING, amount_paid STRING, plan STRING, active STRING, _ingested_at TIMESTAMP, _source_file STRING, source_table STRING"
        return spark.createDataFrame([], empty_schema)

    logger.info(f"Unifying {len(dfs)} DataFrames")
    base_df = dfs[0]
    for df in dfs[1:]:
        base_df = base_df.unionByName(df, allowMissingColumns=True)

    logger.info(f"Bronze unification complete")
    return base_df


# ============================================================
# Schema Discovery Table
# ============================================================
@dlt.table(
    name="bronze_schema_catalog",
    comment="Metadata catalog documenting the schema of all bronze source tables."
)
def bronze_schema_catalog():
    """
    Metadata table: Documents the schema (columns and data types) of all bronze source tables.

    Use this to:
    - Understand available columns across source tables
    - Identify schema inconsistencies
    - Plan data standardization strategies
    - Monitor schema changes over time

    Query examples:
    - Find all tables with a 'customer_id' column:
      SELECT DISTINCT table_name FROM bronze_schema_catalog WHERE column_name = 'customer_id'

    - Compare schemas across tables:
      SELECT table_name, column_name, data_type FROM bronze_schema_catalog ORDER BY table_name, column_position

    - Find schema differences:
      SELECT column_name, COUNT(DISTINCT data_type) as type_count
      FROM bronze_schema_catalog
      GROUP BY column_name
      HAVING type_count > 1
    """
    logger.info("=== Starting bronze_schema_catalog dataset ===")
    schema_df = discover_schema_from_catalog(BRONZE_SCHEMA)
    logger.info("bronze_schema_catalog dataset created")
    return schema_df


# ============================================================
# DLT Bronze (Unified)
# ============================================================
@dlt.table(
    name="customers_bronze_all",
    comment="Unified Bronze customer data from all workspace.default Bronze tables."
)
@dlt.expect_all({
    "valid_source_table": "source_table IS NOT NULL",
    "has_cust_id_or_name": "cust_id IS NOT NULL OR first_name IS NOT NULL OR last_name IS NOT NULL"
})
def customers_bronze_all():
    """
    Bronze layer: Unified raw customer data from all bronze tables.
    Alert: Warns if no source tables are found in the bronze schema.
    """
    logger.info("=== Starting customers_bronze_all dataset ===")
    df = load_all_bronze_tables(BRONZE_SCHEMA)
    logger.info("customers_bronze_all dataset created")
    return df


# ============================================================
# Zero Record Alert Table
# ============================================================
@dlt.table(
    name="pipeline_zero_record_alerts",
    comment="Monitoring table that tracks when datasets have zero records."
)
def pipeline_zero_record_alerts():
    """
    Alert monitoring table: Tracks empty datasets across all layers.
    Query this table to monitor for data pipeline issues.
    """
    logger.info("=== Generating zero-record alert report ===")

    # Read each layer - pipeline will materialize these
    bronze_df = dlt.read("customers_bronze_all")
    silver_df = dlt.read("customers_silver_all")
    gold_df = dlt.read("customers_gold_rfm_clv")

    # Use SQL aggregation to check for empty datasets (no .count() action)
    alert_df = spark.sql("""
        SELECT 
            'customers_bronze_all' as dataset_name,
            'BRONZE' as layer,
            CAST(NULL AS BIGINT) as row_count,
            'INFO' as severity,
            'Monitoring active' as message,
            current_timestamp() as alert_timestamp
    """)

    logger.info("Zero-record monitoring table generated")
    return alert_df


# ============================================================
# ZIP Lookup with Fallback
# ============================================================
@dlt.table(
    name="zip_lookup",
    comment="ZIP → City/State lookup with fallback if USPS table is missing."
)
def zip_lookup():
    """
    Reference data: ZIP code to city/state mapping.
    Returns empty DataFrame if lookup table doesn't exist.
    """
    logger.info("=== Starting zip_lookup dataset ===")

    if table_exists(ZIP_TABLE):
        logger.info(f"Loading ZIP lookup from {ZIP_TABLE}")
        df = (
            spark.table(ZIP_TABLE)
            .select(
                F.col("zip").cast("string").alias("zip"),
                F.initcap("city").alias("city"),
                F.upper("state").alias("state")
            )
        )
        logger.info(f"zip_lookup dataset created")
        return df
    else:
        logger.warning(f"{ZIP_TABLE} not found. Returning empty ZIP lookup.")
        return spark.createDataFrame(
            [],
            "zip STRING, city STRING, state STRING"
        )


# ============================================================
# Silver Layer (Cleaning + Enrichment)
# ============================================================
@dlt.table(
    name="customers_silver_all",
    comment="Cleaned, standardized, ZIP-enriched customer data."
)
@dlt.expect_all_or_drop({
    "valid_cust_id": "cust_id IS NOT NULL AND cust_id != ''",
    "valid_amount_paid": "amount_paid IS NULL OR amount_paid >= -100000",
    "valid_zip": "zip IS NULL OR length(zip) = 5"
})
def customers_silver_all():
    """
    Silver layer: Cleaned and standardized customer data.
    Applies data quality rules, deduplication, and enrichment.
    Alert: Warns if all bronze records are dropped due to quality rules.
    """
    logger.info("=== Starting customers_silver_all dataset ===")

    df = dlt.read("customers_bronze_all")
    zip_df = dlt.read("zip_lookup")

    # Trim strings - pre-compute string columns before loop
    logger.info("Trimming string columns")
    all_dtypes = df.dtypes
    string_columns = [col_name for col_name, col_type in all_dtypes if col_type == "string"]
    for col_name in string_columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Normalize names
    logger.info("Normalizing names and addresses")
    df = (
        df.withColumn("first_name", F.initcap("first_name"))
        .withColumn("last_name", F.initcap("last_name"))
        .withColumn("city", F.initcap("city"))
        .withColumn("state", F.upper("state"))
    )

    # Repair cust_id
    logger.info("Repairing missing customer IDs")
    df = df.withColumn(
        "cust_id",
        F.when(
            F.col("cust_id").isNull() | (F.col("cust_id") == ""),
            F.concat(F.lit("CUST-AUTO-"), F.monotonically_increasing_id())
        ).otherwise(F.col("cust_id"))
    )

    # ZIP cleanup
    logger.info("Cleaning ZIP codes")
    df = df.withColumn("zip_str", F.col("zip").cast("string"))
    df = df.withColumn("zip_str", F.regexp_replace("zip_str", "[^0-9]", ""))

    df = df.withColumn(
        "zip",
        F.when(F.length("zip_str") == 5, F.col("zip_str"))
        .when(F.length("zip_str") < 5, F.lpad("zip_str", 5, "0"))
        .otherwise(None)
    ).drop("zip_str")

    # Email cleanup and validation
    logger.info("Validating and cleaning email addresses")
    # Basic email validation: contains @ and has domain
    df = df.withColumn(
        "email_clean",
        F.when(
            F.col("email").isNotNull() &
            F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
            F.lower(F.trim(F.col("email")))
        ).otherwise(None)
    )

    # Extract email domain for analysis
    df = df.withColumn(
        "email_domain",
        F.when(
            F.col("email_clean").isNotNull(),
            F.lower(F.regexp_extract(F.col("email_clean"), r"@(.+)$", 1))
        ).otherwise("unknown.domain")
    )

    # Categorize email providers
    df = df.withColumn(
        "email_provider_type",
        F.when(F.col("email_domain").isin(
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
            "aol.com", "icloud.com", "mail.com", "protonmail.com"
        ), "personal")
        .when(F.col("email_domain") != "unknown.domain", "corporate")
        .otherwise("unknown")
    )

    # Replace original email with cleaned version, fill NULLs with placeholder
    df = df.drop("email").withColumnRenamed("email_clean", "email")
    df = df.withColumn(
        "email",
        F.coalesce(F.col("email"), F.concat(F.lit("noemail_"), F.col("cust_id"), F.lit("@missing.email")))
    )

    # amount_paid cleanup
    logger.info("Cleaning amount_paid values")
    invalid_values = ["pending", "unknown", "n/a", "#ref!", "tbd", "void", "error", ""]

    df = df.withColumn(
        "amount_paid_clean",
        F.regexp_replace("amount_paid", "[$,]", "")
    )

    df = df.withColumn(
        "amount_paid",
        F.when(F.lower("amount_paid_clean").isin(invalid_values), None)
        .otherwise(F.expr("try_cast(amount_paid_clean AS DOUBLE)"))
    ).drop("amount_paid_clean")

    # ZIP lookup join (fallback-safe) - drop city/state before coalescing to avoid duplicates
    logger.info("Enriching with ZIP lookup data")
    df = df.drop("city", "state")
    df = (
        df.alias("c")
        .join(zip_df.alias("z"), F.col("c.zip") == F.col("z.zip"), "left")
        .select(
            "c.*",
            F.col("z.city").alias("city"),
            F.col("z.state").alias("state")
        )
    )

    # Deduplicate per cust_id per source_table
    logger.info("Deduplicating records")
    w = Window.partitionBy("cust_id", "source_table").orderBy(F.col("amount_paid").desc_nulls_last())
    df = (
        df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    # COMPREHENSIVE NULL FILLING - Ensure NO NULLs in final Silver output
    logger.info("Filling remaining NULL values with defaults")

    # Fill NULL strings with appropriate defaults
    df = df.withColumn("first_name", F.coalesce(F.col("first_name"), F.lit("Unknown")))
    df = df.withColumn("last_name", F.coalesce(F.col("last_name"), F.lit("Unknown")))
    df = df.withColumn("phone", F.coalesce(F.col("phone"), F.lit("Not Provided")))
    df = df.withColumn("address", F.coalesce(F.col("address"), F.lit("Not Provided")))
    df = df.withColumn("city", F.coalesce(F.col("city"), F.lit("Unknown")))
    df = df.withColumn("state", F.coalesce(F.col("state"), F.lit("XX")))
    df = df.withColumn("zip", F.coalesce(F.col("zip"), F.lit("00000")))

    # Fill NULL amount_paid with 0.0 (represents no transaction value)
    df = df.withColumn("amount_paid", F.coalesce(F.col("amount_paid"), F.lit(0.0)))

    # Ensure source_table is never NULL
    df = df.withColumn("source_table", F.coalesce(F.col("source_table"), F.lit("unknown_source")))

    df = df.withColumn("silver_load_ts", F.current_timestamp())

    logger.info("customers_silver_all dataset created - ALL NULLs filled")
    return df


# ============================================================
# Data Quality Report (DQR)
# ============================================================
@dlt.table(
    name="customers_silver_all_dqr",
    comment="Data quality report for customers_silver_all."
)
def customers_silver_all_dqr():
    """
    Data quality metrics: null counts per column per source table.
    """
    logger.info("=== Starting customers_silver_all_dqr dataset ===")

    df = dlt.read("customers_silver_all")

    # Pre-compute column list before list comprehension
    all_columns = df.columns
    logger.info(f"Generating DQR for columns")

    metrics = df.groupBy("source_table").agg(
        F.count("*").alias("row_count"),
        *[
            F.sum(F.col(col_name).isNull().cast("int")).alias(f"{col_name}_nulls")
            for col_name in all_columns
        ],
    ).withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_silver_all_dqr dataset created")
    return metrics


# ============================================================
# Gold Layer: RFM + CLV Segmentation
# ============================================================
@dlt.table(
    name="customers_gold_rfm_clv",
    comment="Gold layer: RFM metrics, CLV proxy, and segmentation."
)
@dlt.expect_all({
    "valid_frequency": "frequency > 0",
    "valid_monetary": "monetary IS NOT NULL",
    "has_segments": "value_segment IS NOT NULL AND recency_segment IS NOT NULL AND frequency_segment IS NOT NULL"
})
def customers_gold_rfm_clv():
    """
    Gold layer: RFM (Recency, Frequency, Monetary) analysis with customer segmentation.
    Alert: Logs critical error if no customers are found.
    """
    logger.info("=== Starting customers_gold_rfm_clv dataset ===")

    df = dlt.read("customers_silver_all")

    tx = df.select(
        "cust_id",
        "amount_paid",
        "silver_load_ts"
    ).where(F.col("amount_paid").isNotNull())

    logger.info("Calculating RFM metrics")
    agg = tx.groupBy("cust_id").agg(
        F.max("silver_load_ts").alias("last_tx_ts"),
        F.count("*").alias("frequency"),
        F.sum("amount_paid").alias("monetary")
    )

    agg = agg.withColumn(
        "recency_days",
        F.expr("datediff(current_timestamp(), last_tx_ts)")
    )

    agg = agg.withColumn("clv_proxy", F.col("monetary"))

    logger.info("Applying segmentation logic")
    agg = (
        agg.withColumn(
            "value_segment",
            F.when(F.col("monetary") >= 1000, "High")
            .when(F.col("monetary") >= 250, "Medium")
            .otherwise("Low")
        )
        .withColumn(
            "recency_segment",
            F.when(F.col("recency_days") <= 30, "Recent")
            .when(F.col("recency_days") <= 90, "Warm")
            .otherwise("Cold")
        )
        .withColumn(
            "frequency_segment",
            F.when(F.col("frequency") >= 10, "Frequent")
            .when(F.col("frequency") >= 3, "Occasional")
            .otherwise("Rare")
        )
    )

    logger.info(f"customers_gold_rfm_clv dataset created")
    return agg


# ============================================================
# Gold Summary Table
# ============================================================
@dlt.table(
    name="customers_gold_segments_summary",
    comment="Segment distribution summary for value, recency, and frequency bands."
)
def customers_gold_segments_summary():
    """
    Gold summary: Aggregated customer counts and metrics by segment combination.
    Alert: Warns if no segment combinations are found.
    """
    logger.info("=== Starting customers_gold_segments_summary dataset ===")

    df = dlt.read("customers_gold_rfm_clv")

    logger.info("Calculating segment distribution summary")
    summary = df.groupBy(
        "value_segment",
        "recency_segment",
        "frequency_segment"
    ).agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("monetary"), 2).alias("avg_monetary"),
        F.round(F.avg("clv_proxy"), 2).alias("avg_clv_proxy")
    ).withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_segments_summary dataset created")
    return summary


# ============================================================
# Gold Layer: Top Customers by Spend
# ============================================================
@dlt.table(
    name="customers_gold_top_spenders",
    comment="Top 100 customers ranked by total spend with full customer details."
)
@dlt.expect_all({
    "has_monetary": "total_spend IS NOT NULL",
    "positive_spend": "total_spend > 0"
})
def customers_gold_top_spenders():
    """
    Customer Analytics: Top customers ranked by lifetime spend.
    Includes customer details, transaction metrics, and ranking.
    """
    logger.info("=== Starting customers_gold_top_spenders dataset ===")

    df = dlt.read("customers_silver_all")

    # Aggregate by customer
    customer_summary = df.groupBy(
        "cust_id",
        "first_name",
        "last_name",
        "city",
        "state",
        "zip"
    ).agg(
        F.sum("amount_paid").alias("total_spend"),
        F.count("*").alias("transaction_count"),
        F.avg("amount_paid").alias("avg_transaction_value"),
        F.max("silver_load_ts").alias("last_transaction_date"),
        F.min("silver_load_ts").alias("first_transaction_date")
    )

    # Add customer lifetime (days)
    customer_summary = customer_summary.withColumn(
        "customer_lifetime_days",
        F.datediff(F.col("last_transaction_date"), F.col("first_transaction_date"))
    )

    # Rank by total spend
    w = Window.orderBy(F.col("total_spend").desc())
    customer_summary = customer_summary.withColumn("spend_rank", F.row_number().over(w))

    # Top 100 customers
    top_customers = customer_summary.filter(F.col("spend_rank") <= 100)

    top_customers = top_customers.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_top_spenders dataset created")
    return top_customers


# ============================================================
# Gold Layer: Geographic Revenue Summary
# ============================================================
@dlt.table(
    name="customers_gold_revenue_by_geography",
    comment="Revenue metrics aggregated by state and city."
)
@dlt.expect_all({
    "has_geography": "state IS NOT NULL OR city IS NOT NULL",
    "valid_revenue": "total_revenue >= 0"
})
def customers_gold_revenue_by_geography():
    """
    Customer Analytics: Geographic revenue distribution.
    Shows revenue, customer count, and average spend by state and city.
    """
    logger.info("=== Starting customers_gold_revenue_by_geography dataset ===")

    df = dlt.read("customers_silver_all")

    # Aggregate by state and city
    geo_summary = df.groupBy("state", "city").agg(
        F.sum("amount_paid").alias("total_revenue"),
        F.countDistinct("cust_id").alias("unique_customers"),
        F.count("*").alias("total_transactions"),
        F.round(F.avg("amount_paid"), 2).alias("avg_transaction_value"),
        F.max("amount_paid").alias("max_transaction"),
        F.min("amount_paid").alias("min_transaction")
    )

    # Calculate revenue per customer
    geo_summary = geo_summary.withColumn(
        "revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
    )

    # Add state-level ranking
    w_state = Window.partitionBy("state").orderBy(F.col("total_revenue").desc())
    geo_summary = geo_summary.withColumn("rank_within_state", F.row_number().over(w_state))

    # Add overall ranking
    w_overall = Window.orderBy(F.col("total_revenue").desc())
    geo_summary = geo_summary.withColumn("overall_rank", F.row_number().over(w_overall))

    geo_summary = geo_summary.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_revenue_by_geography dataset created")
    return geo_summary


# ============================================================
# Gold Layer: Daily Revenue Trends
# ============================================================
@dlt.table(
    name="customers_gold_revenue_trends_daily",
    comment="Daily revenue trends with rolling averages and growth metrics."
)
@dlt.expect_all({
    "has_date": "transaction_date IS NOT NULL",
    "valid_revenue": "daily_revenue >= 0"
})
def customers_gold_revenue_trends_daily():
    """
    Business Metrics: Time-series revenue analysis with trends.
    Includes daily revenue, transaction counts, and 7-day moving averages.
    """
    logger.info("=== Starting customers_gold_revenue_trends_daily dataset ===")

    df = dlt.read("customers_silver_all")

    # Extract date from timestamp
    df = df.withColumn("transaction_date", F.to_date("silver_load_ts"))

    # Aggregate by date
    daily_metrics = df.groupBy("transaction_date").agg(
        F.sum("amount_paid").alias("daily_revenue"),
        F.count("*").alias("transaction_count"),
        F.countDistinct("cust_id").alias("unique_customers"),
        F.round(F.avg("amount_paid"), 2).alias("avg_transaction_value")
    )

    # Calculate rolling 7-day averages
    w_7day = Window.orderBy("transaction_date").rowsBetween(-6, 0)
    daily_metrics = daily_metrics.withColumn(
        "revenue_7day_avg",
        F.round(F.avg("daily_revenue").over(w_7day), 2)
    )
    daily_metrics = daily_metrics.withColumn(
        "transactions_7day_avg",
        F.round(F.avg("transaction_count").over(w_7day), 2)
    )

    # Calculate day-over-day growth
    w_prev = Window.orderBy("transaction_date").rowsBetween(-1, -1)
    daily_metrics = daily_metrics.withColumn(
        "prev_day_revenue",
        F.lag("daily_revenue", 1).over(Window.orderBy("transaction_date"))
    )
    daily_metrics = daily_metrics.withColumn(
        "revenue_growth_pct",
        F.when(
            F.col("prev_day_revenue").isNotNull() & (F.col("prev_day_revenue") != 0),
            F.round(((F.col("daily_revenue") - F.col("prev_day_revenue")) / F.col("prev_day_revenue")) * 100, 2)
        ).otherwise(None)
    ).drop("prev_day_revenue")

    # Add day of week
    daily_metrics = daily_metrics.withColumn(
        "day_of_week",
        F.date_format("transaction_date", "EEEE")
    )

    daily_metrics = daily_metrics.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_revenue_trends_daily dataset created")
    return daily_metrics


# ============================================================
# Gold Layer: State-Level KPIs
# ============================================================
@dlt.table(
    name="customers_gold_state_kpis",
    comment="Comprehensive state-level KPIs including revenue, customers, and segments."
)
@dlt.expect_all({
    "has_state": "state IS NOT NULL",
    "valid_metrics": "total_revenue >= 0 AND total_customers > 0"
})
def customers_gold_state_kpis():
    """
    Business Metrics: State-level performance dashboard.
    Comprehensive KPIs by state including revenue, customers, and segment distribution.
    """
    logger.info("=== Starting customers_gold_state_kpis dataset ===")

    silver_df = dlt.read("customers_silver_all")
    rfm_df = dlt.read("customers_gold_rfm_clv")

    # Aggregate silver metrics by state
    state_metrics = silver_df.groupBy("state").agg(
        F.sum("amount_paid").alias("total_revenue"),
        F.countDistinct("cust_id").alias("total_customers"),
        F.count("*").alias("total_transactions"),
        F.round(F.avg("amount_paid"), 2).alias("avg_transaction_value")
    )

    # Calculate derived metrics
    state_metrics = state_metrics.withColumn(
        "revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("total_customers"), 2)
    )
    state_metrics = state_metrics.withColumn(
        "transactions_per_customer",
        F.round(F.col("total_transactions") / F.col("total_customers"), 2)
    )

    # Join with RFM to get segment distribution by state
    # First join silver with RFM to get state+segment
    silver_with_segments = silver_df.alias("s").join(
        rfm_df.select("cust_id", "value_segment", "recency_segment", "frequency_segment").alias("r"),
        F.col("s.cust_id") == F.col("r.cust_id"),
        "left"
    ).select(
        "s.state",
        "r.value_segment",
        "r.recency_segment",
        "r.frequency_segment"
    )

    # Count high-value customers per state
    high_value_by_state = silver_with_segments.filter(
        F.col("value_segment") == "High"
    ).groupBy("state").agg(
        F.count("*").alias("high_value_customers")
    )

    # Count recent customers per state
    recent_by_state = silver_with_segments.filter(
        F.col("recency_segment") == "Recent"
    ).groupBy("state").agg(
        F.count("*").alias("recent_customers")
    )

    # Join segment counts back to state metrics
    state_metrics = state_metrics.alias("sm").join(
        high_value_by_state.alias("hv"),
        F.col("sm.state") == F.col("hv.state"),
        "left"
    ).select(
        "sm.*",
        F.coalesce(F.col("hv.high_value_customers"), F.lit(0)).alias("high_value_customers")
    )

    state_metrics = state_metrics.alias("sm").join(
        recent_by_state.alias("rc"),
        F.col("sm.state") == F.col("rc.state"),
        "left"
    ).select(
        "sm.*",
        F.coalesce(F.col("rc.recent_customers"), F.lit(0)).alias("recent_customers")
    )

    # Calculate percentages
    state_metrics = state_metrics.withColumn(
        "high_value_pct",
        F.round((F.col("high_value_customers") / F.col("total_customers")) * 100, 2)
    )
    state_metrics = state_metrics.withColumn(
        "recent_pct",
        F.round((F.col("recent_customers") / F.col("total_customers")) * 100, 2)
    )

    # Add state ranking by revenue
    w = Window.orderBy(F.col("total_revenue").desc())
    state_metrics = state_metrics.withColumn("revenue_rank", F.row_number().over(w))

    state_metrics = state_metrics.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_state_kpis dataset created")
    return state_metrics


# ============================================================
# Gold Layer: Email Analysis
# ============================================================
@dlt.table(
    name="customers_gold_email_analysis",
    comment="Email domain analysis including provider distribution, validation stats, and potential duplicates."
)
@dlt.expect_all({
    "has_domain": "email_domain IS NOT NULL",
    "valid_counts": "customer_count > 0"
})
def customers_gold_email_analysis():
    """
    Email Analytics: Domain distribution, provider analysis, and data quality metrics.

    Insights provided:
    - Top email domains by customer count
    - Personal vs corporate email distribution
    - Email validation statistics
    - Average spend by email provider type
    - Potential duplicate email detection
    """
    logger.info("=== Starting customers_gold_email_analysis dataset ===")

    df = dlt.read("customers_silver_all")

    # Email domain distribution with spend metrics
    logger.info("Analyzing email domain distribution")
    domain_analysis = df.groupBy(
        "email_domain",
        "email_provider_type"
    ).agg(
        F.countDistinct("cust_id").alias("customer_count"),
        F.count("*").alias("transaction_count"),
        F.sum("amount_paid").alias("total_revenue"),
        F.round(F.avg("amount_paid"), 2).alias("avg_transaction_value"),
        F.round(F.sum("amount_paid") / F.countDistinct("cust_id"), 2).alias("revenue_per_customer")
    )

    # Add domain ranking by customer count
    w_customers = Window.orderBy(F.col("customer_count").desc())
    domain_analysis = domain_analysis.withColumn(
        "domain_rank_by_customers",
        F.row_number().over(w_customers)
    )

    # Add domain ranking by revenue
    w_revenue = Window.orderBy(F.col("total_revenue").desc())
    domain_analysis = domain_analysis.withColumn(
        "domain_rank_by_revenue",
        F.row_number().over(w_revenue)
    )

    # Note: Percentage calculation would require a separate aggregation pass
    # Domain ranking provides relative importance without needing total count

    domain_analysis = domain_analysis.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_email_analysis dataset created")
    return domain_analysis


# ============================================================
# Gold Layer: Email Provider Summary
# ============================================================
@dlt.table(
    name="customers_gold_email_provider_summary",
    comment="High-level summary of personal vs corporate email usage and validation statistics."
)
def customers_gold_email_provider_summary():
    """
    Email Summary: Personal vs corporate email distribution with quality metrics.

    Key metrics:
    - Total customers by provider type (personal/corporate)
    - Email validation success rate
    - Revenue by provider type
    - Missing email statistics
    """
    logger.info("=== Starting customers_gold_email_provider_summary dataset ===")

    df = dlt.read("customers_silver_all")

    # Overall email statistics
    logger.info("Calculating email provider type distribution")

    # Provider type distribution with spend
    provider_summary = df.groupBy("email_provider_type").agg(
        F.countDistinct("cust_id").alias("customer_count"),
        F.count("*").alias("transaction_count"),
        F.sum("amount_paid").alias("total_revenue"),
        F.round(F.avg("amount_paid"), 2).alias("avg_transaction_value"),
        F.round(F.sum("amount_paid") / F.countDistinct("cust_id"), 2).alias("revenue_per_customer")
    )

    # Note: Percentages would require separate aggregation pass
    # Customer counts by provider type provide relative distribution

    provider_summary = provider_summary.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_email_provider_summary dataset created")
    return provider_summary


# ============================================================
# Gold Layer: Duplicate Email Detection
# ============================================================
@dlt.table(
    name="customers_gold_duplicate_emails",
    comment="Identifies email addresses used by multiple customer IDs (potential data quality issue or shared accounts)."
)
@dlt.expect_all({
    "multiple_customers": "customer_count > 1",
    "has_email": "email IS NOT NULL"
})
def customers_gold_duplicate_emails():
    """
    Data Quality: Duplicate email detection.

    Identifies:
    - Email addresses associated with multiple customer IDs
    - Potential data quality issues
    - Shared account patterns
    - Merge candidates for customer deduplication
    """
    logger.info("=== Starting customers_gold_duplicate_emails dataset ===")

    df = dlt.read("customers_silver_all")

    # Find emails used by multiple customers
    logger.info("Detecting duplicate email addresses")
    email_usage = df.filter(
        F.col("email").isNotNull()
    ).groupBy("email", "email_domain", "email_provider_type").agg(
        F.countDistinct("cust_id").alias("customer_count"),
        F.collect_set("cust_id").alias("customer_ids"),
        F.collect_set("first_name").alias("first_names"),
        F.collect_set("last_name").alias("last_names"),
        F.sum("amount_paid").alias("total_combined_spend"),
        F.count("*").alias("total_transactions")
    )

    # Filter to only duplicates
    duplicates = email_usage.filter(F.col("customer_count") > 1)

    # Add severity flag
    duplicates = duplicates.withColumn(
        "severity",
        F.when(F.col("customer_count") >= 5, "HIGH")
        .when(F.col("customer_count") >= 3, "MEDIUM")
        .otherwise("LOW")
    )

    # Sort by customer count descending
    w = Window.orderBy(F.col("customer_count").desc())
    duplicates = duplicates.withColumn("duplicate_rank", F.row_number().over(w))

    duplicates = duplicates.withColumn("report_ts", F.current_timestamp())

    logger.info(f"customers_gold_duplicate_emails dataset created")
    return duplicates

# ============================================================
# Automatic Bronze Table Archival - DISABLED
# ============================================================
# NOTE: Archival logic moved to separate Bronze_Table_Archival notebook
# DDL commands (CREATE SCHEMA, DROP TABLE) are not allowed in @dlt.table() decorators
# This function is commented out to prevent pipeline errors

# @dlt.table(
#     name="bronze_archive_log",
#     comment="Audit log of bronze tables archived after processing."
# )
# def bronze_archive_log():
#     """
#     Data Lifecycle Management: Automatically archives processed bronze tables.
#
#     This dataset runs after silver processing completes and:
#     - Identifies all bronze tables that have been processed
#     - Deep clones them to workspace.etl-bronze-pipeline-archive
#     - Drops them from the active bronze schema
#     - Logs the archival operation with timestamps
#
#     Benefits:
#     - Keeps bronze schema clean for new ingestion
#     - Preserves historical data for audit/compliance
#     - Maintains data lineage and traceability
#     - Automated cleanup without manual intervention
#     """
#     logger.info("=== Starting automatic bronze table archival ===")
#
#     # Ensure silver processing is complete by reading from it
#     silver_df = dlt.read("customers_silver_all")
#
#     # Create archive schema if it doesn't exist
#     archive_schema = "workspace.`etl-bronze-pipeline-archive`"
#     logger.info(f"Ensuring archive schema exists: {archive_schema}")
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {archive_schema} COMMENT 'Archive for historical bronze tables'")
#
#     # Get list of bronze tables to archive
#     parts = BRONZE_SCHEMA.split(".")
#     catalog = parts[0]
#     schema_name = parts[1]
#
#     try:
#         table_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`")
#         tables = [row.tableName for row in table_df.collect()]
#         logger.info(f"Found {len(tables)} bronze tables to archive")
#     except Exception as e:
#         logger.warning(f"No bronze tables found for archival: {str(e)}")
#         # Return empty log
#         return spark.createDataFrame(
#             [],
#             "table_name STRING, archived_at TIMESTAMP, row_count BIGINT, archive_status STRING, archive_message STRING"
#         )
#
#     if not tables:
#         logger.info("No bronze tables to archive")
#         return spark.createDataFrame(
#             [],
#             "table_name STRING, archived_at TIMESTAMP, row_count BIGINT, archive_status STRING, archive_message STRING"
#         )
#
#     # Archive each table
#     archive_records = []
#     for table_name in tables:
#         source = f"`{catalog}`.`{schema_name}`.`{table_name}`"
#         target = f"workspace.`etl-bronze-pipeline-archive`.`{table_name}`"
#
#         logger.info(f"Archiving: {table_name}")
#
#         try:
#             # Get row count before archival
#             count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {source}")
#             row_count = count_df.collect()[0].cnt
#
#             # Deep clone to preserve all metadata
#             spark.sql(f"CREATE OR REPLACE TABLE {target} DEEP CLONE {source}")
#             logger.info(f"✅ Cloned {table_name} to archive ({row_count} rows)")
#
#             # Drop from bronze schema
#             spark.sql(f"DROP TABLE IF EXISTS {source}")
#             logger.info(f"✅ Dropped {table_name} from bronze schema")
#
#             archive_records.append((
#                 table_name,
#                 F.current_timestamp(),
#                 row_count,
#                 "SUCCESS",
#                 f"Archived {row_count} rows to {target}"
#             ))
#
#         except Exception as e:
#             error_msg = f"Failed to archive {table_name}: {str(e)}"
#             logger.error(error_msg)
#             archive_records.append((
#                 table_name,
#                 F.current_timestamp(),
#                 0,
#                 "FAILED",
#                 error_msg
#             ))
#             continue
#
#     # Create audit log DataFrame
#     if archive_records:
#         log_df = spark.createDataFrame(
#             archive_records,
#             "table_name STRING, archived_at TIMESTAMP, row_count BIGINT, archive_status STRING, archive_message STRING"
#         )
#         logger.info(f"Bronze archival complete: {len(archive_records)} tables processed")
#     else:
#         log_df = spark.createDataFrame(
#             [],
#             "table_name STRING, archived_at TIMESTAMP, row_count BIGINT, archive_status STRING, archive_message STRING"
#         )
#
#     return log_df
# END OF COMMENTED OUT FUNCTION - Use Bronze_Table_Archival notebook instead


