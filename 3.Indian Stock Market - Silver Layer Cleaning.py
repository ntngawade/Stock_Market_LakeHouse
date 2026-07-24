# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 5: Silver Layer Configuration
# Step 5: Silver Layer - Data Cleaning and Conforming
# Purpose: Clean, validate, and standardize Bronze data for analytics

from pyspark.sql import functions as F, Window
from pyspark.sql.types import DateType, DoubleType, LongType
from datetime import datetime, timedelta

# Unity Catalog Configuration
CATALOG_NAME = "StockMarketLakehouse"
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.stock_prices_raw"
SILVER_TABLE = f"{CATALOG_NAME}.silver.stock_prices_clean"
QUARANTINE_TABLE = f"{CATALOG_NAME}.silver.stock_prices_quarantine"

print("✓ Silver layer configuration loaded")
print(f"  Source: {BRONZE_TABLE}")
print(f"  Target: {SILVER_TABLE}")
print(f"  Quarantine: {QUARANTINE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create Silver schema and tables
# MAGIC %sql
# MAGIC -- Create the silver schema for cleaned, conformed data
# MAGIC CREATE SCHEMA IF NOT EXISTS StockMarketLakehouse.silver
# MAGIC COMMENT 'Cleaned and validated stock market data - business-ready';
# MAGIC
# MAGIC -- Verify schema creation
# MAGIC DESCRIBE SCHEMA EXTENDED StockMarketLakehouse.silver;

# COMMAND ----------

# DBTITLE 1,Step 5.1: Read from Bronze with initial filters
# Read from Bronze table
print(f"Reading data from {BRONZE_TABLE}...")

df_bronze = spark.table(BRONZE_TABLE)

print(f"✓ Loaded {df_bronze.count():,} records from Bronze")
print("\nBronze schema:")
df_bronze.printSchema()

# Show sample data
print("\nSample Bronze data:")
display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 5.2: Data Type Corrections and Standardization
# Step 5.2: Apply data type corrections and standardizations

print("Applying data transformations...\n")

df_typed = df_bronze \
    .withColumn("date", F.to_date(F.col("date"))) \
    .withColumn("open", F.col("open").cast(DoubleType())) \
    .withColumn("high", F.col("high").cast(DoubleType())) \
    .withColumn("low", F.col("low").cast(DoubleType())) \
    .withColumn("close", F.col("close").cast(DoubleType())) \
    .withColumn("volume", F.col("volume").cast(LongType()))

print("✓ Data types corrected")

# Standardize ticker naming (keep .NS suffix for traceability)
# If you want to strip it: .withColumn("ticker_clean", F.regexp_replace("ticker", "\.NS$", ""))
df_standardized = df_typed.withColumn(
    "ticker_standard", 
  #  F.upper(F.trim(F.col("ticker")))
  F.regexp_replace("ticker", "\.NS$", "")
)

print("✓ Ticker names standardized")

# Add derived columns for analytics
df_enriched = df_standardized \
    .withColumn(
        "daily_return_pct",
        F.round(((F.col("close") - F.col("open")) / F.col("open")) * 100, 4)
    ) \
    .withColumn(
        "price_range",
        F.round(F.col("high") - F.col("low"), 2)
    ) \
    .withColumn(
        "is_trading_day",
        F.when((F.dayofweek("date").isin([1, 7])), False).otherwise(True)
    )

print("✓ Derived columns added (daily_return_pct, price_range, is_trading_day)")
print(f"\nTransformed records: {df_enriched.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 6: Data Quality Validation Functions
# Step 6: Data Quality Enforcement
# Define quality checks and validation rules

def apply_quality_checks(df):
    """
    Apply comprehensive data quality checks.
    Returns (valid_df, quarantine_df)
    """
    
    # Define quality rules
    quality_rules = [
        ("no_negative_prices", 
         (F.col("open") >= 0) & (F.col("high") >= 0) & 
         (F.col("low") >= 0) & (F.col("close") >= 0)),
        
        ("high_is_highest", 
         (F.col("high") >= F.col("low")) & 
         (F.col("high") >= F.col("close")) & 
         (F.col("high") >= F.col("open"))),
        
        ("low_is_lowest",
         (F.col("low") <= F.col("high")) & 
         (F.col("low") <= F.col("close")) & 
         (F.col("low") <= F.col("open"))),
        
        ("non_negative_volume", F.col("volume") >= 0),
        
        ("no_null_critical_fields",
         F.col("ticker").isNotNull() & 
         F.col("date").isNotNull() & 
         F.col("close").isNotNull())
    ]
    
    # Apply all quality checks and create a combined validity flag
    df_with_checks = df
    for rule_name, rule_condition in quality_rules:
        df_with_checks = df_with_checks.withColumn(
            f"check_{rule_name}",
            rule_condition
        )
    
    # Create overall validity flag
    check_columns = [f"check_{rule_name}" for rule_name, _ in quality_rules]
    df_with_checks = df_with_checks.withColumn(
        "is_valid",
        F.expr(" AND ".join(check_columns))
    )
    
    # Add failure reasons for quarantined records
    failure_reasons = []
    for rule_name, _ in quality_rules:
        failure_reasons.append(
            f"CASE WHEN NOT check_{rule_name} THEN '{rule_name}' ELSE NULL END"
        )
    
    df_with_checks = df_with_checks.withColumn(
        "failure_reasons",
        F.array_remove(
            F.array(*[F.expr(reason) for reason in failure_reasons]),
            None
        )
    )
    
    # Split into valid and quarantine
    df_valid = df_with_checks.filter(F.col("is_valid") == True).drop(
        *check_columns, "is_valid", "failure_reasons"
    )
    
    df_quarantine = df_with_checks.filter(F.col("is_valid") == False).withColumn(
        "quarantine_timestamp", F.current_timestamp()
    )
    
    return df_valid, df_quarantine

print("✓ Data quality validation functions defined")

# COMMAND ----------

# DBTITLE 1,Step 6: Apply Quality Checks and Quarantine
# Apply quality checks to enriched data
print("Running data quality validation...\n")

df_valid, df_quarantine = apply_quality_checks(df_enriched)

valid_count = df_valid.count()
quarantine_count = df_quarantine.count()
total_count = df_enriched.count()

print(f"✓ Quality validation complete:")
print(f"  Valid records:       {valid_count:,} ({valid_count/total_count*100:.2f}%)")
print(f"  Quarantined records: {quarantine_count:,} ({quarantine_count/total_count*100:.2f}%)")
print(f"  Total records:       {total_count:,}")

if quarantine_count > 0:
    print("\n⚠️  Warning: Some records failed quality checks")
    print("\nSample quarantined records:")
    display(df_quarantine.select(
        "ticker", "date", "open", "high", "low", "close", "volume", "failure_reasons"
    ).limit(10))

# COMMAND ----------

# DBTITLE 1,Step 5.3: Deduplication on ticker + date
# Step 5.3: Deduplicate on ticker + date
# Keep the most recent ingestion_timestamp for duplicates

print("Checking for duplicates...\n")

# Count duplicates before deduplication
duplicates_before = df_valid.groupBy("ticker", "date").count().filter("count > 1")
dup_count = duplicates_before.count()

if dup_count > 0:
    print(f"⚠️  Found {dup_count} duplicate (ticker, date) pairs")
    print("\nSample duplicates:")
    display(duplicates_before.limit(10))
else:
    print("✓ No duplicates found")

# Deduplicate by keeping the record with the latest ingestion_timestamp
window_spec = Window.partitionBy("ticker", "date").orderBy(F.col("ingestion_timestamp").desc())

df_deduplicated = df_valid \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"\n✓ Deduplication complete")
print(f"  Records after deduplication: {df_deduplicated.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 5.4: Handle Missing Trading Days (NSE Holidays)
# Step 5.4: Identify missing trading days
# Note: This identifies gaps but doesn't fill them (forward-fill would be in Gold layer)

print("Analyzing missing trading days...\n")

# For each ticker, check for gaps in trading days
window_by_ticker = Window.partitionBy("ticker").orderBy("date")

df_with_gaps = df_deduplicated \
    .withColumn("prev_date", F.lag("date", 1).over(window_by_ticker)) \
    .withColumn(
        "days_gap",
        F.datediff("date", "prev_date")
    ) \
    .withColumn(
        "is_gap",
        (F.col("days_gap") > 3) & (F.col("prev_date").isNotNull())
    )

# Identify significant gaps (more than 3 days, accounting for weekends)
gaps = df_with_gaps.filter(F.col("is_gap") == True)
gap_count = gaps.count()

if gap_count > 0:
    print(f"⚠️  Found {gap_count} significant trading gaps (>3 days)")
    print("\nSample gaps (may indicate NSE holidays, stock suspensions, or data issues):")
    display(gaps.select("ticker", "date", "prev_date", "days_gap").limit(10))
else:
    print("✓ No significant trading gaps detected")

# Drop temporary columns
df_final = df_with_gaps.drop("prev_date", "days_gap", "is_gap")

print(f"\n✓ Final Silver data prepared")
print(f"  Total records: {df_final.count():,}")

# COMMAND ----------

# DBTITLE 1,Step 5.5: Write to Silver with MERGE INTO (Idempotent Upsert)
# Step 5.5: Use MERGE INTO for idempotent upserts
# This is a key DE skill - handles incremental updates gracefully

from delta.tables import DeltaTable

print(f"Writing to Silver table: {SILVER_TABLE}\n")

# Register the cleaned data as a temp view for MERGE
df_final.createOrReplaceTempView("silver_updates")

# Check if Silver table exists
table_exists = spark.catalog.tableExists(SILVER_TABLE)

if not table_exists:
    print("Creating Silver table for the first time...")
    # First-time creation
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingestion_date", "ticker") \
        .saveAsTable(SILVER_TABLE)
    print(f"✓ Created {SILVER_TABLE}")
else:
    print("Silver table exists. Performing MERGE INTO...")
    
    # MERGE INTO pattern for incremental upserts
    delta_table = DeltaTable.forName(spark, SILVER_TABLE)
    
    delta_table.alias("target").merge(
        df_final.alias("source"),
        "target.ticker = source.ticker AND target.date = source.date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"✓ MERGE INTO complete")

print(f"\n✓ Silver table updated: {SILVER_TABLE}")

# Show final record count
final_count = spark.table(SILVER_TABLE).count()
print(f"  Total records in Silver: {final_count:,}")

# COMMAND ----------

# DBTITLE 1,Write Quarantined Records to Separate Table
# Write quarantined records to a separate table for review
if quarantine_count > 0:
    print(f"\nWriting {quarantine_count:,} quarantined records to {QUARANTINE_TABLE}...")
    
    df_quarantine.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(QUARANTINE_TABLE)
    
    print(f"✓ Quarantined records logged to {QUARANTINE_TABLE}")
    print("\n⚠️  Action required: Review quarantined records and fix data quality issues")
else:
    print("\n✓ No records quarantined - all data passed quality checks")

# COMMAND ----------

# DBTITLE 1,Verify Silver Table
# MAGIC %sql
# MAGIC -- Verify Silver table structure and data
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     COUNT(*) as record_count,
# MAGIC     MIN(date) as earliest_date,
# MAGIC     MAX(date) as latest_date,
# MAGIC     AVG(close) as avg_close_price,
# MAGIC     AVG(volume) as avg_volume
# MAGIC FROM StockMarketLakehouse.silver.stock_prices_clean
# MAGIC GROUP BY ticker
# MAGIC ORDER BY ticker
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Data Quality Summary Report
# MAGIC %sql
# MAGIC -- Data quality summary: Compare Bronze vs Silver record counts
# MAGIC WITH bronze_counts AS (
# MAGIC     SELECT 
# MAGIC         'Bronze' as layer,
# MAGIC         COUNT(*) as total_records,
# MAGIC         COUNT(DISTINCT ticker) as unique_tickers,
# MAGIC         COUNT(DISTINCT date) as unique_dates
# MAGIC     FROM StockMarketLakehouse.bronze.stock_prices_raw
# MAGIC ),
# MAGIC silver_counts AS (
# MAGIC     SELECT 
# MAGIC         'Silver' as layer,
# MAGIC         COUNT(*) as total_records,
# MAGIC         COUNT(DISTINCT ticker) as unique_tickers,
# MAGIC         COUNT(DISTINCT date) as unique_dates
# MAGIC     FROM StockMarketLakehouse.silver.stock_prices_clean
# MAGIC )
# MAGIC SELECT * FROM bronze_counts
# MAGIC UNION ALL
# MAGIC SELECT * FROM silver_counts;

# COMMAND ----------

# DBTITLE 1,Sample Silver Data with Quality Metrics
# MAGIC %sql
# MAGIC -- Sample cleaned Silver data with derived columns
# MAGIC SELECT 
# MAGIC     ticker_standard,
# MAGIC     date,
# MAGIC     open,
# MAGIC     high,
# MAGIC     low,
# MAGIC     close,
# MAGIC     volume,
# MAGIC     daily_return_pct,
# MAGIC     price_range,
# MAGIC     is_trading_day
# MAGIC FROM StockMarketLakehouse.silver.stock_prices_clean
# MAGIC WHERE ticker_standard = 'RELIANCE'
# MAGIC     AND date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Silver Layer Summary
# MAGIC %md
# MAGIC ## ✓ Steps 5 & 6 Complete: Silver Layer Implementation
# MAGIC
# MAGIC ### What We Implemented:
# MAGIC
# MAGIC **Step 5: Data Cleaning & Conforming**
# MAGIC * ✓ Data type corrections (dates, doubles, longs)
# MAGIC * ✓ Ticker standardization (uppercase, trimmed)
# MAGIC * ✓ Deduplication on (ticker, date) - kept latest ingestion
# MAGIC * ✓ Missing trading day detection (NSE holidays/gaps)
# MAGIC * ✓ Derived analytics columns (daily_return_pct, price_range)
# MAGIC * ✓ **MERGE INTO pattern** for idempotent upserts
# MAGIC
# MAGIC **Step 6: Data Quality Enforcement**
# MAGIC * ✓ No negative prices validation
# MAGIC * ✓ Price relationship checks (high >= low, high >= close, high >= open)
# MAGIC * ✓ Non-negative volume validation
# MAGIC * ✓ Null critical fields check
# MAGIC * ✓ **Quarantine table** for failed records (not silently dropped)
# MAGIC * ✓ Failure reason tracking
# MAGIC
# MAGIC ### Key DE Skills Demonstrated:
# MAGIC
# MAGIC | Skill | Implementation |
# MAGIC |-------|---------------|
# MAGIC | **Idempotent Upserts** | MERGE INTO (whenMatchedUpdateAll / whenNotMatchedInsertAll) |
# MAGIC | **Data Quality** | Multi-rule validation framework with quarantine |
# MAGIC | **Deduplication** | Window functions with row_number() |
# MAGIC | **Schema Evolution** | Delta table with partitioning |
# MAGIC | **Audit Trail** | Quarantine table logs all failures with reasons |
# MAGIC | **Gap Analysis** | Detect missing trading days using lag() |
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC * "Implemented MERGE INTO for incremental, idempotent processing - can re-run without duplicates"
# MAGIC * "Built a quality framework that quarantines bad records instead of silently dropping them"
# MAGIC * "Used Window functions for deduplication and gap detection"
# MAGIC * "Partitioned by ingestion_date and ticker for query performance"
# MAGIC * "Created separate quarantine table for data quality triage and monitoring"
# MAGIC
# MAGIC ### Silver Table Schema:
# MAGIC ```
# MAGIC StockMarketLakehouse.silver.stock_prices_clean
# MAGIC ├── ticker_standard (string, partition)
# MAGIC ├── date (date)
# MAGIC ├── open, high, low, close (double)
# MAGIC ├── volume (long)
# MAGIC ├── daily_return_pct (double) -- derived
# MAGIC ├── price_range (double) -- derived
# MAGIC ├── is_trading_day (boolean) -- derived
# MAGIC └── ingestion_date (date, partition)
# MAGIC ```
# MAGIC
# MAGIC ### Next Steps:
# MAGIC **Gold Layer** (Step 7) - Business-level aggregations and analytics

# COMMAND ----------

