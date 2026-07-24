# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 2: Define Scope - Configuration
# Step 2: Define Scope - Nifty 50 stocks configuration

from datetime import datetime, timedelta
import pandas as pd

# Unity Catalog Configuration
CATALOG_NAME = "StockMarketLakehouse"

# Load Nifty 50 tickers dynamically from configuration table
# This replaces hardcoded ticker list with table-driven configuration
try:
    tickers_df = spark.table(f"{CATALOG_NAME}.config.nifty50_tickers") \
        .filter("is_active = true") \
        .select("ticker_symbol") \
        .orderBy("ticker_id")
    
    NIFTY_50_TICKERS = [row.ticker_symbol for row in tickers_df.collect()]
    print(f"✓ Loaded {len(NIFTY_50_TICKERS)} active tickers from config table")
except Exception as e:
    print(f"⚠️  Could not load tickers from config table: {str(e)}")
    print("⚠️  Using fallback: First run 'config_nifty50_tickers.sql' to create the config table")
    # Fallback to hardcoded list if config table doesn't exist yet
    NIFTY_50_TICKERS = [
        'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
        'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
        'LT.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS', 'HCLTECH.NS',
        'SUNPHARMA.NS', 'TITAN.NS', 'BAJFINANCE.NS', 'ULTRACEMCO.NS', 'NESTLEIND.NS',
        'WIPRO.NS', 'ONGC.NS', 'NTPC.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
        'POWERGRID.NS', 'M&M.NS', 'TECHM.NS', 'ADANIPORTS.NS', 'BAJAJFINSV.NS',
        'COALINDIA.NS', 'DRREDDY.NS', 'INDUSINDBK.NS', 'CIPLA.NS', 'GRASIM.NS',
        'EICHERMOT.NS', 'JSWSTEEL.NS', 'BRITANNIA.NS', 'DIVISLAB.NS', 'HINDALCO.NS',
        'HEROMOTOCO.NS', 'SHREECEM.NS', 'UPL.NS', 'APOLLOHOSP.NS', 'TATACONSUM.NS',
        'SBILIFE.NS', 'ADANIENT.NS', 'BAJAJ-AUTO.NS', 'HDFCLIFE.NS', 'BPCL.NS'
    ]

# Date range: Last 5 years
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=5*365)

# Target schemas for each layer
BRONZE_SCHEMA_DOC = f"""
Bronze Layer Schema ({CATALOG_NAME}.bronze.stock_prices_raw):
- ticker: string (stock symbol)
- date: timestamp (trading date)
- open: double (opening price)
- high: double (high price)
- low: double (low price)
- close: double (closing price)
- volume: long (trading volume)
- ingestion_timestamp: timestamp (when data was ingested)
- source: string (data source identifier)
- ingestion_date: date (partition key)
"""

SILVER_SCHEMA_DOC = f"""
Silver Layer Schema ({CATALOG_NAME}.silver.stock_prices_clean):
- Same as bronze but with:
  - Null handling applied
  - Data type validation
  - Duplicate removal
  - Business date filtering (exclude weekends/holidays)
  - Additional derived fields (daily_return, etc.)
"""

GOLD_SCHEMA_DOC = f"""
Gold Layer Schema ({CATALOG_NAME}.gold.stock_market_analytics):
- Aggregated metrics by ticker and time period
- Moving averages (50-day, 200-day)
- Volatility metrics
- Trading volume patterns
- Sector-level aggregations
"""

print("✓ Configuration loaded")
print(f"✓ Tracking {len(NIFTY_50_TICKERS)} Nifty 50 stocks")
print(f"✓ Date range: {START_DATE.date()} to {END_DATE.date()}")
print("\n" + BRONZE_SCHEMA_DOC)

# COMMAND ----------

# DBTITLE 1,Step 3: Bronze Layer - Raw Ingestion Function
# MAGIC %pip install yfinance
# MAGIC
# MAGIC import yfinance as yf
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC def fetch_stock_data_raw(ticker, start_date, end_date):
# MAGIC     try:
# MAGIC         stock = yf.Ticker(ticker)
# MAGIC         df = stock.history(start=start_date, end=end_date)
# MAGIC         if df.empty:
# MAGIC             print(f"⚠️  No data returned for {ticker}")
# MAGIC             return None
# MAGIC         df = df.reset_index()
# MAGIC         df['ticker'] = ticker
# MAGIC         df['ingestion_timestamp'] = pd.Timestamp.now()
# MAGIC         df['source'] = 'yfinance_api'
# MAGIC         df['ingestion_date'] = pd.Timestamp.now().date()
# MAGIC         df = df.rename(columns={
# MAGIC             'Date': 'date',
# MAGIC             'Open': 'open',
# MAGIC             'High': 'high',
# MAGIC             'Low': 'low',
# MAGIC             'Close': 'close',
# MAGIC             'Volume': 'volume'
# MAGIC         })
# MAGIC         columns_to_keep = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 
# MAGIC                           'ingestion_timestamp', 'source', 'ingestion_date']
# MAGIC         df = df[[col for col in columns_to_keep if col in df.columns]]
# MAGIC         return df
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error fetching data for {ticker}: {str(e)}")
# MAGIC         return None
# MAGIC
# MAGIC def ingest_to_bronze(tickers, start_date, end_date, bronze_table=f"{CATALOG_NAME}.bronze.stock_prices_raw"):
# MAGIC     all_data = []
# MAGIC     print(f"Starting ingestion for {len(tickers)} tickers...\n")
# MAGIC     for i, ticker in enumerate(tickers, 1):
# MAGIC         print(f"[{i}/{len(tickers)}] Fetching {ticker}...", end=" ")
# MAGIC         df = fetch_stock_data_raw(ticker, start_date, end_date)
# MAGIC         if df is not None:
# MAGIC             all_data.append(df)
# MAGIC             print(f"✓ Got {len(df)} rows")
# MAGIC         else:
# MAGIC             print("✗ Skipped")
# MAGIC     if not all_data:
# MAGIC         print("\n❌ No data fetched. Aborting.")
# MAGIC         return
# MAGIC     combined_df = pd.concat(all_data, ignore_index=True)
# MAGIC     spark_df = spark.createDataFrame(combined_df)
# MAGIC     print(f"\nWriting {len(combined_df)} total rows to {bronze_table}...")
# MAGIC     spark_df.write \
# MAGIC         .format("delta") \
# MAGIC         .mode("append") \
# MAGIC         .partitionBy("ingestion_date", "ticker") \
# MAGIC         .saveAsTable(bronze_table)
# MAGIC     print(f"✓ Successfully ingested data to {bronze_table}")
# MAGIC     return spark_df
# MAGIC
# MAGIC print("✓ Bronze layer ingestion functions defined")

# COMMAND ----------

# DBTITLE 1,Create Bronze schema (catalog.schema)
# MAGIC %sql
# MAGIC -- Create the catalog if it doesn't exist (Unity Catalog)
# MAGIC CREATE CATALOG IF NOT EXISTS StockMarketLakehouse
# MAGIC COMMENT 'Indian Stock Market Lakehouse - Production data engineering project';
# MAGIC
# MAGIC -- Create the bronze schema within the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS StockMarketLakehouse.bronze
# MAGIC COMMENT 'Raw, unprocessed stock market data from external sources';
# MAGIC
# MAGIC DESCRIBE SCHEMA EXTENDED StockMarketLakehouse.bronze;

# COMMAND ----------

# DBTITLE 1,Verify catalog and schema structure
# MAGIC %sql
# MAGIC -- Show all catalogs and verify StockMarketLakehouse exists
# MAGIC SHOW CATALOGS;
# MAGIC
# MAGIC -- Show all schemas in the catalog
# MAGIC SHOW SCHEMAS IN StockMarketLakehouse;

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Volumes for data storage
# MAGIC %sql
# MAGIC -- Create volumes for landing zone and checkpoints
# MAGIC -- Volumes provide managed storage for files in Unity Catalog
# MAGIC
# MAGIC -- Volume for landing zone (raw file drops)
# MAGIC CREATE VOLUME IF NOT EXISTS StockMarketLakehouse.bronze.landing_zone
# MAGIC COMMENT 'Landing zone for raw stock data files before ingestion';
# MAGIC
# MAGIC -- Volume for Auto Loader checkpoints
# MAGIC CREATE VOLUME IF NOT EXISTS StockMarketLakehouse.bronze.checkpoints
# MAGIC COMMENT 'Checkpoint storage for streaming ingestion jobs';
# MAGIC
# MAGIC -- Verify volumes were created
# MAGIC SHOW VOLUMES IN StockMarketLakehouse.bronze;

# COMMAND ----------

# DBTITLE 1,Run initial Bronze ingestion (one-time historical load)
# Run the initial ingestion for all Nifty 50 stocks
# This simulates the first historical load

df_bronze = ingest_to_bronze(
    tickers=NIFTY_50_TICKERS,
    start_date=START_DATE,
    end_date=END_DATE,
    bronze_table=f"{CATALOG_NAME}.bronze.stock_prices_raw"
)

if df_bronze:
    display(df_bronze.limit(10))

# COMMAND ----------

# DBTITLE 1,Verify Bronze table
# MAGIC %sql
# MAGIC -- Verify the bronze table was created and check record counts
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     COUNT(*) as record_count,
# MAGIC     MIN(date) as earliest_date,
# MAGIC     MAX(date) as latest_date
# MAGIC FROM StockMarketLakehouse.bronze.stock_prices_raw
# MAGIC GROUP BY ticker
# MAGIC ORDER BY ticker;

# COMMAND ----------

# DBTITLE 1,Step 4: Simulate Incremental Ingestion - Daily Batch Files
# Step 4: Simulate incremental ingestion by creating daily batch files
# This simulates "daily drops" - each day's data as a separate file

from datetime import timedelta

# Define a landing zone path using Unity Catalog Volume (replaces DBFS /tmp)
LANDING_ZONE_PATH = "/Volumes/StockMarketLakehouse/bronze/landing_zone"

def create_daily_batch_files(tickers, start_date, end_date, output_path):
    """
    Simulate daily data drops by creating separate files for each day.
    In production, this would be an external system dropping files daily.
    """
    # Create the landing zone directory
    dbutils.fs.mkdirs(output_path)
    
    print(f"Creating daily batch files in {output_path}...\n")
    
    # Generate files for each day (we'll do last 30 days as a demo)
    demo_start = end_date - timedelta(days=30)
    current_date = demo_start
    file_count = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Fetch data for this specific day for all tickers
        day_data = []
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                # Fetch just this one day
                df = stock.history(start=current_date, end=current_date + timedelta(days=1))
                
                if not df.empty:
                    df = df.reset_index()
                    df['ticker'] = ticker
                    df['ingestion_timestamp'] = pd.Timestamp.now()
                    df['source'] = 'yfinance_api'
                    df['ingestion_date'] = current_date.date()
                    
                    df = df.rename(columns={
                        'Date': 'date',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
                    
                    columns_to_keep = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 
                                      'ingestion_timestamp', 'source', 'ingestion_date']
                    df = df[[col for col in columns_to_keep if col in df.columns]]
                    day_data.append(df)
            except Exception as e:
                print(f"❌ Error fetching {ticker} for {date_str}: {str(e)}")
        
        if day_data:
            # Combine data for this day
            combined_day_df = pd.concat(day_data, ignore_index=True)
            
            # Write to a JSON file (common format for streaming ingestion)
            file_path = f"{output_path}/stock_data_{date_str}.json"
            
            # Convert to Spark DF and write
            spark_day_df = spark.createDataFrame(combined_day_df)
            spark_day_df.coalesce(1).write.mode("overwrite").json(file_path)
            
            file_count += 1
            print(f"✓ Created batch file for {date_str} ({len(combined_day_df)} records)")
        
        current_date += timedelta(days=1)
    
    print(f"\n✓ Created {file_count} daily batch files in {output_path}")
    return file_count

# Create demo batch files (last 30 days)
print("Simulating daily data drops (last 30 days)...\n")
file_count = create_daily_batch_files(
    tickers=NIFTY_50_TICKERS[:5],  # Use first 5 tickers for demo
    start_date=START_DATE,
    end_date=END_DATE,
    output_path=LANDING_ZONE_PATH
)

print(f"\n✓ Landing zone ready with {file_count} files")

# COMMAND ----------

# DBTITLE 1,List generated batch files
# List the files in the landing zone
files = dbutils.fs.ls(LANDING_ZONE_PATH)
print(f"Files in landing zone ({len(files)} files):")
for f in files[:10]:  # Show first 10
    print(f"  {f.name} - {f.size} bytes")

if len(files) > 10:
    print(f"  ... and {len(files) - 10} more files")

# COMMAND ----------

# DBTITLE 1,Step 4: Auto Loader - Incremental Streaming Ingestion
# Step 4: Use Auto Loader to incrementally ingest files from landing zone
# Auto Loader automatically handles:
# - Schema inference
# - Schema evolution  
# - Checkpointing (exactly-once processing)
# - New file detection

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, LongType, TimestampType

# Define checkpoint location using Unity Catalog Volume (replaces DBFS /tmp)
CHECKPOINT_PATH = "/Volumes/StockMarketLakehouse/bronze/checkpoints/stock_data"
BRONZE_STREAM_TABLE = f"{CATALOG_NAME}.bronze.stock_prices_streaming"

# Define schema to ensure consistency
stock_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

print("Setting up Auto Loader stream...")
print(f"Source: {LANDING_ZONE_PATH}")
print(f"Target: {BRONZE_STREAM_TABLE}")
print(f"Checkpoint: {CHECKPOINT_PATH}")

# Create streaming read with Auto Loader
df_stream = (spark.readStream
    .format("cloudFiles")  # Auto Loader format
    .option("cloudFiles.format", "json")  # Source file format
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)  # Where to store inferred schema
    .option("cloudFiles.schemaHints", "date TIMESTAMP, ingestion_date DATE")  # Schema hints
    .option("cloudFiles.inferColumnTypes", "true")  # Infer types automatically
    .schema(stock_schema)  # Provide explicit schema (best practice)
    .load(LANDING_ZONE_PATH)
)

# Add processing timestamp
df_stream_processed = df_stream.withColumn("processing_timestamp", F.current_timestamp())

print("\n✓ Auto Loader stream configured")
print("\nStream schema:")
df_stream_processed.printSchema()

# COMMAND ----------

# DBTITLE 1,Write stream to Bronze Delta table
# Write the stream to a Delta table
# This will continuously process new files as they arrive

print(f"Starting streaming write to {BRONZE_STREAM_TABLE}...\n")

query = (df_stream_processed.writeStream
    .format("delta")
    .outputMode("append")  # Append new records
    .option("checkpointLocation", CHECKPOINT_PATH)  # Track progress
    .option("mergeSchema", "true")  # Handle schema evolution
    .partitionBy("ingestion_date", "ticker")  # Partition strategy
    .trigger(availableNow=True)  # Process all available files then stop (for demo)
    .table(BRONZE_STREAM_TABLE)
)

# Wait for the stream to process all files
query.awaitTermination()

print(f"\n✓ Stream processing complete")
print(f"✓ Data written to {BRONZE_STREAM_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify Auto Loader ingestion results
# MAGIC %sql
# MAGIC -- Verify the streaming table and compare with batch table
# MAGIC SELECT 
# MAGIC     'Streaming Table' as source,
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(DISTINCT ticker) as unique_tickers,
# MAGIC     MIN(date) as earliest_date,
# MAGIC     MAX(date) as latest_date
# MAGIC FROM StockMarketLakehouse.bronze.stock_prices_streaming
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Batch Table' as source,
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(DISTINCT ticker) as unique_tickers,
# MAGIC     MIN(date) as earliest_date,
# MAGIC     MAX(date) as latest_date
# MAGIC FROM StockMarketLakehouse.bronze.stock_prices_raw;

# COMMAND ----------

# DBTITLE 1,Key Auto Loader Benefits Demonstrated
# MAGIC %md
# MAGIC ## ✓ Step 4 Complete: Auto Loader Implementation
# MAGIC
# MAGIC ### What We Demonstrated:
# MAGIC
# MAGIC **1. Incremental Processing**
# MAGIC - Created daily batch files simulating real-world data drops
# MAGIC - Auto Loader processes only new files, not re-reading old ones
# MAGIC - Checkpoint tracks which files have been processed
# MAGIC
# MAGIC **2. Schema Management**
# MAGIC - Schema inference: Auto Loader automatically detects column types
# MAGIC - Schema evolution: `mergeSchema=true` handles new columns gracefully
# MAGIC - Schema hints: Provided hints for date/timestamp columns
# MAGIC
# MAGIC **3. Exactly-Once Semantics**
# MAGIC - Checkpoint ensures each file is processed exactly once
# MAGIC - No duplicate data even if stream is restarted
# MAGIC - Production-ready reliability
# MAGIC
# MAGIC **4. Auto Loader vs. Batch Load**
# MAGIC | Aspect | Batch Load (Step 3) | Auto Loader (Step 4) |
# MAGIC |--------|---------------------|----------------------|
# MAGIC | **Processing** | One-time historical | Continuous/incremental |
# MAGIC | **File tracking** | Manual | Automatic |
# MAGIC | **Schema evolution** | Manual handling | Automatic |
# MAGIC | **Idempotency** | Requires custom logic | Built-in |
# MAGIC | **Use case** | Initial backfill | Production streaming |
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC - "Auto Loader automatically discovers and processes new files with exactly-once guarantees"
# MAGIC - "Used cloudFiles format with JSON schema inference and explicit schema hints"
# MAGIC - "Implemented checkpoint-based incremental processing with schema evolution support"
# MAGIC - "Partitioned by ingestion_date and ticker for efficient querying"
# MAGIC
# MAGIC ### Next Steps (Silver/Gold):
# MAGIC - **Silver layer**: Data quality checks, deduplication, type validation
# MAGIC - **Gold layer**: Business metrics, aggregations, analytics-ready datasets

# COMMAND ----------

