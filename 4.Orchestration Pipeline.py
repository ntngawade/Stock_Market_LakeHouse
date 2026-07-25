# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 8: Orchestration Configuration
# Step 8: Orchestration - End-to-End Pipeline
# Purpose: Coordinate Bronze → Silver → Gold data flow

from datetime import datetime, timedelta
import sys

# Unity Catalog Configuration
CATALOG_NAME = "StockMarketLakehouse"

# Pipeline parameters (can be overridden via Job parameters)
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("layer", "all", ["all", "bronze", "silver", "gold"], "Layer to Run")

START_DATE = dbutils.widgets.get("start_date") or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
END_DATE = dbutils.widgets.get("end_date") or datetime.now().strftime('%Y-%m-%d')
LAYER = dbutils.widgets.get("layer")

print("="*70)
print(f"🚀 INDIAN STOCK MARKET LAKEHOUSE PIPELINE")
print("="*70)
print(f"Start Date: {START_DATE}")
print(f"End Date: {END_DATE}")
print(f"Layer: {LAYER}")
print(f"Execution Time: {datetime.now()}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Helper: Pipeline Status Tracking
# Pipeline status tracking
import json
from datetime import datetime

class PipelineStatus:
    def __init__(self):
        self.start_time = datetime.now()
        self.status = {
            "pipeline_name": "Indian Stock Market Lakehouse",
            "run_id": datetime.now().strftime('%Y%m%d_%H%M%S'),
            "start_time": self.start_time.isoformat(),
            "layers": {}
        }
    
    def start_layer(self, layer_name):
        self.status["layers"][layer_name] = {
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "records_processed": 0,
            "errors": []
        }
        print(f"\n{'='*70}")
        print(f"▶️  Starting {layer_name.upper()} layer...")
        print(f"{'='*70}")
    
    def complete_layer(self, layer_name, records_processed=0, metrics=None):
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.status["layers"][layer_name]["start_time"])
        duration = (end_time - start_time).total_seconds()
        
        self.status["layers"][layer_name].update({
            "status": "completed",
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "records_processed": records_processed,
            "metrics": metrics or {}
        })
        print(f"\n✅ {layer_name.upper()} layer completed in {duration:.2f}s")
        print(f"   Records processed: {records_processed:,}")
        if metrics:
            for k, v in metrics.items():
                print(f"   {k}: {v}")
    
    def fail_layer(self, layer_name, error_message):
        self.status["layers"][layer_name].update({
            "status": "failed",
            "end_time": datetime.now().isoformat(),
            "errors": [error_message]
        })
        print(f"\n❌ {layer_name.upper()} layer failed: {error_message}")
    
    def get_summary(self):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self.status["end_time"] = end_time.isoformat()
        self.status["total_duration_seconds"] = duration
        
        completed = sum(1 for l in self.status["layers"].values() if l["status"] == "completed")
        failed = sum(1 for l in self.status["layers"].values() if l["status"] == "failed")
        
        print(f"\n{'='*70}")
        print(f"📊 PIPELINE SUMMARY")
        print(f"{'='*70}")
        print(f"Total Duration: {duration:.2f}s")
        print(f"Layers Completed: {completed}")
        print(f"Layers Failed: {failed}")
        print(f"Overall Status: {'✅ SUCCESS' if failed == 0 else '❌ FAILED'}")
        print(f"{'='*70}")
        
        return self.status

status = PipelineStatus()

# COMMAND ----------

# DBTITLE 1,Task 1: Bronze Layer - Data Ingestion
# Task 1: Bronze Layer Ingestion
# Fetches data from yfinance API and lands in Bronze table

def run_bronze_layer(start_date, end_date):
    """
    Execute Bronze layer ingestion.
    Returns: record count
    """
    status.start_layer("bronze")
    
    try:
        # Install yfinance package
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "--quiet"])
        
        import yfinance as yf
        from pyspark.sql import functions as F
        import pandas as pd
        from datetime import datetime
        
        # Load Nifty 50 tickers dynamically from config table
        tickers_df = spark.table(f"{CATALOG_NAME}.config.nifty50_tickers") \
            .filter("is_active = true") \
            .select("ticker_symbol") \
            .orderBy("ticker_id")
        
        NIFTY_50_TICKERS = [row.ticker_symbol for row in tickers_df.collect()]
        print(f"Loaded {len(NIFTY_50_TICKERS)} active tickers from config table")
        
        BRONZE_TABLE = f"{CATALOG_NAME}.bronze.stock_prices_raw"
        
        print(f"Fetching data for {len(NIFTY_50_TICKERS)} tickers from {start_date} to {end_date}...")
        
        all_data = []
        success_count = 0
        
        for ticker in NIFTY_50_TICKERS:
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(start=start_date, end=end_date)
                
                if not df.empty:
                    df = df.reset_index()
                    df['ticker'] = ticker
                    df['ingestion_timestamp'] = pd.Timestamp.now()
                    df['source'] = 'yfinance_api'
                    df['ingestion_date'] = pd.Timestamp.now().date()
                    
                    df = df.rename(columns={
                        'Date': 'date',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
                    
                    columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume',
                              'ingestion_timestamp', 'source', 'ingestion_date']
                    df = df[[col for col in columns if col in df.columns]]
                    
                    all_data.append(df)
                    success_count += 1
            except Exception as e:
                print(f"  ⚠️  Error fetching {ticker}: {str(e)}")
        
        if not all_data:
            raise Exception("No data fetched from API")
        
        # Combine and write to Bronze
        combined_df = pd.concat(all_data, ignore_index=True)
        spark_df = spark.createDataFrame(combined_df)
        
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingestion_date", "ticker") \
            .saveAsTable(BRONZE_TABLE)
        
        record_count = len(combined_df)
        
        status.complete_layer(
            "bronze",
            records_processed=record_count,
            metrics={
                "tickers_fetched": success_count,
                "tickers_total": len(NIFTY_50_TICKERS),
                "table": BRONZE_TABLE
            }
        )
        
        return record_count
        
    except Exception as e:
        status.fail_layer("bronze", str(e))
        raise

if LAYER in ["all", "bronze"]:
    bronze_records = run_bronze_layer(START_DATE, END_DATE)

# COMMAND ----------

# DBTITLE 1,Task 2: Silver Layer - Data Cleaning
# Task 2: Silver Layer Cleaning and Validation
# Applies quality checks, deduplication, and conformance

def run_silver_layer():
    """
    Execute Silver layer cleaning.
    Returns: (valid_count, quarantine_count)
    """
    status.start_layer("silver")
    
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, LongType
        from delta.tables import DeltaTable
        
        BRONZE_TABLE = f"{CATALOG_NAME}.bronze.stock_prices_raw"
        SILVER_TABLE = f"{CATALOG_NAME}.silver.stock_prices_clean"
        QUARANTINE_TABLE = f"{CATALOG_NAME}.silver.stock_prices_quarantine"
        
        print(f"Reading from {BRONZE_TABLE}...")
        df_bronze = spark.table(BRONZE_TABLE)
        
        # Type corrections and standardization
        df_typed = df_bronze \
            .withColumn("date", F.to_date(F.col("date"))) \
            .withColumn("open", F.col("open").cast(DoubleType())) \
            .withColumn("high", F.col("high").cast(DoubleType())) \
            .withColumn("low", F.col("low").cast(DoubleType())) \
            .withColumn("close", F.col("close").cast(DoubleType())) \
            .withColumn("volume", F.col("volume").cast(LongType())) \
            .withColumn("ticker_standard", F.regexp_replace(F.upper(F.trim(F.col("ticker"))), "\\.NS$", "")) \
            .withColumn("daily_return_pct", F.round(((F.col("close") - F.col("open")) / F.col("open")) * 100, 4)) \
            .withColumn("price_range", F.round(F.col("high") - F.col("low"), 2)) \
            .withColumn("is_trading_day", F.when(F.dayofweek("date").isin([1, 7]), False).otherwise(True))
        
        # Quality checks
        quality_checks = [
            ("no_negative_prices", (F.col("open") >= 0) & (F.col("high") >= 0) & (F.col("low") >= 0) & (F.col("close") >= 0)),
            ("high_is_highest", (F.col("high") >= F.col("low")) & (F.col("high") >= F.col("close")) & (F.col("high") >= F.col("open"))),
            ("low_is_lowest", (F.col("low") <= F.col("high")) & (F.col("low") <= F.col("close")) & (F.col("low") <= F.col("open"))),
            ("non_negative_volume", F.col("volume") >= 0),
            ("no_null_critical_fields", F.col("ticker").isNotNull() & F.col("date").isNotNull() & F.col("close").isNotNull())
        ]
        
        df_with_checks = df_typed
        for rule_name, rule_condition in quality_checks:
            df_with_checks = df_with_checks.withColumn(f"check_{rule_name}", rule_condition)
        
        check_columns = [f"check_{rule_name}" for rule_name, _ in quality_checks]
        df_with_checks = df_with_checks.withColumn("is_valid", F.expr(" AND ".join(check_columns)))
        
        # Split valid and quarantine
        df_valid = df_with_checks.filter(F.col("is_valid") == True).drop(*check_columns, "is_valid")
        df_quarantine = df_with_checks.filter(F.col("is_valid") == False).withColumn("quarantine_timestamp", F.current_timestamp())
        
        valid_count = df_valid.count()
        quarantine_count = df_quarantine.count()
        
        # Deduplication
        window_spec = Window.partitionBy("ticker", "date").orderBy(F.col("ingestion_timestamp").desc())
        df_deduplicated = df_valid.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1).drop("row_num")
        
        # MERGE INTO Silver
        if spark.catalog.tableExists(SILVER_TABLE):
            delta_table = DeltaTable.forName(spark, SILVER_TABLE)
            delta_table.alias("target").merge(
                df_deduplicated.alias("source"),
                "target.ticker = source.ticker AND target.date = source.date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_deduplicated.write.format("delta").mode("overwrite") \
                .partitionBy("ingestion_date", "ticker").saveAsTable(SILVER_TABLE)
        
        # Write quarantine
        if quarantine_count > 0:
            df_quarantine.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
        
        status.complete_layer(
            "silver",
            records_processed=valid_count,
            metrics={
                "valid_records": valid_count,
                "quarantined_records": quarantine_count,
                "quality_pass_rate": f"{(valid_count/(valid_count+quarantine_count)*100):.2f}%",
                "table": SILVER_TABLE
            }
        )
        
        return valid_count, quarantine_count
        
    except Exception as e:
        status.fail_layer("silver", str(e))
        raise

if LAYER in ["all", "silver"]:
    silver_valid, silver_quarantine = run_silver_layer()

# COMMAND ----------

# DBTITLE 1,Task 3: Gold Layer - Analytics Tables
# Task 3: Gold Layer Analytics
# Builds business-ready aggregation tables

def run_gold_layer():
    """
    Execute Gold layer analytics.
    Returns: table_count
    """
    status.start_layer("gold")
    
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType
        
        SILVER_TABLE = f"{CATALOG_NAME}.silver.stock_prices_clean"
        SECTOR_TABLE = f"{CATALOG_NAME}.gold.ticker_sector_mapping"
        
        print(f"Reading from {SILVER_TABLE}...")
        df_silver = spark.table(SILVER_TABLE)
        
        # Join with sector mapping (create from config table if not exists)
        if not spark.catalog.tableExists(SECTOR_TABLE):
            print("Creating sector mapping table from config...")
            CONFIG_TABLE = f"{CATALOG_NAME}.config.nifty50_tickers"
            spark.table(CONFIG_TABLE) \
                .filter("is_active = true") \
                .select(
                    F.col("ticker_clean").alias("ticker_standard"),
                    F.col("sector")
                ).distinct() \
                .write.format("delta").mode("overwrite").saveAsTable(SECTOR_TABLE)
            print(f"Created sector mapping from {CONFIG_TABLE}")
        
        df_sectors = spark.table(SECTOR_TABLE)
        df_silver_with_sector = df_silver.join(df_sectors, on="ticker_standard", how="left")
        
        # Calculate moving averages
        window_20 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-19, 0)
        window_50 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-49, 0)
        window_200 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-199, 0)
        
        df_stock_metrics = df_silver_with_sector \
            .withColumn("ma_20", F.round(F.avg("close").over(window_20), 2)) \
            .withColumn("ma_50", F.round(F.avg("close").over(window_50), 2)) \
            .withColumn("ma_200", F.round(F.avg("close").over(window_200), 2)) \
            .withColumn("trend_signal",
                F.when((F.col("ma_20") > F.col("ma_50")) & (F.col("ma_50") > F.col("ma_200")), "Bullish")
                .when((F.col("ma_20") < F.col("ma_50")) & (F.col("ma_50") < F.col("ma_200")), "Bearish")
                .otherwise("Neutral"))
        
        # Write Gold Table 1: Stock Daily Metrics
        GOLD_STOCK_METRICS = f"{CATALOG_NAME}.gold.stock_daily_metrics"
        df_stock_metrics.select(
            "ticker_standard", "sector", "date", "open", "high", "low", "close", "volume",
            "daily_return_pct", "price_range", "ma_20", "ma_50", "ma_200", "trend_signal"
        ).write.format("delta").mode("overwrite").partitionBy("date").saveAsTable(GOLD_STOCK_METRICS)
        
        # Write Gold Table 2: Sector Metrics
        GOLD_SECTOR_METRICS = f"{CATALOG_NAME}.gold.sector_daily_metrics"
        df_stock_metrics.filter(F.col("sector").isNotNull()).groupBy("sector", "date").agg(
            F.count("ticker_standard").alias("stock_count"),
            F.round(F.avg("daily_return_pct"), 2).alias("avg_daily_return_pct"),
            F.sum("volume").alias("total_volume")
        ).write.format("delta").mode("overwrite").partitionBy("date").saveAsTable(GOLD_SECTOR_METRICS)
        
        # Write Gold Table 3: Top Movers
        GOLD_TOP_MOVERS = f"{CATALOG_NAME}.gold.top_movers_daily"
        window_rank = Window.partitionBy("date").orderBy(F.desc("daily_return_pct"))
        df_stock_metrics.withColumn("rank", F.row_number().over(window_rank)) \
            .filter(F.col("rank") <= 10) \
            .select("date", "ticker_standard", "sector", "daily_return_pct", "close") \
            .write.format("delta").mode("overwrite").partitionBy("date").saveAsTable(GOLD_TOP_MOVERS)
        
        table_count = 3
        total_records = df_stock_metrics.count()
        
        status.complete_layer(
            "gold",
            records_processed=total_records,
            metrics={
                "tables_created": table_count,
                "stock_metrics_records": total_records
            }
        )
        
        return table_count
        
    except Exception as e:
        status.fail_layer("gold", str(e))
        raise

if LAYER in ["all", "gold"]:
    gold_tables = run_gold_layer()

# COMMAND ----------

# DBTITLE 1,Task 4: Dashboard Refresh
# Task 4: Refresh Dashboard After Data Load
# Refreshes the Stock Market Analysis Dashboard with latest data

def refresh_dashboard():
    """
    Refresh the Stock Market Analysis Dashboard.
    """
    status.start_layer("dashboard_refresh")
    
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import WarehouseType
        
        # Dashboard configuration
        DASHBOARD_ID = "01f1882f566e19eebabed1cd928b4b63"
        DASHBOARD_NAME = "Stock Market Analysis Dashboard"
        
        print(f"Refreshing dashboard: {DASHBOARD_NAME}")
        print(f"Dashboard ID: {DASHBOARD_ID}")
        
        # Initialize Databricks SDK client
        w = WorkspaceClient()
        
        try:
            # Get dashboard details
            dashboard = w.lakeview.get(DASHBOARD_ID)
            print(f"✓ Found dashboard: {dashboard.display_name}")
            
            # Publish the dashboard to refresh with latest data
            # This ensures all widgets query the updated gold tables
            published = w.lakeview.publish(DASHBOARD_ID)
            print(f"✓ Dashboard published and refreshed")
            print(f"  Published version: {published.published_dashboard_id}")
            
            status.complete_layer(
                "dashboard_refresh",
                records_processed=1,
                metrics={
                    "dashboard_id": DASHBOARD_ID,
                    "dashboard_name": DASHBOARD_NAME,
                    "status": "refreshed"
                }
            )
            
            return True
            
        except Exception as sdk_error:
            print(f"⚠️  SDK method failed: {str(sdk_error)}")
            print("Dashboard will auto-refresh on next view")
            
            status.complete_layer(
                "dashboard_refresh",
                records_processed=1,
                metrics={
                    "dashboard_id": DASHBOARD_ID,
                    "status": "skipped - will auto-refresh",
                    "note": "Dashboard queries will use latest data on next access"
                }
            )
            return True
        
    except Exception as e:
        print(f"⚠️  Dashboard refresh warning: {str(e)}")
        print("Pipeline completed successfully. Dashboard will refresh on next view.")
        status.complete_layer(
            "dashboard_refresh",
            records_processed=0,
            metrics={"status": "skipped", "reason": str(e)}
        )
        return True  # Don't fail pipeline if dashboard refresh fails

if LAYER in ["all", "gold"]:
    refresh_dashboard()

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Summary
# Generate final summary
final_status = status.get_summary()

# Return status for Job monitoring
dbutils.notebook.exit(json.dumps(final_status))

# COMMAND ----------

# DBTITLE 1,Orchestration Summary
# MAGIC %md
# MAGIC ## ✓ Step 8: Orchestration Notebook Created
# MAGIC
# MAGIC ### What This Notebook Does:
# MAGIC
# MAGIC **End-to-End Pipeline Execution:**
# MAGIC 1. **Bronze Layer** - Fetches data from yfinance API
# MAGIC 2. **Silver Layer** - Applies quality checks and cleaning
# MAGIC 3. **Gold Layer** - Builds analytics tables with moving averages
# MAGIC 4. **Dashboard Refresh** - Refreshes Stock Market Analysis Dashboard with latest data
# MAGIC
# MAGIC **Features:**
# MAGIC * ✓ Parameterized execution (start_date, end_date, layer)
# MAGIC * ✓ Status tracking and metrics collection
# MAGIC * ✓ Error handling with detailed logging
# MAGIC * ✓ Structured JSON output for monitoring
# MAGIC * ✓ Can run all layers or individual layers
# MAGIC
# MAGIC ### Usage:
# MAGIC
# MAGIC **Run all layers:**
# MAGIC ```python
# MAGIC # Default: runs all layers for yesterday's data
# MAGIC dbutils.notebook.run("/path/to/notebook", 3600)
# MAGIC ```
# MAGIC
# MAGIC **Run specific layer:**
# MAGIC ```python
# MAGIC dbutils.notebook.run(
# MAGIC     "/path/to/notebook",
# MAGIC     3600,
# MAGIC     {"layer": "silver", "start_date": "2026-07-24", "end_date": "2026-07-24"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Next Step:
# MAGIC Create a **Databricks Job** with:
# MAGIC * Multi-task workflow (Bronze → Silver → Gold in sequence)
# MAGIC * Daily schedule (4 PM IST, post-market close)
# MAGIC * Retry logic and failure alerts
# MAGIC * Email/Slack notifications
# MAGIC
# MAGIC Let's navigate to the Jobs UI to create the production workflow!

# COMMAND ----------

