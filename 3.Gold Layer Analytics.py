# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Step 7: Gold Layer Configuration
# Step 7: Gold Layer - Business Aggregates and Analytics
# Purpose: Create query-ready, business-focused analytics tables

from pyspark.sql import functions as F, Window
from pyspark.sql.types import LongType

# Unity Catalog Configuration
CATALOG_NAME = "StockMarketLakehouse"
SILVER_TABLE = f"{CATALOG_NAME}.silver.stock_prices_clean"
GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

# Gold layer tables
GOLD_STOCK_METRICS = f"{GOLD_SCHEMA}.stock_daily_metrics"
GOLD_SECTOR_METRICS = f"{GOLD_SCHEMA}.sector_daily_metrics"
GOLD_TOP_MOVERS = f"{GOLD_SCHEMA}.top_movers_daily"
GOLD_MARKET_OVERVIEW = f"{GOLD_SCHEMA}.market_overview_daily"

print("✓ Gold layer configuration loaded")
print(f"  Source: {SILVER_TABLE}")
print(f"  Target schema: {GOLD_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create Gold schema
# MAGIC %sql
# MAGIC -- Create the gold schema for business-ready analytics
# MAGIC CREATE SCHEMA IF NOT EXISTS StockMarketLakehouse.gold
# MAGIC COMMENT 'Business-ready analytics and aggregations - BI tool ready';
# MAGIC
# MAGIC DESCRIBE SCHEMA EXTENDED StockMarketLakehouse.gold;

# COMMAND ----------

# DBTITLE 1,Create Ticker → Sector Mapping Table
# Create a static mapping table: ticker → sector
# In production, this would come from a reference data source

from pyspark.sql.types import StructType, StructField, StringType

# Nifty 50 sector mapping (industry classification)
sector_mapping_data = [
    # Financial Services
    ('HDFCBANK', 'Financial Services'),
    ('ICICIBANK', 'Financial Services'),
    ('KOTAKBANK', 'Financial Services'),
    ('AXISBANK', 'Financial Services'),
    ('SBIN', 'Financial Services'),
    ('INDUSINDBK', 'Financial Services'),
    ('BAJFINANCE', 'Financial Services'),
    ('BAJAJFINSV', 'Financial Services'),
    ('HDFCLIFE', 'Financial Services'),
    ('SBILIFE', 'Financial Services'),
    
    # IT Services
    ('TCS', 'IT Services'),
    ('INFY', 'IT Services'),
    ('HCLTECH', 'IT Services'),
    ('WIPRO', 'IT Services'),
    ('TECHM', 'IT Services'),
    
    # Oil & Gas
    ('RELIANCE', 'Oil & Gas'),
    ('ONGC', 'Oil & Gas'),
    ('BPCL', 'Oil & Gas'),
    
    # FMCG
    ('HINDUNILVR', 'FMCG'),
    ('ITC', 'FMCG'),
    ('NESTLEIND', 'FMCG'),
    ('BRITANNIA', 'FMCG'),
    ('TATACONSUM', 'FMCG'),
    
    # Pharma
    ('SUNPHARMA', 'Pharma'),
    ('DRREDDY', 'Pharma'),
    ('CIPLA', 'Pharma'),
    ('DIVISLAB', 'Pharma'),
    ('APOLLOHOSP', 'Pharma'),
    
    # Auto
    ('MARUTI', 'Auto'),
    ('TATAMOTORS', 'Auto'),
    ('M&M', 'Auto'),
    ('EICHERMOT', 'Auto'),
    ('BAJAJ-AUTO', 'Auto'),
    ('HEROMOTOCO', 'Auto'),
    
    # Metals & Mining
    ('TATASTEEL', 'Metals & Mining'),
    ('HINDALCO', 'Metals & Mining'),
    ('JSWSTEEL', 'Metals & Mining'),
    ('COALINDIA', 'Metals & Mining'),
    
    # Cement
    ('ULTRACEMCO', 'Cement'),
    ('SHREECEM', 'Cement'),
    ('GRASIM', 'Cement'),
    
    # Consumer Durables
    ('TITAN', 'Consumer Durables'),
    ('ASIANPAINT', 'Consumer Durables'),
    
    # Telecom
    ('BHARTIARTL', 'Telecom'),
    
    # Infrastructure
    ('LT', 'Infrastructure'),
    ('ADANIPORTS', 'Infrastructure'),
    ('POWERGRID', 'Infrastructure'),
    ('NTPC', 'Infrastructure'),
    ('ADANIENT', 'Infrastructure'),
    
    # Agro Chemicals
    ('UPL', 'Agro Chemicals')
]

# Create DataFrame
sector_schema = StructType([
    StructField("ticker_standard", StringType(), False),
    StructField("sector", StringType(), False)
])

df_sectors = spark.createDataFrame(sector_mapping_data, schema=sector_schema)

# Write to Delta table
SECTOR_TABLE = f"{GOLD_SCHEMA}.ticker_sector_mapping"

df_sectors.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(SECTOR_TABLE)

print(f"✓ Created sector mapping table: {SECTOR_TABLE}")
print(f"  Total sectors: {df_sectors.select('sector').distinct().count()}")
print(f"  Total tickers mapped: {df_sectors.count()}")

# Display sector distribution
print("\nSector distribution:")
display(df_sectors.groupBy("sector").count().orderBy(F.desc("count")))

# COMMAND ----------

# DBTITLE 1,Read Silver data and join with sectors
# Read Silver table and join with sector mapping
print(f"Reading data from {SILVER_TABLE}...")

df_silver = spark.table(SILVER_TABLE)
df_sectors = spark.table(SECTOR_TABLE)

# Join with sector mapping
df_silver_with_sector = df_silver.join(
    df_sectors,
    on="ticker_standard",
    how="left"
)

print(f"✓ Loaded {df_silver_with_sector.count():,} records")
print(f"  Date range: {df_silver.agg(F.min('date')).collect()[0][0]} to {df_silver.agg(F.max('date')).collect()[0][0]}")

# Check for unmapped tickers
unmapped_count = df_silver_with_sector.filter(F.col("sector").isNull()).count()
if unmapped_count > 0:
    print(f"\n⚠️  Warning: {unmapped_count} records without sector mapping")
    print("\nUnmapped tickers:")
    display(df_silver_with_sector.filter(F.col("sector").isNull()).select("ticker_standard").distinct())
else:
    print("✓ All tickers mapped to sectors")

# COMMAND ----------

# DBTITLE 1,Gold Table 1: Stock Daily Metrics with Moving Averages
# Gold Table 1: Stock Daily Metrics with Moving Averages (20/50/200-day)
# This is THE core analytics table for stock analysis

print("Building Stock Daily Metrics with Moving Averages...\n")

# Define window specifications for moving averages
window_20 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-19, 0)
window_50 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-49, 0)
window_200 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-199, 0)

# Calculate daily metrics with moving averages
df_stock_metrics = df_silver_with_sector \
    .withColumn("ma_20", F.round(F.avg("close").over(window_20), 2)) \
    .withColumn("ma_50", F.round(F.avg("close").over(window_50), 2)) \
    .withColumn("ma_200", F.round(F.avg("close").over(window_200), 2)) \
    .withColumn(
        "ma_20_volume",
        F.round(F.avg("volume").over(window_20), 0).cast(LongType())
    ) \
    .withColumn(
        "price_vs_ma20_pct",
        F.round(((F.col("close") - F.col("ma_20")) / F.col("ma_20")) * 100, 2)
    ) \
    .withColumn(
        "price_vs_ma50_pct",
        F.round(((F.col("close") - F.col("ma_50")) / F.col("ma_50")) * 100, 2)
    ) \
    .withColumn(
        "price_vs_ma200_pct",
        F.round(((F.col("close") - F.col("ma_200")) / F.col("ma_200")) * 100, 2)
    ) \
    .withColumn(
        "trend_signal",
        F.when(
            (F.col("ma_20") > F.col("ma_50")) & (F.col("ma_50") > F.col("ma_200")),
            "Bullish"
        ).when(
            (F.col("ma_20") < F.col("ma_50")) & (F.col("ma_50") < F.col("ma_200")),
            "Bearish"
        ).otherwise("Neutral")
    ) \
    .withColumn(
        "volume_vs_avg_pct",
        F.round(((F.col("volume") - F.col("ma_20_volume")) / F.col("ma_20_volume")) * 100, 2)
    )

# Select final columns for Gold table
df_stock_metrics_final = df_stock_metrics.select(
    "ticker_standard",
    "sector",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "daily_return_pct",
    "price_range",
    "ma_20",
    "ma_50",
    "ma_200",
    "price_vs_ma20_pct",
    "price_vs_ma50_pct",
    "price_vs_ma200_pct",
    "trend_signal",
    "ma_20_volume",
    "volume_vs_avg_pct"
)

# Write to Gold table
print(f"Writing to {GOLD_STOCK_METRICS}...")

df_stock_metrics_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_STOCK_METRICS)

print(f"✓ Created {GOLD_STOCK_METRICS}")
print(f"  Records: {df_stock_metrics_final.count():,}")
print("\nSample data:")
display(df_stock_metrics_final.filter(F.col("ticker_standard") == "RELIANCE").orderBy(F.desc("date")).limit(5))

# COMMAND ----------

# DBTITLE 1,Gold Table 2: Sector-Level Daily Aggregations
# Gold Table 2: Sector-Level Daily Aggregations
# Roll up stock metrics to sector level for portfolio/market analysis

print("\nBuilding Sector Daily Metrics...\n")

# Filter out records without sector mapping
df_with_sector = df_stock_metrics_final.filter(F.col("sector").isNotNull())

# Aggregate to sector level
df_sector_metrics = df_with_sector.groupBy("sector", "date").agg(
    F.count("ticker_standard").alias("stock_count"),
    
    # Price metrics
    F.round(F.avg("close"), 2).alias("avg_close_price"),
    F.round(F.avg("daily_return_pct"), 2).alias("avg_daily_return_pct"),
    F.round(F.stddev("daily_return_pct"), 2).alias("return_volatility"),
    
    # Moving average metrics
    F.round(F.avg("price_vs_ma20_pct"), 2).alias("avg_price_vs_ma20_pct"),
    F.round(F.avg("price_vs_ma50_pct"), 2).alias("avg_price_vs_ma50_pct"),
    
    # Volume metrics
    F.sum("volume").alias("total_volume"),
    F.round(F.avg("volume"), 0).cast(LongType()).alias("avg_volume"),
    F.round(F.avg("volume_vs_avg_pct"), 2).alias("avg_volume_vs_avg_pct"),
    
    # Trend signals
    F.sum(F.when(F.col("trend_signal") == "Bullish", 1).otherwise(0)).alias("bullish_count"),
    F.sum(F.when(F.col("trend_signal") == "Bearish", 1).otherwise(0)).alias("bearish_count"),
    F.sum(F.when(F.col("trend_signal") == "Neutral", 1).otherwise(0)).alias("neutral_count"),
    
    # Gainers/Losers
    F.sum(F.when(F.col("daily_return_pct") > 0, 1).otherwise(0)).alias("gainers_count"),
    F.sum(F.when(F.col("daily_return_pct") < 0, 1).otherwise(0)).alias("losers_count"),
    
    # Best/Worst performers
    F.max("daily_return_pct").alias("best_performer_return"),
    F.min("daily_return_pct").alias("worst_performer_return")
).withColumn(
    "sector_sentiment",
    F.when(F.col("avg_daily_return_pct") > 1, "Positive")
     .when(F.col("avg_daily_return_pct") < -1, "Negative")
     .otherwise("Neutral")
)

# Write to Gold table
print(f"Writing to {GOLD_SECTOR_METRICS}...")

df_sector_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_SECTOR_METRICS)

print(f"✓ Created {GOLD_SECTOR_METRICS}")
print(f"  Records: {df_sector_metrics.count():,}")
print("\nSample data (latest date):")
latest_date = df_sector_metrics.agg(F.max("date")).collect()[0][0]
display(
    df_sector_metrics
    .filter(F.col("date") == latest_date)
    .orderBy(F.desc("avg_daily_return_pct"))
)

# COMMAND ----------

# DBTITLE 1,Gold Table 3: Top Movers (Gainers/Losers) Daily
# Gold Table 3: Top Gainers and Losers per Day
# Pre-compute top 10 gainers and losers for each trading day

print("\nBuilding Top Movers Daily...\n")

# Rank stocks by daily return within each date
window_gainers = Window.partitionBy("date").orderBy(F.desc("daily_return_pct"))
window_losers = Window.partitionBy("date").orderBy(F.asc("daily_return_pct"))

# Get top 10 gainers
df_top_gainers = df_stock_metrics_final \
    .withColumn("rank", F.row_number().over(window_gainers)) \
    .filter(F.col("rank") <= 10) \
    .withColumn("mover_type", F.lit("Gainer")) \
    .select(
        "date",
        "ticker_standard",
        "sector",
        "mover_type",
        F.col("rank").alias("rank_position"),
        "close",
        "daily_return_pct",
        "volume",
        "volume_vs_avg_pct",
        "trend_signal"
    )

# Get top 10 losers
df_top_losers = df_stock_metrics_final \
    .withColumn("rank", F.row_number().over(window_losers)) \
    .filter(F.col("rank") <= 10) \
    .withColumn("mover_type", F.lit("Loser")) \
    .select(
        "date",
        "ticker_standard",
        "sector",
        "mover_type",
        F.col("rank").alias("rank_position"),
        "close",
        "daily_return_pct",
        "volume",
        "volume_vs_avg_pct",
        "trend_signal"
    )

# Union gainers and losers
df_top_movers = df_top_gainers.union(df_top_losers)

# Write to Gold table
print(f"Writing to {GOLD_TOP_MOVERS}...")

df_top_movers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("date", "mover_type") \
    .saveAsTable(GOLD_TOP_MOVERS)

print(f"✓ Created {GOLD_TOP_MOVERS}")
print(f"  Records: {df_top_movers.count():,}")
print("\nTop 5 Gainers (latest date):")
latest_date = df_top_movers.agg(F.max("date")).collect()[0][0]
display(
    df_top_movers
    .filter((F.col("date") == latest_date) & (F.col("mover_type") == "Gainer"))
    .orderBy("rank_position")
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Gold Table 4: Market Overview Daily
# Gold Table 4: Market-Level Daily Overview
# Single-row per day summary of overall market health

print("\nBuilding Market Overview Daily...\n")

df_market_overview = df_stock_metrics_final.groupBy("date").agg(
    # Stock counts
    F.count("ticker_standard").alias("total_stocks"),
    F.countDistinct("sector").alias("sectors_tracked"),
    
    # Market returns
    F.round(F.avg("daily_return_pct"), 2).alias("market_avg_return_pct"),
    F.round(F.stddev("daily_return_pct"), 2).alias("market_volatility"),
    F.round(F.expr("percentile_approx(daily_return_pct, 0.5)"), 2).alias("median_return_pct"),
    
    # Price movements
    F.round(F.avg("close"), 2).alias("avg_stock_price"),
    F.round(F.avg("price_range"), 2).alias("avg_price_range"),
    
    # Volume
    F.sum("volume").alias("total_market_volume"),
    F.round(F.avg("volume"), 0).cast(LongType()).alias("avg_stock_volume"),
    
    # Breadth indicators (gainers vs losers)
    F.sum(F.when(F.col("daily_return_pct") > 0, 1).otherwise(0)).alias("advancing_stocks"),
    F.sum(F.when(F.col("daily_return_pct") < 0, 1).otherwise(0)).alias("declining_stocks"),
    F.sum(F.when(F.col("daily_return_pct") == 0, 1).otherwise(0)).alias("unchanged_stocks"),
    
    # Trend indicators
    F.sum(F.when(F.col("trend_signal") == "Bullish", 1).otherwise(0)).alias("bullish_trends"),
    F.sum(F.when(F.col("trend_signal") == "Bearish", 1).otherwise(0)).alias("bearish_trends"),
    
    # Moving average positions
    F.sum(F.when(F.col("close") > F.col("ma_20"), 1).otherwise(0)).alias("above_ma20_count"),
    F.sum(F.when(F.col("close") > F.col("ma_50"), 1).otherwise(0)).alias("above_ma50_count"),
    F.sum(F.when(F.col("close") > F.col("ma_200"), 1).otherwise(0)).alias("above_ma200_count"),
    
    # Extreme movers
    F.max("daily_return_pct").alias("best_stock_return"),
    F.min("daily_return_pct").alias("worst_stock_return")
)

df_market_overview = df_market_overview.withColumn(
    "advance_decline_ratio",
    F.when(F.col("declining_stocks") == 0, None)
     .otherwise(F.round(F.col("advancing_stocks") / F.col("declining_stocks"), 2))
).withColumn(
    "market_sentiment",
    F.when(F.col("advancing_stocks") > F.col("declining_stocks") * 1.5, "Bullish")
     .when(F.col("declining_stocks") > F.col("advancing_stocks") * 1.5, "Bearish")
     .otherwise("Neutral")
)

# Write to Gold table
print(f"Writing to {GOLD_MARKET_OVERVIEW}...")

df_market_overview.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_MARKET_OVERVIEW)

print(f"✓ Created {GOLD_MARKET_OVERVIEW}")
print(f"  Records: {df_market_overview.count():,}")
print("\nMarket overview (last 5 days):")
display(df_market_overview.orderBy(F.desc("date")).limit(5))

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables - Summary
# MAGIC %sql
# MAGIC -- Verify all Gold tables were created
# MAGIC SHOW TABLES IN StockMarketLakehouse.gold;

# COMMAND ----------

# DBTITLE 1,Query Gold Table 1: Stock with Moving Averages
# MAGIC %sql
# MAGIC -- Sample query: Stocks with bullish trend (MA alignment)
# MAGIC SELECT 
# MAGIC     ticker_standard,
# MAGIC     sector,
# MAGIC     date,
# MAGIC     close,
# MAGIC     ma_20,
# MAGIC     ma_50,
# MAGIC     ma_200,
# MAGIC     daily_return_pct,
# MAGIC     trend_signal,
# MAGIC     price_vs_ma20_pct
# MAGIC FROM StockMarketLakehouse.gold.stock_daily_metrics
# MAGIC WHERE date = (SELECT MAX(date) FROM StockMarketLakehouse.gold.stock_daily_metrics)
# MAGIC     AND trend_signal = 'Bullish'
# MAGIC     AND price_vs_ma20_pct > 0
# MAGIC ORDER BY daily_return_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query Gold Table 2: Sector Performance
# MAGIC %sql
# MAGIC -- Sample query: Best performing sectors today
# MAGIC SELECT 
# MAGIC     sector,
# MAGIC     stock_count,
# MAGIC     avg_daily_return_pct,
# MAGIC     return_volatility,
# MAGIC     total_volume,
# MAGIC     gainers_count,
# MAGIC     losers_count,
# MAGIC     sector_sentiment
# MAGIC FROM StockMarketLakehouse.gold.sector_daily_metrics
# MAGIC WHERE date = (SELECT MAX(date) FROM StockMarketLakehouse.gold.sector_daily_metrics)
# MAGIC ORDER BY avg_daily_return_pct DESC;

# COMMAND ----------

# DBTITLE 1,Query Gold Table 3: Top Movers
# MAGIC %sql
# MAGIC -- Sample query: Today's top 5 gainers and losers
# MAGIC WITH latest_date AS (
# MAGIC     SELECT MAX(date) as max_date 
# MAGIC     FROM StockMarketLakehouse.gold.top_movers_daily
# MAGIC )
# MAGIC SELECT 
# MAGIC     mover_type,
# MAGIC     rank_position,
# MAGIC     ticker_standard,
# MAGIC     sector,
# MAGIC     close,
# MAGIC     daily_return_pct,
# MAGIC     volume,
# MAGIC     volume_vs_avg_pct
# MAGIC FROM StockMarketLakehouse.gold.top_movers_daily
# MAGIC WHERE date = (SELECT max_date FROM latest_date)
# MAGIC     AND rank_position <= 5
# MAGIC ORDER BY mover_type DESC, rank_position;

# COMMAND ----------

# DBTITLE 1,Query Gold Table 4: Market Overview
# MAGIC %sql
# MAGIC -- Sample query: Market health overview (last 7 days)
# MAGIC SELECT 
# MAGIC     date,
# MAGIC     total_stocks,
# MAGIC     market_avg_return_pct,
# MAGIC     market_volatility,
# MAGIC     advancing_stocks,
# MAGIC     declining_stocks,
# MAGIC     advance_decline_ratio,
# MAGIC     market_sentiment,
# MAGIC     total_market_volume,
# MAGIC     above_ma200_count
# MAGIC FROM StockMarketLakehouse.gold.market_overview_daily
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 7;

# COMMAND ----------

# DBTITLE 1,Gold Layer Summary & BI Use Cases
# MAGIC %md
# MAGIC ## ✓ Step 7 Complete: Gold Layer Business Analytics
# MAGIC
# MAGIC ### What We Built:
# MAGIC
# MAGIC **4 Query-Ready Gold Tables:**
# MAGIC
# MAGIC | Table | Purpose | Key Metrics |
# MAGIC |-------|---------|-------------|
# MAGIC | **stock_daily_metrics** | Individual stock analysis | Moving averages (20/50/200-day), trend signals, price vs MA |
# MAGIC | **sector_daily_metrics** | Sector-level aggregations | Sector returns, volatility, breadth indicators |
# MAGIC | **top_movers_daily** | Top gainers/losers | Daily top 10 performers/underperformers |
# MAGIC | **market_overview_daily** | Overall market health | Market breadth, volatility, sentiment |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Features Implemented:
# MAGIC
# MAGIC **1. Moving Averages (20/50/200-day)** ⭐
# MAGIC ```sql
# MAGIC -- Window functions for rolling calculations
# MAGIC Window.partitionBy("ticker").orderBy("date").rowsBetween(-19, 0)
# MAGIC ```
# MAGIC
# MAGIC **2. Trend Signals**
# MAGIC - **Bullish:** MA20 > MA50 > MA200 (golden cross)
# MAGIC - **Bearish:** MA20 < MA50 < MA200 (death cross)
# MAGIC - **Neutral:** Mixed signals
# MAGIC
# MAGIC **3. Sector Mapping**
# MAGIC - Created `ticker_sector_mapping` reference table
# MAGIC - 13 sectors across Nifty 50 stocks
# MAGIC - Enables sector-level analytics
# MAGIC
# MAGIC **4. Market Breadth Indicators**
# MAGIC - Advance/Decline ratio
# MAGIC - Stocks above key moving averages
# MAGIC - Volume vs average comparisons
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### BI Tool Ready Queries:
# MAGIC
# MAGIC **Use Case 1: Portfolio Dashboard**
# MAGIC ```sql
# MAGIC SELECT ticker_standard, close, ma_20, ma_50, ma_200, trend_signal
# MAGIC FROM gold.stock_daily_metrics
# MAGIC WHERE date = CURRENT_DATE() - 1;
# MAGIC ```
# MAGIC
# MAGIC **Use Case 2: Sector Heatmap**
# MAGIC ```sql
# MAGIC SELECT sector, avg_daily_return_pct, return_volatility
# MAGIC FROM gold.sector_daily_metrics
# MAGIC WHERE date = CURRENT_DATE() - 1;
# MAGIC ```
# MAGIC
# MAGIC **Use Case 3: Top Movers Alert**
# MAGIC ```sql
# MAGIC SELECT ticker_standard, daily_return_pct
# MAGIC FROM gold.top_movers_daily
# MAGIC WHERE date = CURRENT_DATE() - 1 AND mover_type = 'Gainer';
# MAGIC ```
# MAGIC
# MAGIC **Use Case 4: Market Health Scorecard**
# MAGIC ```sql
# MAGIC SELECT market_avg_return_pct, advance_decline_ratio, market_sentiment
# MAGIC FROM gold.market_overview_daily
# MAGIC ORDER BY date DESC LIMIT 1;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DE Skills Demonstrated:
# MAGIC
# MAGIC | Skill | Implementation |
# MAGIC |-------|---------------|
# MAGIC | **Window Functions** | Moving averages, ranking, lag calculations |
# MAGIC | **Aggregations** | Sector rollups, market-level summaries |
# MAGIC | **Reference Data** | Static mapping tables (ticker → sector) |
# MAGIC | **Partitioning Strategy** | By date for time-series queries |
# MAGIC | **Business Logic** | Trend signals, sentiment classification |
# MAGIC | **Query Optimization** | Pre-computed aggregates vs on-the-fly |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Interview Talking Points:
# MAGIC
# MAGIC ✓ **"Built 4 Gold tables optimized for BI consumption with pre-computed metrics"**
# MAGIC
# MAGIC ✓ **"Implemented 20/50/200-day moving averages using Spark Window functions"**
# MAGIC
# MAGIC ✓ **"Created sector hierarchy with reference data for drill-down analysis"**
# MAGIC
# MAGIC ✓ **"Pre-computed top gainers/losers to avoid expensive daily ranking queries"**
# MAGIC
# MAGIC ✓ **"Partitioned by date for efficient time-series lookups in dashboards"**
# MAGIC
# MAGIC ✓ **"Market overview table provides single-row summaries for executive dashboards"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Gold Layer Schema Summary:
# MAGIC
# MAGIC ```
# MAGIC StockMarketLakehouse.gold/
# MAGIC ├── ticker_sector_mapping (reference)
# MAGIC ├── stock_daily_metrics (core analytics)
# MAGIC │   ├── MA 20/50/200
# MAGIC │   ├── Trend signals
# MAGIC │   └── Price/volume metrics
# MAGIC ├── sector_daily_metrics (aggregations)
# MAGIC │   ├── Sector returns
# MAGIC │   └── Breadth indicators
# MAGIC ├── top_movers_daily (rankings)
# MAGIC │   ├── Top 10 gainers
# MAGIC │   └── Top 10 losers
# MAGIC └── market_overview_daily (summary)
# MAGIC     ├── Market breadth
# MAGIC     └── Sentiment indicators
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Medallion Architecture Complete!
# MAGIC
# MAGIC **Bronze** (Raw) → **Silver** (Clean) → **Gold** (Analytics) ✓
# MAGIC
# MAGIC Your lakehouse is now production-ready with:
# MAGIC - 60K+ cleaned stock records
# MAGIC - 4 business analytics tables
# MAGIC - BI tool ready queries
# MAGIC - Comprehensive data quality
# MAGIC - Full audit trail

# COMMAND ----------

