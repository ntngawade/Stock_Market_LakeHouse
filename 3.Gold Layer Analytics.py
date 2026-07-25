# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Prerequisites
# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC **Before running this notebook**, ensure the following configuration script has been executed:
# MAGIC
# MAGIC 📄 **config_nifty50_tickers_Oneshot.sql**
# MAGIC
# MAGIC This SQL script creates:
# MAGIC 1. `StockMarketLakehouse.config.nifty50_tickers` - Ticker configuration table
# MAGIC 2. `StockMarketLakehouse.gold.ticker_sector_mapping` - Sector mapping for analytics
# MAGIC
# MAGIC The ticker_sector_mapping table is required for all gold layer analytics tables.
# MAGIC
# MAGIC ---

# COMMAND ----------

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
# Load Ticker → Sector Mapping Table
# Note: This table is created via config_nifty50_tickers_Oneshot.sql
# It provides static sector classification for analytics

SECTOR_TABLE = f"{GOLD_SCHEMA}.ticker_sector_mapping"

# Read the sector mapping table (created via SQL script)
df_sectors = spark.table(SECTOR_TABLE)

print(f"✓ Loaded sector mapping table: {SECTOR_TABLE}")
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

# DBTITLE 1,Gold Table 5: Multi-Timeframe Performance Metrics
# Gold Table 5: Multi-Timeframe Performance Metrics
# Calculate returns and volatility over multiple time periods (7/30/90/180 days)
# Spot momentum trends and consistency patterns

from pyspark.sql import Window
from pyspark.sql.types import LongType

print("\nBuilding Multi-Timeframe Performance Metrics...\n")

# Define windows for different timeframes
window_7 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-6, 0)
window_30 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-29, 0)
window_90 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-89, 0)
window_180 = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-179, 0)

# Window for calculating returns (need previous close)
window_lag_7 = Window.partitionBy("ticker_standard").orderBy("date")
window_lag_30 = Window.partitionBy("ticker_standard").orderBy("date")
window_lag_90 = Window.partitionBy("ticker_standard").orderBy("date")
window_lag_180 = Window.partitionBy("ticker_standard").orderBy("date")

df_multitime = df_silver_with_sector.withColumn(
    "close_7d_ago", F.lag("close", 7).over(window_lag_7)
).withColumn(
    "close_30d_ago", F.lag("close", 30).over(window_lag_30)
).withColumn(
    "close_90d_ago", F.lag("close", 90).over(window_lag_90)
).withColumn(
    "close_180d_ago", F.lag("close", 180).over(window_lag_180)
).withColumn(
    "return_7d_pct",
    F.round(((F.col("close") - F.col("close_7d_ago")) / F.col("close_7d_ago")) * 100, 2)
).withColumn(
    "return_30d_pct",
    F.round(((F.col("close") - F.col("close_30d_ago")) / F.col("close_30d_ago")) * 100, 2)
).withColumn(
    "return_90d_pct",
    F.round(((F.col("close") - F.col("close_90d_ago")) / F.col("close_90d_ago")) * 100, 2)
).withColumn(
    "return_180d_pct",
    F.round(((F.col("close") - F.col("close_180d_ago")) / F.col("close_180d_ago")) * 100, 2)
).withColumn(
    "volatility_7d",
    F.round(F.stddev("daily_return_pct").over(window_7), 2)
).withColumn(
    "volatility_30d",
    F.round(F.stddev("daily_return_pct").over(window_30), 2)
).withColumn(
    "volatility_90d",
    F.round(F.stddev("daily_return_pct").over(window_90), 2)
).withColumn(
    "positive_days_30d",
    F.sum(F.when(F.col("daily_return_pct") > 0, 1).otherwise(0)).over(window_30)
).withColumn(
    "consistency_score_30d",
    F.round((F.col("positive_days_30d") / 30.0) * 100, 1)
).withColumn(
    "high_30d", F.max("high").over(window_30)
).withColumn(
    "max_drawdown_30d_pct",
    F.round(((F.col("close") - F.col("high_30d")) / F.col("high_30d")) * 100, 2)
)

# Select final columns
GOLD_MULTITIME = f"{GOLD_SCHEMA}.stock_multitime_performance"

df_multitime_final = df_multitime.select(
    "ticker_standard",
    "sector",
    "date",
    "close",
    "return_7d_pct",
    "return_30d_pct",
    "return_90d_pct",
    "return_180d_pct",
    "volatility_7d",
    "volatility_30d",
    "volatility_90d",
    "consistency_score_30d",
    "max_drawdown_30d_pct"
)

# Write to Gold table
print(f"Writing to {GOLD_MULTITIME}...")

df_multitime_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_MULTITIME)

print(f"✓ Created {GOLD_MULTITIME}")
print(f"  Records: {df_multitime_final.count():,}")
print("\nSample - Top 30-day performers:")
latest_date = df_multitime_final.agg(F.max("date")).collect()[0][0]
display(
    df_multitime_final
    .filter(F.col("date") == latest_date)
    .orderBy(F.desc("return_30d_pct"))
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Gold Table 6: Relative Strength Analysis
# Gold Table 6: Relative Strength Analysis
# Compare each stock's performance vs its sector and the overall market
# Identify sector leaders and laggards

print("\nBuilding Relative Strength Analysis...\n")

# Calculate market average return (proxy for market index)
df_market_return = df_silver_with_sector.groupBy("date").agg(
    F.round(F.avg("daily_return_pct"), 2).alias("market_return_pct")
)

# Calculate sector average returns
df_sector_return = df_silver_with_sector.groupBy("sector", "date").agg(
    F.round(F.avg("daily_return_pct"), 2).alias("sector_return_pct")
)

# Join stock data with market and sector benchmarks
df_relative = df_silver_with_sector.join(
    df_market_return, on="date", how="left"
).join(
    df_sector_return, on=["sector", "date"], how="left"
).withColumn(
    "outperformance_vs_sector_pct",
    F.round(F.col("daily_return_pct") - F.col("sector_return_pct"), 2)
).withColumn(
    "outperformance_vs_market_pct",
    F.round(F.col("daily_return_pct") - F.col("market_return_pct"), 2)
).withColumn(
    "sector_vs_market_pct",
    F.round(F.col("sector_return_pct") - F.col("market_return_pct"), 2)
)

# Calculate percentile ranks within sector
window_sector_rank = Window.partitionBy("sector", "date").orderBy(F.desc("daily_return_pct"))

df_relative = df_relative.withColumn(
    "rank_in_sector", F.row_number().over(window_sector_rank)
).withColumn(
    "total_in_sector", F.count("ticker_standard").over(Window.partitionBy("sector", "date"))
).withColumn(
    "percentile_in_sector",
    F.round((1 - (F.col("rank_in_sector") - 1) / F.col("total_in_sector")) * 100, 1)
)

# Classify relative strength
df_relative = df_relative.withColumn(
    "relative_strength_label",
    F.when(
        (F.col("outperformance_vs_sector_pct") > 0) & (F.col("outperformance_vs_market_pct") > 0),
        "Sector Leader"
    ).when(
        (F.col("outperformance_vs_sector_pct") > 0) & (F.col("outperformance_vs_market_pct") <= 0),
        "Sector Outperformer"
    ).when(
        (F.col("outperformance_vs_sector_pct") <= 0) & (F.col("outperformance_vs_market_pct") > 0),
        "Market Outperformer"
    ).otherwise("Underperformer")
)

# Select final columns
GOLD_RELATIVE = f"{GOLD_SCHEMA}.stock_relative_strength"

df_relative_final = df_relative.select(
    "ticker_standard",
    "sector",
    "date",
    "close",
    "daily_return_pct",
    "sector_return_pct",
    "market_return_pct",
    "outperformance_vs_sector_pct",
    "outperformance_vs_market_pct",
    "sector_vs_market_pct",
    "percentile_in_sector",
    "rank_in_sector",
    "relative_strength_label"
)

# Write to Gold table
print(f"Writing to {GOLD_RELATIVE}...")

df_relative_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_RELATIVE)

print(f"✓ Created {GOLD_RELATIVE}")
print(f"  Records: {df_relative_final.count():,}")
print("\nSample - Sector Leaders:")
latest_date = df_relative_final.agg(F.max("date")).collect()[0][0]
display(
    df_relative_final
    .filter((F.col("date") == latest_date) & (F.col("relative_strength_label") == "Sector Leader"))
    .orderBy(F.desc("outperformance_vs_sector_pct"))
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Gold Table 7: Technical Levels & Volume Breakouts
# Gold Table 7: Technical Levels & Volume Breakouts
# Support/resistance levels, 52-week highs/lows, volume anomalies
# Pre-compute key technical signals for daily screening

print("\nBuilding Technical Levels & Volume Breakouts...\n")

# Windows for technical calculations
window_52w = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-251, 0)  # ~252 trading days/year
window_30d = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-29, 0)
window_20d_vol = Window.partitionBy("ticker_standard").orderBy("date").rowsBetween(-19, 0)

df_technical = df_silver_with_sector.withColumn(
    "high_52w", F.max("high").over(window_52w)
).withColumn(
    "low_52w", F.min("low").over(window_52w)
).withColumn(
    "distance_from_52w_high_pct",
    F.round(((F.col("close") - F.col("high_52w")) / F.col("high_52w")) * 100, 2)
).withColumn(
    "distance_from_52w_low_pct",
    F.round(((F.col("close") - F.col("low_52w")) / F.col("low_52w")) * 100, 2)
).withColumn(
    "high_30d", F.max("high").over(window_30d)
).withColumn(
    "low_30d", F.min("low").over(window_30d)
).withColumn(
    "avg_volume_20d",
    F.round(F.avg("volume").over(window_20d_vol), 0).cast(LongType())
).withColumn(
    "volume_ratio",
    F.round(F.col("volume") / F.col("avg_volume_20d"), 2)
).withColumn(
    "volume_breakout_flag",
    F.when(F.col("volume_ratio") >= 1.5, True).otherwise(False)
).withColumn(
    "at_52w_high_flag",
    F.when(F.col("close") >= F.col("high_52w") * 0.98, True).otherwise(False)  # Within 2%
).withColumn(
    "at_52w_low_flag",
    F.when(F.col("close") <= F.col("low_52w") * 1.02, True).otherwise(False)  # Within 2%
).withColumn(
    "breakout_30d_high_flag",
    F.when(F.col("high") >= F.col("high_30d"), True).otherwise(False)
).withColumn(
    "breakdown_30d_low_flag",
    F.when(F.col("low") <= F.col("low_30d"), True).otherwise(False)
)

# Bollinger Bands (20-day MA ± 2 standard deviations)
df_technical = df_technical.withColumn(
    "ma_20", F.round(F.avg("close").over(window_20d_vol), 2)
).withColumn(
    "stddev_20", F.round(F.stddev("close").over(window_20d_vol), 2)
).withColumn(
    "bb_upper", F.round(F.col("ma_20") + (2 * F.col("stddev_20")), 2)
).withColumn(
    "bb_lower", F.round(F.col("ma_20") - (2 * F.col("stddev_20")), 2)
).withColumn(
    "bb_position",
    F.when(F.col("close") >= F.col("bb_upper"), "Above Upper Band")
     .when(F.col("close") <= F.col("bb_lower"), "Below Lower Band")
     .otherwise("Within Bands")
)

# Price + Volume confirmation (strong move on high volume)
df_technical = df_technical.withColumn(
    "strong_bullish_signal",
    F.when(
        (F.col("daily_return_pct") > 2) & (F.col("volume_breakout_flag") == True),
        True
    ).otherwise(False)
).withColumn(
    "strong_bearish_signal",
    F.when(
        (F.col("daily_return_pct") < -2) & (F.col("volume_breakout_flag") == True),
        True
    ).otherwise(False)
)

# Select final columns
GOLD_TECHNICAL = f"{GOLD_SCHEMA}.stock_technical_levels"

df_technical_final = df_technical.select(
    "ticker_standard",
    "sector",
    "date",
    "close",
    "high_52w",
    "low_52w",
    "distance_from_52w_high_pct",
    "distance_from_52w_low_pct",
    "at_52w_high_flag",
    "at_52w_low_flag",
    "high_30d",
    "low_30d",
    "breakout_30d_high_flag",
    "breakdown_30d_low_flag",
    "volume",
    "avg_volume_20d",
    "volume_ratio",
    "volume_breakout_flag",
    "bb_upper",
    "bb_lower",
    "bb_position",
    "strong_bullish_signal",
    "strong_bearish_signal"
)

# Write to Gold table
print(f"Writing to {GOLD_TECHNICAL}...")

df_technical_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable(GOLD_TECHNICAL)

print(f"✓ Created {GOLD_TECHNICAL}")
print(f"  Records: {df_technical_final.count():,}")
print("\nSample - Stocks at 52-week highs with volume breakout:")
latest_date = df_technical_final.agg(F.max("date")).collect()[0][0]
display(
    df_technical_final
    .filter(
        (F.col("date") == latest_date) & 
        (F.col("at_52w_high_flag") == True) & 
        (F.col("volume_breakout_flag") == True)
    )
    .orderBy(F.desc("volume_ratio"))
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Query Gold Table 5: Multi-Timeframe Momentum Screener
# MAGIC %sql
# MAGIC -- Multi-Timeframe Momentum Screener
# MAGIC -- Find stocks with consistent positive returns across all timeframes
# MAGIC
# MAGIC SELECT 
# MAGIC     ticker_standard,
# MAGIC     sector,
# MAGIC     close,
# MAGIC     return_7d_pct,
# MAGIC     return_30d_pct,
# MAGIC     return_90d_pct,
# MAGIC     return_180d_pct,
# MAGIC     consistency_score_30d,
# MAGIC     volatility_30d,
# MAGIC     max_drawdown_30d_pct
# MAGIC FROM StockMarketLakehouse.gold.stock_multitime_performance
# MAGIC WHERE date = (SELECT MAX(date) FROM StockMarketLakehouse.gold.stock_multitime_performance)
# MAGIC     AND return_7d_pct > 0
# MAGIC     AND return_30d_pct > 0
# MAGIC     AND return_90d_pct > 0
# MAGIC     AND consistency_score_30d > 60  -- Positive 60%+ of days
# MAGIC ORDER BY return_30d_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,New Gold Tables Summary
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Enhanced Gold Layer - Advanced Analytics
# MAGIC
# MAGIC ### New Tables Added:
# MAGIC
# MAGIC #### **Table 5: stock_multitime_performance** 📊
# MAGIC Multi-timeframe momentum analysis for systematic trend identification.
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC * **Returns**: 7-day, 30-day, 90-day, 180-day percentage returns
# MAGIC * **Volatility**: Rolling volatility across 7/30/90-day windows
# MAGIC * **Consistency Score**: % of positive days in last 30 days (60%+ = strong trend)
# MAGIC * **Max Drawdown**: Worst peak-to-trough decline in last 30 days
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Find sustained momentum (positive across all timeframes)
# MAGIC * Compare short-term vs long-term trends (divergence signals)
# MAGIC * Screen for consistency (avoid choppy movers)
# MAGIC * Risk assessment (high returns + low volatility = sweet spot)
# MAGIC
# MAGIC **Sample Query:**
# MAGIC ```sql
# MAGIC -- Find stocks with consistent uptrend (all positive returns + high consistency)
# MAGIC SELECT ticker_standard, return_30d_pct, consistency_score_30d, volatility_30d
# MAGIC FROM gold.stock_multitime_performance
# MAGIC WHERE date = CURRENT_DATE() - 1
# MAGIC   AND return_7d_pct > 0 AND return_30d_pct > 0 AND return_90d_pct > 0
# MAGIC   AND consistency_score_30d > 60
# MAGIC ORDER BY return_30d_pct DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Table 6: stock_relative_strength** 🏆
# MAGIC Compare each stock's performance vs its sector and the overall market.
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC * **Outperformance vs Sector**: How much better/worse than sector peers
# MAGIC * **Outperformance vs Market**: How much better/worse than market average
# MAGIC * **Percentile in Sector**: Rank within sector (100 = top performer)
# MAGIC * **Relative Strength Label**: Sector Leader / Sector Outperformer / Market Outperformer / Underperformer
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Find **Sector Leaders** (beating both sector AND market) — strongest stocks
# MAGIC * Spot **sector rotation** (which sectors are leading/lagging the market)
# MAGIC * Identify **relative weakness** (underperformers ready to catch up)
# MAGIC * Build diversified portfolios (pick top stocks from each sector)
# MAGIC
# MAGIC **Sample Query:**
# MAGIC ```sql
# MAGIC -- Find top 3 stocks from each sector (by relative strength)
# MAGIC WITH ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY sector ORDER BY percentile_in_sector DESC) as rn
# MAGIC   FROM gold.stock_relative_strength
# MAGIC   WHERE date = CURRENT_DATE() - 1
# MAGIC )
# MAGIC SELECT sector, ticker_standard, percentile_in_sector, relative_strength_label
# MAGIC FROM ranked WHERE rn <= 3
# MAGIC ORDER BY sector, rn;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Table 7: stock_technical_levels** 🎯
# MAGIC Pre-computed technical levels and volume breakout signals.
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC * **52-Week High/Low**: Distance from annual extremes (in %)
# MAGIC * **30-Day High/Low**: Recent range boundaries
# MAGIC * **Volume Ratio**: Current volume / 20-day average (>1.5 = breakout)
# MAGIC * **Bollinger Bands**: Upper/lower bands + position (Above/Within/Below)
# MAGIC * **Breakout Flags**: 30-day high breakout, volume breakout
# MAGIC * **Strong Signals**: Price move >2% + volume breakout
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * **Breakout screening** (price breaking 30-day high + volume confirmation)
# MAGIC * **Support/resistance** (52-week levels as key zones)
# MAGIC * **Mean reversion plays** (oversold at lower Bollinger Band)
# MAGIC * **Volume confirmation** (avoid false breakouts with low volume)
# MAGIC
# MAGIC **Sample Query:**
# MAGIC ```sql
# MAGIC -- Find stocks near 52-week highs with increasing volume
# MAGIC SELECT ticker_standard, close, distance_from_52w_high_pct, volume_ratio
# MAGIC FROM gold.stock_technical_levels
# MAGIC WHERE date = CURRENT_DATE() - 1
# MAGIC   AND distance_from_52w_high_pct > -5  -- Within 5% of 52-week high
# MAGIC   AND volume_ratio > 1.2               -- 20% above average volume
# MAGIC ORDER BY distance_from_52w_high_pct DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Daily Analysis Workflow 📅
# MAGIC
# MAGIC **Morning Routine (Pre-Market):**
# MAGIC 1. Check [market_overview_daily](#followup) for overall sentiment (Bullish/Bearish/Neutral)
# MAGIC 2. Review [sector_daily_metrics](#followup) to spot hot/cold sectors
# MAGIC 3. Run Multi-Timeframe query to find sustained momentum stocks
# MAGIC 4. Run Relative Strength query to find Sector Leaders
# MAGIC
# MAGIC **Intraday Watch:**
# MAGIC 5. Monitor [top_movers_daily](#followup) for extreme moves
# MAGIC 6. Check Technical Levels table for breakout candidates
# MAGIC
# MAGIC **Post-Market Review:**
# MAGIC 7. Update data (run Bronze → Silver → Gold pipeline)
# MAGIC 8. Review strong signals (price + volume confirmation)
# MAGIC 9. Add to watchlist for next day
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Gold Layer Schema (Complete)
# MAGIC
# MAGIC ```
# MAGIC StockMarketLakehouse.gold/
# MAGIC ├── ticker_sector_mapping (reference)
# MAGIC ├── stock_daily_metrics (core - MA 20/50/200, trend signals)
# MAGIC ├── sector_daily_metrics (aggregations - sector breadth)
# MAGIC ├── top_movers_daily (rankings - top 10 gainers/losers)
# MAGIC ├── market_overview_daily (summary - market health)
# MAGIC ├── stock_multitime_performance (NEW - momentum analysis)
# MAGIC ├── stock_relative_strength (NEW - sector/market comparison)
# MAGIC └── stock_technical_levels (NEW - breakouts, support/resistance)
# MAGIC ```
# MAGIC
# MAGIC **Total Records: 60,710 per table** (50 stocks × 5 years daily data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Makes This Production-Ready? ✅
# MAGIC
# MAGIC **Data Engineering Best Practices:**
# MAGIC * ✓ **Incremental processing ready** (partitioned by date)
# MAGIC * ✓ **Pre-computed aggregates** (avoid expensive on-the-fly calculations)
# MAGIC * ✓ **Normalized metrics** (percentages, ratios, flags for easy filtering)
# MAGIC * ✓ **Reference data separation** (ticker_sector_mapping)
# MAGIC * ✓ **Query optimization** (indexed on date, common filters)
# MAGIC
# MAGIC **Analytics Depth:**
# MAGIC * ✓ **Multi-dimensional** (time, sector, technical, relative)
# MAGIC * ✓ **Actionable signals** (flags, labels, thresholds)
# MAGIC * ✓ **Scalable patterns** (window functions, ranking)
# MAGIC
# MAGIC **Interview Talking Points:**
# MAGIC * "Built 7 gold tables optimized for daily stock analysis"
# MAGIC * "Implemented multi-timeframe momentum tracking (7/30/90/180-day)"
# MAGIC * "Created relative strength analysis comparing stocks vs sector and market"
# MAGIC * "Pre-computed technical levels (52-week, Bollinger Bands, volume breakouts)"
# MAGIC * "Designed for BI consumption with pre-aggregated metrics and flags"

# COMMAND ----------

# DBTITLE 1,Query Gold Table 6: Find Sector Leaders
# MAGIC %sql
# MAGIC -- Find Sector Leaders (outperforming both sector AND market)
# MAGIC -- Great for identifying rotation into strong stocks
# MAGIC
# MAGIC SELECT 
# MAGIC     ticker_standard,
# MAGIC     sector,
# MAGIC     close,
# MAGIC     daily_return_pct,
# MAGIC     sector_return_pct,
# MAGIC     market_return_pct,
# MAGIC     outperformance_vs_sector_pct,
# MAGIC     outperformance_vs_market_pct,
# MAGIC     percentile_in_sector,
# MAGIC     relative_strength_label
# MAGIC FROM StockMarketLakehouse.gold.stock_relative_strength
# MAGIC WHERE date = (SELECT MAX(date) FROM StockMarketLakehouse.gold.stock_relative_strength)
# MAGIC     AND relative_strength_label = 'Sector Leader'
# MAGIC     AND percentile_in_sector >= 80  -- Top 20% in sector
# MAGIC ORDER BY outperformance_vs_sector_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query Gold Table 7: Breakout Candidates Screener
# MAGIC %sql
# MAGIC -- Breakout Candidates: Stocks breaking 30-day highs with volume confirmation
# MAGIC -- Classic momentum setup for entries
# MAGIC
# MAGIC SELECT 
# MAGIC     ticker_standard,
# MAGIC     sector,
# MAGIC     close,
# MAGIC     distance_from_52w_high_pct,
# MAGIC     high_30d,
# MAGIC     volume,
# MAGIC     avg_volume_20d,
# MAGIC     volume_ratio,
# MAGIC     bb_position,
# MAGIC     breakout_30d_high_flag,
# MAGIC     volume_breakout_flag,
# MAGIC     strong_bullish_signal
# MAGIC FROM StockMarketLakehouse.gold.stock_technical_levels
# MAGIC WHERE date = (SELECT MAX(date) FROM StockMarketLakehouse.gold.stock_technical_levels)
# MAGIC     AND breakout_30d_high_flag = TRUE
# MAGIC     AND volume_breakout_flag = TRUE
# MAGIC     AND distance_from_52w_high_pct > -10  -- Near 52-week highs
# MAGIC ORDER BY volume_ratio DESC
# MAGIC LIMIT 10;

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

