-- ==============================================================================
-- NIFTY 50 TICKER CONFIGURATION TABLE
-- Purpose: Store ticker metadata for dynamic ingestion
-- Author: Data Engineering Team
-- Date: 2026-07-24
-- ==============================================================================

-- Step 1: Create config schema for metadata tables
CREATE SCHEMA IF NOT EXISTS StockMarketLakehouse.config
COMMENT 'Configuration and metadata tables for the lakehouse';

-- Step 2: Create ticker configuration table
CREATE OR REPLACE TABLE StockMarketLakehouse.config.nifty50_tickers (
    ticker_id INT COMMENT 'Unique identifier for the ticker',
    ticker_symbol STRING COMMENT 'Yahoo Finance ticker symbol (with .NS suffix)',
    ticker_clean STRING COMMENT 'Clean ticker name without suffix',
    company_name STRING COMMENT 'Company full name',
    sector STRING COMMENT 'Industry sector',
    is_active BOOLEAN COMMENT 'Whether ticker is currently active for ingestion',
    added_date DATE COMMENT 'Date when ticker was added to the list',
    last_updated TIMESTAMP COMMENT 'Last time this record was updated'
)
COMMENT 'Nifty 50 ticker configuration for stock market data ingestion'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'gold',
    'domain' = 'configuration'
);

-- Step 3: Insert all Nifty 50 tickers with metadata
INSERT INTO StockMarketLakehouse.config.nifty50_tickers VALUES
-- Financial Services Sector
(1, 'HDFCBANK.NS', 'HDFCBANK', 'HDFC Bank Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(2, 'ICICIBANK.NS', 'ICICIBANK', 'ICICI Bank Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(3, 'KOTAKBANK.NS', 'KOTAKBANK', 'Kotak Mahindra Bank Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(4, 'AXISBANK.NS', 'AXISBANK', 'Axis Bank Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(5, 'SBIN.NS', 'SBIN', 'State Bank of India', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(6, 'BAJFINANCE.NS', 'BAJFINANCE', 'Bajaj Finance Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(7, 'BAJAJFINSV.NS', 'BAJAJFINSV', 'Bajaj Finserv Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(8, 'SBILIFE.NS', 'SBILIFE', 'SBI Life Insurance Company Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(9, 'HDFCLIFE.NS', 'HDFCLIFE', 'HDFC Life Insurance Company Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(10, 'INDUSINDBK.NS', 'INDUSINDBK', 'IndusInd Bank Limited', 'Financial Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- IT Services Sector
(11, 'TCS.NS', 'TCS', 'Tata Consultancy Services Limited', 'IT Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(12, 'INFY.NS', 'INFY', 'Infosys Limited', 'IT Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(13, 'HCLTECH.NS', 'HCLTECH', 'HCL Technologies Limited', 'IT Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(14, 'WIPRO.NS', 'WIPRO', 'Wipro Limited', 'IT Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(15, 'TECHM.NS', 'TECHM', 'Tech Mahindra Limited', 'IT Services', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Oil & Gas Sector
(16, 'RELIANCE.NS', 'RELIANCE', 'Reliance Industries Limited', 'Oil & Gas', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(17, 'ONGC.NS', 'ONGC', 'Oil and Natural Gas Corporation Limited', 'Oil & Gas', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(18, 'BPCL.NS', 'BPCL', 'Bharat Petroleum Corporation Limited', 'Oil & Gas', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- FMCG Sector
(19, 'HINDUNILVR.NS', 'HINDUNILVR', 'Hindustan Unilever Limited', 'FMCG', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(20, 'ITC.NS', 'ITC', 'ITC Limited', 'FMCG', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(21, 'NESTLEIND.NS', 'NESTLEIND', 'Nestle India Limited', 'FMCG', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(22, 'BRITANNIA.NS', 'BRITANNIA', 'Britannia Industries Limited', 'FMCG', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(23, 'TATACONSUM.NS', 'TATACONSUM', 'Tata Consumer Products Limited', 'FMCG', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Pharma Sector
(24, 'SUNPHARMA.NS', 'SUNPHARMA', 'Sun Pharmaceutical Industries Limited', 'Pharma', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(25, 'DRREDDY.NS', 'DRREDDY', 'Dr. Reddys Laboratories Limited', 'Pharma', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(26, 'CIPLA.NS', 'CIPLA', 'Cipla Limited', 'Pharma', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(27, 'DIVISLAB.NS', 'DIVISLAB', 'Divis Laboratories Limited', 'Pharma', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(28, 'APOLLOHOSP.NS', 'APOLLOHOSP', 'Apollo Hospitals Enterprise Limited', 'Healthcare', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Auto Sector
(29, 'MARUTI.NS', 'MARUTI', 'Maruti Suzuki India Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(30, 'TATAMOTORS.NS', 'TATAMOTORS', 'Tata Motors Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(31, 'M&M.NS', 'M&M', 'Mahindra & Mahindra Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(32, 'BAJAJ-AUTO.NS', 'BAJAJ-AUTO', 'Bajaj Auto Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(33, 'EICHERMOT.NS', 'EICHERMOT', 'Eicher Motors Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(34, 'HEROMOTOCO.NS', 'HEROMOTOCO', 'Hero MotoCorp Limited', 'Auto', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Metals & Mining Sector
(35, 'TATASTEEL.NS', 'TATASTEEL', 'Tata Steel Limited', 'Metals & Mining', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(36, 'HINDALCO.NS', 'HINDALCO', 'Hindalco Industries Limited', 'Metals & Mining', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(37, 'JSWSTEEL.NS', 'JSWSTEEL', 'JSW Steel Limited', 'Metals & Mining', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(38, 'COALINDIA.NS', 'COALINDIA', 'Coal India Limited', 'Metals & Mining', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Cement Sector
(39, 'ULTRACEMCO.NS', 'ULTRACEMCO', 'UltraTech Cement Limited', 'Cement', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(40, 'SHREECEM.NS', 'SHREECEM', 'Shree Cement Limited', 'Cement', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(41, 'GRASIM.NS', 'GRASIM', 'Grasim Industries Limited', 'Cement', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Telecom Sector
(42, 'BHARTIARTL.NS', 'BHARTIARTL', 'Bharti Airtel Limited', 'Telecom', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Power Sector
(43, 'NTPC.NS', 'NTPC', 'NTPC Limited', 'Power', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(44, 'POWERGRID.NS', 'POWERGRID', 'Power Grid Corporation of India Limited', 'Power', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Diversified Sector
(45, 'LT.NS', 'LT', 'Larsen & Toubro Limited', 'Engineering & Construction', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(46, 'ADANIPORTS.NS', 'ADANIPORTS', 'Adani Ports and Special Economic Zone Limited', 'Infrastructure', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(47, 'ADANIENT.NS', 'ADANIENT', 'Adani Enterprises Limited', 'Diversified', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),

-- Others
(48, 'ASIANPAINT.NS', 'ASIANPAINT', 'Asian Paints Limited', 'Consumer Goods', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(49, 'TITAN.NS', 'TITAN', 'Titan Company Limited', 'Consumer Goods', true, CURRENT_DATE(), CURRENT_TIMESTAMP()),
(50, 'UPL.NS', 'UPL', 'UPL Limited', 'Agrochemicals', true, CURRENT_DATE(), CURRENT_TIMESTAMP());

-- Step 4: Verify data inserted successfully
SELECT 
    sector,
    COUNT(*) as ticker_count
FROM StockMarketLakehouse.config.nifty50_tickers
WHERE is_active = true
GROUP BY sector
ORDER BY ticker_count DESC;

-- ==============================================================================
-- USAGE EXAMPLES
-- ==============================================================================

-- Example 1: Get all active tickers for ingestion
SELECT ticker_symbol 
FROM StockMarketLakehouse.config.nifty50_tickers 
WHERE is_active = true
ORDER BY ticker_id;

-- Example 2: Get tickers by sector
SELECT ticker_symbol, company_name
FROM StockMarketLakehouse.config.nifty50_tickers
WHERE sector = 'IT Services' AND is_active = true;

-- Example 3: Get ticker count by sector
SELECT 
    sector,
    COUNT(*) as count,
    COUNT(CASE WHEN is_active THEN 1 END) as active_count
FROM StockMarketLakehouse.config.nifty50_tickers
GROUP BY sector
ORDER BY count DESC;

-- Example 4: Use in Python (to be added to Bronze notebook)
/*
# Python code to read tickers dynamically:
tickers_df = spark.table("StockMarketLakehouse.config.nifty50_tickers") \
    .filter("is_active = true") \
    .select("ticker_symbol") \
    .orderBy("ticker_id")

NIFTY_50_TICKERS = [row.ticker_symbol for row in tickers_df.collect()]
print(f"Loaded {len(NIFTY_50_TICKERS)} active tickers from config table")
*/

-- ==============================================================================
-- MAINTENANCE OPERATIONS
-- ==============================================================================

-- Deactivate a ticker (if delisted or no longer needed)
-- UPDATE StockMarketLakehouse.config.nifty50_tickers 
-- SET is_active = false, last_updated = CURRENT_TIMESTAMP()
-- WHERE ticker_symbol = 'EXAMPLE.NS';

-- Add a new ticker
-- INSERT INTO StockMarketLakehouse.config.nifty50_tickers VALUES
-- (51, 'NEWTICKER.NS', 'NEWTICKER', 'New Company Name', 'Sector', true, CURRENT_DATE(), CURRENT_TIMESTAMP());

-- View table history (if Change Data Feed is enabled)
-- DESCRIBE HISTORY StockMarketLakehouse.config.nifty50_tickers;