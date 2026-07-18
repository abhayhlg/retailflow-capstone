-- ============================================================================
-- 1. STAGING SCHEMAS (Matches Glue Metadata exactly to prevent 15007 errors)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.stg_customer (
    customer_id VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    signup_date DATE,
    city VARCHAR(100),
    segment VARCHAR(100)
);

CREATE TABLE staging.stg_products (
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price DOUBLE PRECISION,  -- Matches Glue double
    active_flag BOOLEAN
);

CREATE TABLE staging.stg_orders (
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_ts TIMESTAMP,
    store_region VARCHAR(100),
    status VARCHAR(50),
    discount_code VARCHAR(50),
    order_total DOUBLE PRECISION,
    dataqualityevaluationresult VARCHAR(255)
);

CREATE TABLE staging.stg_order_items (
    order_id VARCHAR(100),
    product_id VARCHAR(100),
    quantity INT,
    unit_price DOUBLE PRECISION,  -- Matches Glue double
    line_total DOUBLE PRECISION,  -- Matches Glue double
    dataqualityevaluationresult VARCHAR(255)
);

-- ============================================================================
-- 2. PRODUCTION CORE SCHEMA (Optimized Star Schema Design)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- Dimension 1: Large Dimension - Key Distributed by Customer ID
CREATE TABLE analytics.dim_customer (
    customer_id BIGINT NOT NULL, -- Casted to numeric key for join performance
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    signup_date DATE,
    city VARCHAR(100),
    segment VARCHAR(100)
)
DISTSTYLE KEY DISTKEY (customer_id)
SORTKEY (customer_id);

-- Dimension 2: Small Dimension - Copied to all slices for localized joining
CREATE TABLE analytics.dim_product (
    product_id VARCHAR(100) NOT NULL, -- Business key kept as VARCHAR string
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price DECIMAL(10, 2), -- Casted to Decimal to avoid rounding issues
    active_flag BOOLEAN
)
DISTSTYLE ALL
SORTKEY (product_id);

-- Dimension 3: Time Dimension - Small Dimension
CREATE TABLE analytics.dim_date (
    date_key INT NOT NULL, -- YYYYMMDD format
    calendar_date DATE NOT NULL,
    day_of_week VARCHAR(15),
    calendar_month VARCHAR(15),
    calendar_year INT NOT NULL
)
DISTSTYLE ALL
SORTKEY (calendar_date);

-- Fact Table: Distributed on Customer ID to achieve collocation with dim_customer
CREATE TABLE analytics.fact_order_items (
    order_item_id INT IDENTITY(1,1),
    order_id VARCHAR(100) NOT NULL,
    customer_id BIGINT NOT NULL,  -- Collocated join key
    product_id VARCHAR(100) NOT NULL,
    date_key INT NOT NULL,        -- Sort key for time-series range restriction
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    line_total DECIMAL(12, 2) NOT NULL,
    order_ts TIMESTAMP,
    store_region VARCHAR(100)
)
DISTSTYLE KEY DISTKEY (customer_id)
SORTKEY (date_key);

-- ============================================================================
-- 3. AMAZON REDSHIFT SPECTRUM (External Schema over Gold)
-- ============================================================================
CREATE EXTERNAL SCHEMA spectrum_gold
FROM DATA CATALOG
DATABASE 'retailflow_gold_db'
IAM_ROLE 'arn:aws:iam::988640945996:role/RedshiftServerlessS3SpectrumRole';

CREATE EXTERNAL TABLE spectrum_gold.ext_daily_revenue (
    order_date DATE,
    total_revenue DECIMAL(12, 2))
STORED AS PARQUET
LOCATION 's3://retailflow-abhay/consumption/daily_revenue/';

-- ============================================================================
-- 4. MATERIALIZED VIEW (Daily Revenue by Category with Auto-Refresh)
-- ============================================================================
CREATE MATERIALIZED VIEW analytics.mv_daily_revenue_by_category
AUTO REFRESH YES
AS
SELECT 
    d.calendar_date AS order_date,
    p.category AS product_category,
    SUM(f.line_total) AS daily_revenue,
    SUM(f.quantity) AS total_items_sold
FROM analytics.fact_order_items f
JOIN analytics.dim_date d ON f.date_key = d.date_key
JOIN analytics.dim_product p ON f.product_id = p.product_id
GROUP BY d.calendar_date, p.category;