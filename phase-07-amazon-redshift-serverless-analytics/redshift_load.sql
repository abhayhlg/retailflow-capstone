-- ============================================================================
-- 1. STAGING INGESTION VIA S3 MANIFEST FILES
-- ============================================================================
TRUNCATE TABLE staging.stg_customer;
TRUNCATE TABLE staging.stg_products;
TRUNCATE TABLE staging.stg_orders;
TRUNCATE TABLE staging.stg_order_items;

COPY staging.stg_customer
FROM 's3://retailflow-abhay/curated/customers/customers_manifest.json'
IAM_ROLE 'arn:aws:iam::988640945996:role/RedshiftServerlessS3SpectrumRole'
FORMAT AS PARQUET
MANIFEST;

COPY staging.stg_products
--FROM 's3://retailflow-abhay/curated/products/products_manifest.json'
FROM 's3://retailflow-abhay/curated/products/'
IAM_ROLE 'arn:aws:iam::988640945996:role/RedshiftServerlessS3SpectrumRole'
FORMAT AS PARQUET;

COPY staging.stg_orders
--FROM 's3://retailflow-abhay/curated/orders/orders_manifest.json'
FROM 's3://retailflow-abhay/curated/orders/'
IAM_ROLE 'arn:aws:iam::988640945996:role/RedshiftServerlessS3SpectrumRole'
FORMAT AS PARQUET;

COPY staging.stg_order_items
--FROM 's3://retailflow-abhay/curated/order_items/order_items_manifest.json'
FROM 's3://retailflow-abhay/curated/order_items/'
IAM_ROLE 'arn:aws:iam::988640945996:role/RedshiftServerlessS3SpectrumRole'
FORMAT AS PARQUET;

-- ============================================================================
-- 2. ELT PIPELINE TRANSLATION (Staging to Core Schema Ingestion)
-- ============================================================================

-- Load Customer Dimension (Casting text customer_id safely to BIGINT)
INSERT INTO analytics.dim_customer (customer_id, first_name, last_name, email, signup_date, city, segment)
SELECT 
    CAST(customer_id AS BIGINT),
    first_name,
    last_name,
    email,
    signup_date,
    city,
    segment
FROM staging.stg_customer;

-- Load Product Dimension (Safely mapping double precision values to precise Decimals)
INSERT INTO analytics.dim_product (product_id, product_name, category, unit_price, active_flag)
SELECT 
    product_id,
    product_name,
    category,
    CAST(unit_price AS DECIMAL(10,2)),
    active_flag
FROM staging.stg_products;

-- Load Fact Table (Denormalizing and transforming data tracking keys)
INSERT INTO analytics.fact_order_items (order_id, customer_id, product_id, date_key, quantity, unit_price, line_total, order_ts, store_region)
SELECT 
    oi.order_id,
    CAST(o.customer_id AS BIGINT),
    oi.product_id,
    CAST(TO_CHAR(o.order_ts, 'YYYYMMDD') AS INT) AS date_key,
    oi.quantity,
    CAST(oi.unit_price AS DECIMAL(10,2)),
    CAST(oi.line_total AS DECIMAL(12,2)),
    o.order_ts,
    o.store_region
FROM staging.stg_order_items oi
JOIN staging.stg_orders o ON oi.order_id = o.order_id
WHERE o.dataqualityevaluationresult = 'Passed'; -- Ingesting clean files exclusively

-- ============================================================================
-- 3. PRODUCTION ERROR HANDLING DEMONSTRATION
-- ============================================================================

-- Query to audit structural CSV/JSON copy bugs
SELECT 
    query, starttime, filename, line_number, colname, err_reason 
FROM stl_load_errors 
ORDER BY starttime DESC 
LIMIT 5;

-- Query to catch modern Redshift Serverless spectrum/parquet scan type schema mismatches
SELECT 
    query_id, start_time, s3_uri, error_code, error_message 
FROM sys_load_error_detail 
ORDER BY start_time DESC 
LIMIT 5;


