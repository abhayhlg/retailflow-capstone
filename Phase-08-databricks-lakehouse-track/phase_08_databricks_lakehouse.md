# Phase 8: Databricks Lakehouse Track Reference Guide

This document provides complete, production-ready implementation blueprints, cluster deployment logs, architectural workflows, and governance controls for **Phase 8: Databricks Lakehouse Track**.

---

## Deliverable 1: Cluster, Repo & Unity Catalog Setup

### Task 42: Compute and Git Integration Setup

#### 1. Create an All-Purpose Cluster
1. Log in to your Databricks Workspace (`[your-workspace].cloud.databricks.com`).
2. In the sidebar, click **Compute** and then click **Create compute**.
3. In the **Performance** settings configuration, apply the following parameters:
   * **Cluster mode:** Single node (this automatically configures `spark.master local[*]`).
   * **Databricks Runtime version:** Select a current standard long-term support release, such as **15.4 LTS (Scala 2.12, Spark 3.5.0)**.
   * **Use Photon Acceleration:** Check this box to optimize processing execution.
   * **Node type:** Select `m5.large` (2 vCPUs, 8 GB Memory) or `i3.xlarge` if high-performance NVMe storage backing is needed.
   * **Autoscaling:** This is automatically disabled and grayed out under Single Node mode.
4. Expand **Advanced options**, click the **Instances** tab, and set **Terminate after** to `30` minutes of inactivity to control idle cloud costs.
5. Click **Create compute**.

#### 2. Configure Databricks Repos / Git Folders
1. In the sidebar, click **Workspace** -> **Repos** (or **Git Folders**).
2. Click **Add Repo** (or **New Git Folder**).
3. Enter your Git repository URL: `https://github.com/yourorg/retailflow`.
4. Select your Git provider from the dropdown.
5. Leave the link name as `retailflow`. Click **Create**.
6. *Note on Credentials:* If prompted, navigate to **User Settings** -> **Linked accounts**, select your Git provider, and enter your Personal Access Token (PAT) with `repo` scopes.

---

### Task 47: AWS IAM Integration & Unity Catalog Object Hierarchy

#### 1. AWS IAM Role and Policy Configuration
To connect securely without using legacy instance profiles or exposing static AWS access keys, configure an AWS IAM role that trusts the Databricks AWS root account.

Log in to your AWS Management Console for Account ID `988640945996` and create the following resources:

##### Step A: Create IAM Policy (`DatabricksUC-S3-Policy`)
Navigate to **IAM** -> **Policies** -> **Create Policy**. Switch to the JSON tab and paste the following policy configuration to grant access to your target S3 bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "StorageAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::retailflow-abhay",
        "arn:aws:s3:::retailflow-abhay/*"
      ]
    }
  ]
}
```

##### Step B: Create IAM Trust Role (`DatabricksUC-Storage-Role`)
1. Navigate to **IAM** -> **Roles** -> **Create Role**.
2. Select **Custom trust policy** and paste the trust relationship below. Replace `063544620592` with your specific Databricks control plane AWS account ID if it differs by region, and enter your unique Databricks Account ID as the external ID string:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::063544620592:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "databricks-account-id-placeholder"
        }
      }
    }
  ]
}
```
3. Attach the `DatabricksUC-S3-Policy` created in Step A to this role.
4. Save the role and record its ARN: `arn:aws:iam::988640945996:role/DatabricksUC-Storage-Role`.

#### 2. Create Storage Credential and External Location in Databricks UI
1. In your Databricks workspace, switch to the **Catalog** persona using the sidebar.
2. Scroll down on the left navigation and click **External Locations** -> **Storage Credentials** -> **Create credential**.
3. **Credential name:** `retailflow_storage_credential`
4. **Access type:** IAM Role ARN
5. **Role ARN:** `arn:aws:iam::988640945996:role/DatabricksUC-Storage-Role`
6. Click **Create**.
7. Now, select the **External Locations** tab directly above and click **Create location**.
8. **External location name:** `retailflow_s3_root`
9. **URL:** `s3://retailflow-abhay/`
10. **Storage Credential:** Select `retailflow_storage_credential`.
11. Click **Create**.

#### 3. Establish the 3-Level Namespace Catalog and Schemas
Run the following SQL commands within a Databricks SQL Warehouse or an all-purpose cluster notebook to create your database structure, using the secure external location for root storage:

```sql
-- Create the top-level Unity Catalog
CREATE CATALOG IF NOT EXISTS retailflow_catalog
MANAGED LOCATION 's3://retailflow-abhay/consumption/';

USE CATALOG retailflow_catalog;

-- Create isolated environments/schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

#### 4. Verifying Lineage in the UI
Once tables are populated by the notebooks in the subsequent phases:
1. Navigate to **Catalog Explorer**, find your table under `retailflow_catalog.silver` or `retailflow_catalog.gold`.
2. Click the **Lineage** tab next to the Schema UI definition.
3. Click **Lineage Graph** to view a visual map tracking data flow upstream to Bronze sources and downstream to Gold tables. This tracking happens automatically across all notebooks running on Unity Catalog-compliant compute clusters.

---

## Deliverable 2: Bronze Layer Notebooks (.py)

### Task 43: Ingesting Raw Files to Managed Bronze Tables
*Understanding Step 43:* External locations provide a secure handshake between Unity Catalog and AWS S3. By using an external location path like `s3://retailflow-abhay/raw/...`, Unity Catalog references your configured IAM role in the background. This allows your Spark code to safely read raw source files and write them into managed Delta tables without needing legacy instance profiles, environment keys, or plain-text credentials.

Create a notebook named `01_bronze_ingestion.py` in your repository path:

```python
# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retailflow_catalog;

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, BooleanType, IntegerType

# Define strict target schemas to prevent parsing drift
customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("active_flag", BooleanType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_ts", StringType(), True),
    StructField("store_region", StringType(), True),
    StructField("status", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])

# Define S3 source paths routed through the validated External Location
bucket = "retailflow-abhay"
sources = {
    "customers": (f"s3://{bucket}/raw/customers/", "csv", customers_schema, {"header": "true"}),
    "products": (f"s3://{bucket}/raw/products/", "csv", products_schema, {"header": "true"}),
    "orders": (f"s3://{bucket}/raw/orders/", "json", orders_schema, {}),
    "order_items": (f"s3://{bucket}/raw/order_items/", "json", order_items_schema, {})
}

# Read raw files and save them as managed Delta tables in the Bronze schema
for table_name, (path, fmt, schema, opts) in sources.items():
    print(f"Processing raw source for bronze.{table_name} from {path}...")
    
    df_reader = spark.read.format(fmt).schema(schema)
    for key, value in opts.items():
        df_reader = df_reader.option(key, value)
        
    df = df_reader.load(path)
    
    # Save as a managed Unity Catalog Delta table
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(f"bronze.{table_name}")
    
    print(f"Successfully populated table: retailflow_catalog.bronze.{table_name}")
```

---

### Task 44: Clickstream Stream Ingestion via Auto Loader
Create a notebook named `02_bronze_clickstream_autoloader.py` in your repository path:

```python
# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retailflow_catalog;

# COMMAND ----------
# Configure streaming input and checkpoint coordinates
bucket = "retailflow-abhay"
source_landing_zone = f"s3://{bucket}/raw/"
checkpoint_dir = f"s3://{bucket}/checkpoints/bronze_clickstream/"

# Auto Loader stream setup targeting day1 and day2 JSON files
clickstream_stream = spark.readStream     .format("cloudFiles")     .option("cloudFiles.format", "json")     .option("cloudFiles.useNotifications", "false")     .option("cloudFiles.schemaLocation", f"s3://{bucket}/schemas/bronze_clickstream/")     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     .load(source_landing_zone)

# Write stream out to a managed Bronze Delta table
query = clickstream_stream.writeStream     .format("delta")     .outputMode("append")     .option("checkpointLocation", checkpoint_dir)     .trigger(availableNow=True)     .toTable("bronze.clickstream")

query.awaitTermination()
print("Clickstream processing batch sequence complete.")
```

---

## Deliverable 3: Silver & Gold Layer Notebooks (.py)

### Task 45: Silver and Gold Transformations with Change Data Feed Validation
Create a notebook named `03_silver_gold_processing.py` in your repository path:

```python
# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retailflow_catalog;

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- SILVER LAYER TRANSFORMATIONS ---

# 1. Clean and Deduplicate Customers
df_cust = spark.read.table("bronze.customers")
window_cust = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc())

df_cust_clean = df_cust     .filter(F.col("customer_id").isNotNull())     .withColumn("rn", F.row_number().over(window_cust))     .filter(F.col("rn") == 1)     .drop("rn")     .withColumn("email", F.lower(F.trim(F.col("email"))))     .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))

df_cust_clean.write.format("delta").mode("overwrite")     .option("delta.enableChangeDataFeed", "true")     .saveAsTable("silver.customers")

# 2. Clean and Deduplicate Products
df_prod = spark.read.table("bronze.products")
window_prod = Window.partitionBy("product_id").orderBy(F.col("product_name"))

df_prod_clean = df_prod     .filter(F.col("product_id").isNotNull())     .withColumn("rn", F.row_number().over(window_prod))     .filter(F.col("rn") == 1)     .drop("rn")     .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.lit(0.0)))     .withColumn("active_flag", F.coalesce(F.col("active_flag"), F.lit(True)))

df_prod_clean.write.format("delta").mode("overwrite")     .option("delta.enableChangeDataFeed", "true")     .saveAsTable("silver.products")

# 3. Clean and Standardize Orders
df_ord = spark.read.table("bronze.orders")
df_ord_clean = df_ord     .filter(F.col("order_id").isNotNull())     .dropDuplicates(["order_id"])     .withColumn("order_ts", F.to_timestamp(F.col("order_ts")))     .withColumn("status", F.coalesce(F.col("status"), F.lit("UNKNOWN")))

df_ord_clean.write.format("delta").mode("overwrite")     .option("delta.enableChangeDataFeed", "true")     .saveAsTable("silver.orders")

# 4. Clean and Cast Order Items
df_items = spark.read.table("bronze.order_items")
df_items_clean = df_items     .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())     .dropDuplicates(["order_id", "product_id"])     .withColumn("quantity", F.when(F.col("quantity") < 0, 0).otherwise(F.col("quantity")))     .withColumn("line_total", F.col("quantity") * F.col("unit_price"))

df_items_clean.write.format("delta").mode("overwrite")     .option("delta.enableChangeDataFeed", "true")     .saveAsTable("silver.order_items")


# --- GOLD LAYER TRANSFORMATIONS ---

# Join datasets to build a business-ready aggregate table
orders_fact = spark.read.table("silver.orders")
items_dim = spark.read.table("silver.order_items")
products_dim = spark.read.table("silver.products")

df_gold_source = orders_fact     .join(items_dim, "order_id", "inner")     .join(products_dim, "product_id", "inner")

df_gold_agg = df_gold_source     .withColumn("order_date", F.to_date(F.col("order_ts")))     .groupBy("order_date", "category", "store_region")     .agg(
        F.sum("line_total").alias("daily_revenue"),
        F.sum("quantity").alias("total_items_sold"),
        F.countDistinct("order_id").alias("total_order_count")
    )

df_gold_agg.write.format("delta").mode("overwrite").saveAsTable("gold.daily_regional_revenue")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Change Data Feed Validation Check

# COMMAND ----------
# Query the Change Data Feed to verify changes are captured
cdf_df = spark.read.format("delta")     .option("readChangeFeed", "true")     .option("startingVersion", 0)     .table("silver.customers")

display(cdf_df.select("_change_type", "customer_id", "email", "_commit_version").limit(5))
```

---

## Deliverable 4: Delta Live Tables Pipeline (.py + pipeline JSON)

### Task 46: Declarative Python DLT Specification
Create a pipeline script file named `04_dlt_silver_pipeline.py` in your repository path:

```python
import dlt
from pyspark.sql import functions as F

@dlt.view(name="bronze_customers_v")
def bronze_customers_v():
    return spark.read.table("retailflow_catalog.bronze.customers")

@dlt.view(name="bronze_order_items_v")
def bronze_order_items_v():
    return spark.read.table("retailflow_catalog.bronze.order_items")

# Silver Customer Processing with Warn and Drop Expectations
@dlt.table(
    name="silver_customers",
    comment="Cleansed customer records with email format validation and identity checks.",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email_format", "email LIKE '%_@__%.__%'")
def silver_customers():
    return dlt.read("bronze_customers_v")         .dropDuplicates(["customer_id"])         .withColumn("email", F.lower(F.trim(F.col("email"))))

# Silver Order Items Processing with a Fail Expectation
@dlt.table(
    name="silver_order_items",
    comment="Enforces logical quantity controls on individual item rows.",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_fail("positive_quantity_enforced", "quantity > 0")
def silver_order_items():
    return dlt.read("bronze_order_items_v")         .dropDuplicates(["order_id", "product_id"])
```

#### Complete DLT Pipeline Settings Configuration JSON
To run this pipeline, navigate to **Delta Live Tables** -> **Create pipeline** in the UI, switch to the JSON editor tab, and save the following configuration structure:

```json
{
    "id": "dlt-retailflow-pipeline-id",
    "name": "retailflow_silver_pipeline",
    "storage": "s3://retailflow-abhay/consumption/dlt_storage",
    "configuration": {
        "pipelines.channel": "current"
    },
    "clusters": [
        {
            "label": "default",
            "node_type_id": "m5.large",
            "driver_node_type_id": "m5.large",
            "num_workers": 1,
            "spark_conf": {
                "spark.master": "local[*]"
            }
        }
    ],
    "libraries": [
        {
            "notebook": {
                "path": "/Workspace/Repos/yourorg/retailflow/04_dlt_silver_pipeline"
            }
        }
    ],
    "target": "silver",
    "continuous": false,
    "development": true,
    "catalog": "retailflow_catalog",
    "photon": true
}
```

> **Tier Variance Notice:** Delta Live Tables requires a **Premium** or **Enterprise** Databricks workspace tier. The standard basic tier does not support managed declarative pipeline runtimes.

---

## Deliverable 5: Governance (Unity Catalog Masking)

### Task 47 (masking): Dynamic Column-Level Masking Enforcement
Run the following SQL commands to build a dynamic masking function and apply it to the customer table. This restricts plain-text visibility based on account group membership.

```sql
USE CATALOG retailflow_catalog;

-- 1. Define the dynamic masking function logic
CREATE OR REPLACE FUNCTION silver.email_mask_fn(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('data_admins') THEN email
  ELSE regexp_replace(email, '(?<=.).(?=.*@)', '*')
END;

-- 2. Bind the function rule to the target silver column
ALTER TABLE silver.customers ALTER COLUMN email SET MASK silver.email_mask_fn;
```

#### Masking Behavior Verification Queries

##### Executed by a user in the `data_admins` group:
```sql
SELECT customer_id, first_name, email FROM retailflow_catalog.silver.customers LIMIT 1;
-- Output shows unmasked data:
-- 3862 | Abhay | abhay.analytics@domain.com
```

##### Executed by a general analyst or user outside the `data_admins` group:
```sql
SELECT customer_id, first_name, email FROM retailflow_catalog.silver.customers LIMIT 1;
-- Output shows obfuscated data:
-- 3862 | Abhay | a*****************s@domain.com
```

---

## Deliverable 6: Delta Sharing

### Task 48: Open Data Sharing Architecture
Run the following SQL setup commands to expose your Gold table to an external business partner without copying data:

```sql
-- 1. Create an isolated open data share asset
CREATE SHARE IF NOT EXISTS retailflow_external_share
COMMENT 'Exposes verified aggregation logs to external partner organizations';

-- 2. Register the targeted gold table into the share envelope
ALTER SHARE retailflow_external_share ADD TABLE retailflow_catalog.gold.daily_regional_revenue
COMMENT 'Aggregated regional store sales volume analytics tracking';

-- 3. Create a unique consumer profile profile for the recipient
CREATE RECIPIENT IF NOT EXISTS partner_analytics_recipient
COMMENT 'External analytics vendor account endpoint access token identifier';
```

#### Databricks UI Walkthrough:
1. Navigate to the **Catalog** explorer screen via the sidebar interface.
2. Expand the **Delta Sharing** dropdown selection and click **Shared by me**.
3. Select `retailflow_external_share` to confirm that the `daily_regional_revenue` table is listed.
4. Click the **Recipients** tab and select `partner_analytics_recipient`.
5. Click **Activation Link** to generate a secure credential file URL. Download and send this `.share` configuration file to your partner through a secure channel. They can use this file to read the data directly using tools like Pandas, PowerBI, or an external Apache Spark environment.

> **Tier Variance Notice:** Delta Sharing requires a **Premium** or **Enterprise** workspace tier to manage active data recipient links.

---

## Deliverable 7: Workflow Orchestration (Workflow JSON)

### Task 49: End-to-End Orchestration Definition
The following JSON defines a three-task Databricks Workflow. It runs the Auto Loader ingest stream, runs the Delta Live Tables pipeline to clean the data, and finishes by executing the Gold aggregation notebook.

Save this JSON content to a file named `workflow.json` and upload it via the **Workflows** -> **Create Job** -> **Advanced (JSON)** UI window:

```json
{
  "name": "RetailFlow_Data_Pipeline_Job",
  "tasks": [
    {
      "task_key": "Ingest_Clickstream_AutoLoader",
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/yourorg/retailflow/02_bronze_clickstream_autoloader",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "0123-your-all-purpose-cluster-id",
      "retry_policy": {
        "max_retries": 2,
        "interval_seconds": 120,
        "backoff_matrix_multiplier": 2.0
      }
    },
    {
      "task_key": "Run_DLT_Silver_Pipeline",
      "depends_on": [
        {
          "task_key": "Ingest_Clickstream_AutoLoader"
        }
      ],
      "pipeline_task": {
        "pipeline_id": "dlt-retailflow-pipeline-id",
        "full_refresh": false
      },
      "retry_policy": {
        "max_retries": 1,
        "interval_seconds": 60,
        "backoff_matrix_multiplier": 1.0
      }
    },
    {
      "task_key": "Generate_Gold_Aggregations",
      "depends_on": [
        {
          "task_key": "Run_DLT_Silver_Pipeline"
        }
      ],
      "notebook_task": {
        "notebook_path": "/Workspace/Repos/yourorg/retailflow/03_silver_gold_processing",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "0123-your-all-purpose-cluster-id",
      "retry_policy": {
        "max_retries": 1,
        "interval_seconds": 60,
        "backoff_matrix_multiplier": 1.0
      }
    }
  ],
  "notification_settings": {
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "email_notifications": {
    "on_failure": [
      "abhay.ops@yourcompany.com"
    ]
  }
}
```

---

## Deliverable 8: SQL Warehouse & Dashboard

### Task 50: Business Intelligence Configuration

#### 1. Provision a Serverless or Pro SQL Warehouse
1. In the sidebar, select the **SQL Warehouse** option tab.
2. Click **Create SQL Warehouse**.
3. **Name:** `RetailFlow_Analytics_Warehouse`
4. **Cluster size:** 2X-Small (designed to run light dashboard analytics workloads at lower costs).
5. **Type:** Select **Serverless** for fast startup times (under 5 seconds). If serverless features are unavailable in your region, select **Pro**.
6. Set **Auto-stop** to `10` minutes to automatically shut down the warehouse when idle.
7. Click **Create**.

#### 2. Analytical SQL Core for Dashboard Tiles
Create a new SQL notebook or use the SQL Editor attached to `RetailFlow_Analytics_Warehouse` to run the following dashboard queries:

##### Tile 1: Daily Revenue Trend Over Time
```sql
SELECT 
  order_date, 
  SUM(daily_revenue) AS global_revenue
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY order_date
ORDER BY order_date ASC;
```

##### Tile 2: Total Revenue Broken Down by Product Category
```sql
SELECT 
  category, 
  SUM(daily_revenue) AS categorical_revenue
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY category
ORDER BY categorical_revenue DESC;
```

##### Tile 3: Top Operational Regions Ranked by Volume Sales
```sql
SELECT 
  store_region, 
  SUM(daily_revenue) AS regional_revenue,
  SUM(total_order_count) AS aggregate_orders
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY store_region
ORDER BY regional_revenue DESC;
```

##### Tile 4: Order Count Key Performance Metric Summary
```sql
SELECT 
  SUM(total_order_count) AS total_processed_orders
FROM retailflow_catalog.gold.daily_regional_revenue;
```

#### 3. Setting Up a KPI Threshold Alert
1. In the sidebar, navigate to **SQL Editor** -> **Alerts** and click **Create Alert**.
2. **Query:** Select the query you created for **Tile 4** (or create a dedicated query checking revenue for the current date).
3. **Trigger condition rule setup:**
   * **Value Column:** `total_processed_orders`
   * **Operator:** Less Than (`<`)
   * **Threshold Value:** `150` (adjust this number based on your target minimal operational threshold).
4. **When triggered, send notification to:** `abhay.ops@yourcompany.com`.
5. Set the check schedule interval frequency to run **Every 1 Day** at a specific time (e.g., `18:00`). This ensures your team is automatically notified if daily order volume drops below expected minimum levels.
