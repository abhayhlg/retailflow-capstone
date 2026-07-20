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

df_cust_clean = df_cust \
    .filter(F.col("customer_id").isNotNull()) \
    .withColumn("rn", F.row_number().over(window_cust)) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .withColumn("email", F.lower(F.trim(F.col("email")))) \
    .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))

df_cust_clean.write.format("delta").mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("silver.customers")

# 2. Clean and Deduplicate Products
df_prod = spark.read.table("bronze.products")
window_prod = Window.partitionBy("product_id").orderBy(F.col("product_name"))

df_prod_clean = df_prod \
    .filter(F.col("product_id").isNotNull()) \
    .withColumn("rn", F.row_number().over(window_prod)) \
    .filter(F.col("rn") == 1) \
    .drop("rn") \
    .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.lit(0.0))) \
    .withColumn("active_flag", F.coalesce(F.col("active_flag"), F.lit(True)))

df_prod_clean.write.format("delta").mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("silver.products")

# 3. Clean and Standardize Orders
df_ord = spark.read.table("bronze.orders")
df_ord_clean = df_ord \
    .filter(F.col("order_id").isNotNull()) \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_ts", F.to_timestamp(F.col("order_ts"))) \
    .withColumn("status", F.coalesce(F.col("status"), F.lit("UNKNOWN")))

df_ord_clean.write.format("delta").mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("silver.orders")

# 4. Clean and Cast Order Items
df_items = spark.read.table("bronze.order_items")
df_items_clean = df_items \
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull()) \
    .dropDuplicates(["order_id", "product_id"]) \
    .withColumn("quantity", F.when(F.col("quantity") < 0, 0).otherwise(F.col("quantity"))) \
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))

df_items_clean.write.format("delta").mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("silver.order_items")


# --- GOLD LAYER TRANSFORMATIONS ---

# Join datasets to build a business-ready aggregate table
orders_fact = spark.read.table("silver.orders")
items_dim = spark.read.table("silver.order_items")
products_dim = spark.read.table("silver.products")

df_gold_source = orders_fact \
    .join(items_dim, "order_id", "inner") \
    .join(products_dim, "product_id", "inner")

df_gold_agg = df_gold_source \
    .withColumn("order_date", F.to_date(F.col("order_ts"))) \
    .groupBy("order_date", "category", "store_region") \
    .agg(
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
cdf_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("silver.customers")

display(cdf_df.select("_change_type", "customer_id", "email", "_commit_version").limit(5))