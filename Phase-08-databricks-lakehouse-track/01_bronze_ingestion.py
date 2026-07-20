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