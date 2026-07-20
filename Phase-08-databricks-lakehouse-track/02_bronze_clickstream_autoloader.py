# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retailflow_catalog;

# COMMAND ----------
# Configure streaming input and checkpoint coordinates
bucket = "retailflow-abhay"
source_landing_zone = f"s3://{bucket}/raw/clickstream/"
checkpoint_dir = f"s3://{bucket}/checkpoints/bronze_clickstream/"

# Auto Loader stream setup targeting day1 and day2 JSON files
clickstream_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useNotifications", "false") \
    .option("cloudFiles.schemaLocation", f"s3://{bucket}/schemas/bronze_clickstream/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load(source_landing_zone)

# Write stream out to a managed Bronze Delta table
query = clickstream_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(availableNow=True) \
    .toTable("bronze.clickstream")

query.awaitTermination()
print("Clickstream processing batch sequence complete.")