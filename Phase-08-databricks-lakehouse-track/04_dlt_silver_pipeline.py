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
    return dlt.read("bronze_customers_v") \
        .dropDuplicates(["customer_id"]) \
        .withColumn("email", F.lower(F.trim(F.col("email"))))

# Silver Order Items Processing with a Fail Expectation
@dlt.table(
    name="silver_order_items",
    comment="Enforces logical quantity controls on individual item rows.",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_fail("positive_quantity_enforced", "quantity > 0")
def silver_order_items():
    return dlt.read("bronze_order_items_v") \
        .dropDuplicates(["order_id", "product_id"])