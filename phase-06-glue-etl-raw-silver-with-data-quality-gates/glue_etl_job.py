"""
glue_etl_job.py
Phase 6 — Raw -> Silver (curated) ETL with Job Bookmarks, schema evolution,
and Glue Data Quality gates that route failing rows to quarantine/.

READS DIRECTLY FROM S3 (not the Glue Data Catalog) via
glueContext.create_dynamic_frame.from_options(connection_type="s3", ...).

Field names below match the ACTUAL output of generate_retailflow_dataset.py:
  orders:       order_id, customer_id, order_ts, store_region, status,
                (discount_code — day2 only, schema evolution)
  order_items:  order_id, product_id, quantity, unit_price, line_total
  customers:    customer_id, first_name, last_name, email, signup_date,
                city, segment
  products:     product_id, product_name, category, unit_price, active_flag

NOTE: raw `orders` has no order_total column — it's computed in this job by
summing order_items.line_total per order_id. NOTE: raw `order_items` already
has unit_price — earlier versions of this script incorrectly derived it from
line_total/quantity; that's removed.

Two separate Data Quality gates are run, against two different frames:
  - orders_dq_ruleset.dqdl       -> evaluated against the ORDERS frame
                                     (completeness + uniqueness on order_id)
  - order_items_dq_ruleset.dqdl  -> evaluated against the ORDER_ITEMS frame
                                     (completeness + referential integrity
                                     against products — product_id only
                                     exists on order_items, not orders)
Rows failing either gate are quarantined separately; passing rows land in
the matching curated/ table.

Job parameters expected (Glue Studio job details / Job parameters tab):
  --raw_orders_path        s3://<YOUR_BUCKET>/raw/orders/
  --raw_order_items_path   s3://<YOUR_BUCKET>/raw/order_items/
  --raw_customers_path     s3://<YOUR_BUCKET>/raw/customers/
  --raw_products_path      s3://<YOUR_BUCKET>/raw/products/
  --curated_s3_path        s3://<YOUR_BUCKET>/curated/
  --quarantine_s3_path     s3://<YOUR_BUCKET>/quarantine/
  --job-bookmark-option    job-bookmark-enable   (set via console toggle, not here)

Referenced files needed at runtime (Job details -> Advanced properties ->
Referenced files path), uploaded to S3 alongside the script:
  orders_dq_ruleset.dqdl
  order_items_dq_ruleset.dqdl
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# ---------------------------------------------------------------------------
# 0. Boilerplate init
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_orders_path",
        "raw_order_items_path",
        "raw_customers_path",
        "raw_products_path",
        "curated_s3_path",
        "quarantine_s3_path",
        "consumption_s3_path",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CURATED_PATH = args["curated_s3_path"].rstrip("/")
QUARANTINE_PATH = args["quarantine_s3_path"].rstrip("/")
CONSUMPTION_PATH = args["consumption_s3_path"].rstrip("/")

# Step 31: allow the discount_code column (added on day2) to merge cleanly
# with day1-shaped Parquet that doesn't have it, without failing the job.
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# ---------------------------------------------------------------------------
# HELPER: schema-safe select.
#
# Job-bookmarked S3 reads (via from_options) can legitimately come back with
# ZERO matching files on a given run (e.g. customers.csv untouched since the
# last run while a new orders/order_date=.../ file was added). When that
# happens Spark cannot infer ANY schema from the empty result, so the
# DataFrame has zero columns. Referencing a possibly-missing column directly
# throws UNRESOLVED_COLUMN.
#
# safe_select() builds the target schema explicitly: for each expected
# column it casts the real column if present, or substitutes a typed NULL
# literal if the source came back without it. This can never raise
# UNRESOLVED_COLUMN, regardless of whether the source had 0, some, or all
# of the expected columns.
# ---------------------------------------------------------------------------
def safe_select(df, expected_cols_types):
    cols = []
    for name, dtype in expected_cols_types:
        if name in df.columns:
            cols.append(F.col(name).cast(dtype).alias(name))
        else:
            cols.append(F.lit(None).cast(dtype).alias(name))
    return df.select(*cols)

# ---------------------------------------------------------------------------
# 1. Read raw data DIRECTLY FROM S3 (not the Glue Catalog), WITH bookmarks
#    (step 30). transformation_ctx must stay stable across runs so Glue can
#    persist per-source "already processed" state.
#
#    NOTE on bookmark granularity: Glue tracks S3-source bookmarks per
#    object (key + last-modified time), not per catalog partition. Each
#    day's file lands under a new order_date=YYYY-MM-DD/ key, so day2 is
#    unambiguously "new" the moment it's uploaded.
# ---------------------------------------------------------------------------
orders_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["raw_orders_path"]], "recurse": True},
    format="json",
    transformation_ctx="orders_ctx",
)

order_items_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["raw_order_items_path"]], "recurse": True},
    format="json",
    transformation_ctx="order_items_ctx",
)

customers_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["raw_customers_path"]], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="customers_ctx",
)

products_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["raw_products_path"]], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="products_ctx",
)

# Referential integrity (step 32) needs the FULL current product catalog on
# every run, not just whatever product rows happen to be new since the last
# bookmark. Read products a second time with NO transformation_ctx (i.e. no
# bookmark applied) so this frame always reflects everything in
# raw_products_path.
products_full_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["raw_products_path"]], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    # no transformation_ctx here on purpose — always a full, unbookmarked read
)

orders_df = orders_dyf.toDF()
order_items_df = order_items_dyf.toDF()
customers_df = customers_dyf.toDF()
products_df = products_dyf.toDF()
products_full_df = products_full_dyf.toDF()

# ---------------------------------------------------------------------------
# 2. order_items: schema-safe cast, null handling, dedup (step 29).
#    unit_price is a REAL raw column here — cast it directly, don't derive
#    it from line_total/quantity.
#    Processed BEFORE orders, because orders.order_total is computed from
#    this frame below.
# ---------------------------------------------------------------------------
ORDER_ITEMS_SCHEMA = [
    ("order_id", "string"),
    ("product_id", "string"),
    ("quantity", "int"),
    ("unit_price", "double"),
    ("line_total", "double"),
]
order_items_typed = safe_select(order_items_df, ORDER_ITEMS_SCHEMA)
order_items_clean = (
    order_items_typed
    .withColumn("line_total", F.coalesce(F.col("line_total"), F.lit(0.0)))
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
    .dropDuplicates(["order_id", "product_id"])
)
# Cache immediately: this frame is read by multiple downstream actions
# (the count below, the order_total groupBy/join for orders, the DQ
# evaluation, and the final write). Without caching, each action
# re-triggers Spark's lazy DAG from scratch, which re-executes the
# bookmarked S3 read underneath it — and that re-read has proven
# inconsistent across repeated actions in this environment. Caching once
# here guarantees every downstream use sees the exact same materialized
# rows.
order_items_clean.cache()
order_items_new_count = order_items_clean.count()
print(f"[bookmark check] new order_items rows this run: {order_items_new_count}")

# ---------------------------------------------------------------------------
# 3. Orders: schema-safe cast using the REAL column name "status" (raw data
#    has no "order_status"), derive order_date from the S3 partition path,
#    schema evolution (discount_code absent on day1), compute order_total
#    from order_items (raw orders has no order_total column at all), dedup
#    (step 29, 31).
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = [
    ("order_id", "string"),
    ("customer_id", "string"),
    ("order_ts", "timestamp"),
    ("store_region", "string"),
    ("status", "string"),
    ("discount_code", "string"),  # absent on day1 files — safe_select fills NULL
]
orders_typed = safe_select(orders_df, ORDERS_SCHEMA)

# input_file_name() doesn't reference a source column, so this is always
# safe to run even against an empty read.
orders_typed = orders_typed.withColumn("order_date", F.to_date(F.col("order_ts")))


# order_total is NOT a raw column on orders — compute it by summing this
# run's order_items.line_total per order_id, then left-join onto orders.
# Orders with no matching item rows in THIS run's order_items batch default
# to 0.0 (acceptable for this dataset, since each day's orders and
# order_items files land together — see bookmark granularity note above).
order_totals = (
    order_items_clean
    .groupBy("order_id")
    .agg(F.sum("line_total").alias("order_total"))
)
orders_typed = (
    orders_typed
    .join(order_totals, on="order_id", how="left")
    .withColumn("order_total", F.coalesce(F.col("order_total"), F.lit(0.0)))
)

# Dedup: keep the latest row per order_id if the same order_id shows up more
# than once across raw files.
dedup_window = Window.partitionBy("order_id").orderBy(F.desc("order_date"))
orders_clean = (
    orders_typed
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .filter(F.col("order_id").isNotNull())  # drop the all-NULL row from a
    .drop("_rn")                            # truly empty read, if any
)
orders_clean.cache()
orders_new_count = orders_clean.count()
print(f"[bookmark check] new orders rows this run: {orders_new_count}")
print("Orders DF:")
print(orders_clean.show(5))

# ---------------------------------------------------------------------------
# 4. customers / products: schema-safe cast, null handling, dedup
# ---------------------------------------------------------------------------
CUSTOMERS_SCHEMA = [
    ("customer_id", "string"),
    ("first_name", "string"),
    ("last_name", "string"),
    ("email", "string"),
    ("signup_date", "date"),
    ("city", "string"),
    ("segment", "string"),
]
customers_typed = safe_select(customers_df, CUSTOMERS_SCHEMA)
customers_clean = (
    customers_typed
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)
customers_clean.cache()
customers_new_count = customers_clean.count()
print(f"[bookmark check] new customers rows this run: {customers_new_count}")

PRODUCTS_SCHEMA = [
    ("product_id", "string"),
    ("product_name", "string"),
    ("category", "string"),
    ("unit_price", "double"),
    ("active_flag", "boolean"),
]
products_typed = safe_select(products_df, PRODUCTS_SCHEMA)
products_clean = (
    products_typed
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)
products_clean.cache()
products_new_count = products_clean.count()
print(f"[bookmark check] new products rows this run: {products_new_count}")

# Full (unbookmarked) product catalog, cleaned the same way, used ONLY for
# the referential integrity DQ check below — always the complete catalog,
# regardless of what the bookmarked products_dyf returned this run.
products_full_typed = safe_select(products_full_df, PRODUCTS_SCHEMA)
products_full_clean = (
    products_full_typed
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)
products_full_clean.cache()

if (orders_new_count == 0 and order_items_new_count == 0
        and customers_new_count == 0 and products_new_count == 0):
    print("No new data on ANY source since last bookmark — exiting without writing output.")
    job.commit()
    sys.exit(0)

# ---------------------------------------------------------------------------
# 5. Data Quality gate #1 — ORDERS (step 32, 33)
#    Completeness on required columns + Uniqueness on order_id.
# ---------------------------------------------------------------------------
if orders_new_count > 0:
    with open("orders_dq_ruleset.dqdl") as f:
        ORDERS_DQ_RULESET = f.read()

    orders_dyf_clean = DynamicFrame.fromDF(orders_clean, glueContext, "orders_dyf_clean")

    dq_results_orders = EvaluateDataQuality().process_rows(
        frame=orders_dyf_clean,
        ruleset=ORDERS_DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "orders_dq_check",
            "enableDataQualityCloudWatchMetrics": True,   # step 33
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={"observations.scope": "ALL", "performanceTuning.caching": "CACHE_NOTHING"},
    )

    # process_rows() returns a DynamicFrameCollection with two named frames:
    # "ruleOutcomes" (rule-level pass/fail summary) and "rowLevelOutcomes"
    # (the original rows with a DataQualityRulesPass/DataQualityRulesFail
    # column appended per row) — pull the row-level one out before .toDF().
    orders_row_level_dyf = SelectFromCollection.apply(
        dfc=dq_results_orders,
        key="rowLevelOutcomes",
        transformation_ctx="orders_row_level_outcomes",
    )

    orders_outcomes_df = orders_row_level_dyf.toDF()
    # DataQualityRulesPass / DataQualityRulesFail are ARRAYS of rule names
    # that passed/failed for each row, not counts — use F.size() to test
    # emptiness instead of comparing the array directly to an integer.
    orders_passed_df = orders_outcomes_df.filter(F.size(F.col("DataQualityRulesFail")) == 0)
    orders_failed_df = orders_outcomes_df.filter(F.size(F.col("DataQualityRulesFail")) > 0)

    orders_pass_count = orders_passed_df.count()
    orders_fail_count = orders_failed_df.count()
    orders_total = orders_pass_count + orders_fail_count
    orders_score = round((orders_pass_count / orders_total) * 100, 2) if orders_total else 100.0
    print(f"[DQ orders] passed={orders_pass_count} failed={orders_fail_count} score={orders_score}%")

    if orders_fail_count > 0:
        (
            orders_failed_df
            .withColumn("dq_failure_reason", F.lit("failed_orders_completeness_or_uniqueness"))
            .write.mode("append").partitionBy("order_date")
            .parquet(f"{QUARANTINE_PATH}/orders/")
        )
        print(f"[quarantine] wrote {orders_fail_count} rows to {QUARANTINE_PATH}/orders/")

    if orders_pass_count > 0:
        keep_cols = [c for c in orders_passed_df.columns if not c.startswith("DataQualityRules")]
        (
            orders_passed_df.select(*keep_cols)
            .write.mode("append").partitionBy("order_date")
            .parquet(f"{CURATED_PATH}/orders/")
        )
        print(f"[curated] wrote {orders_pass_count} rows to {CURATED_PATH}/orders/")

    # Write to consumption layer        
        daily_revenue_df = (
            orders_passed_df
            .groupBy("order_date")
            .agg(
                    F.round(F.sum("order_total"), 2).alias("total_revenue"),
                )
            .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0.0)))
            .select(
                "order_date",
                "total_revenue"
            )
        )
        daily_revenue_df.write.mode("overwrite").partitionBy("order_date").parquet(f"{CONSUMPTION_PATH}/daily_revenue/")
                
        
        print(f"[consumption] wrote {orders_pass_count} rows to {CONSUMPTION_PATH}/daily_revenue/")
    
else:
    print("[DQ orders] skipped — no new orders rows this run.")

# ---------------------------------------------------------------------------
# 6. Data Quality gate #2 — ORDER_ITEMS (step 32, 33)
#    Completeness + Referential Integrity against products. This is
#    evaluated against the ORDER_ITEMS frame, since product_id only exists
#    there (not on orders).
# ---------------------------------------------------------------------------
if order_items_new_count > 0:
    with open("order_items_dq_ruleset.dqdl") as f:
        ORDER_ITEMS_DQ_RULESET = f.read()

    order_items_dyf_clean = DynamicFrame.fromDF(order_items_clean, glueContext, "order_items_dyf_clean")
    products_ref_dyf = DynamicFrame.fromDF(products_full_clean, glueContext, "products_ref")

    dq_results_items = EvaluateDataQuality().process_rows(
        frame=order_items_dyf_clean,
        ruleset=ORDER_ITEMS_DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "order_items_dq_check",
            "enableDataQualityCloudWatchMetrics": True,   # step 33
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={"observations.scope": "ALL", "performanceTuning.caching": "CACHE_NOTHING"},
        additional_data_sources={"products_ref": products_ref_dyf},
    )

    items_row_level_dyf = SelectFromCollection.apply(
        dfc=dq_results_items,
        key="rowLevelOutcomes",
        transformation_ctx="items_row_level_outcomes",
    )

    items_outcomes_df = items_row_level_dyf.toDF()
    items_passed_df = items_outcomes_df.filter(F.size(F.col("DataQualityRulesFail")) == 0)
    items_failed_df = items_outcomes_df.filter(F.size(F.col("DataQualityRulesFail")) > 0)

    items_pass_count = items_passed_df.count()
    items_fail_count = items_failed_df.count()
    items_total = items_pass_count + items_fail_count
    items_score = round((items_pass_count / items_total) * 100, 2) if items_total else 100.0
    print(f"[DQ order_items] passed={items_pass_count} failed={items_fail_count} score={items_score}%")

    if items_fail_count > 0:
        (
            items_failed_df
            .withColumn("dq_failure_reason", F.lit("failed_completeness_or_referential_integrity"))
            .write.mode("append")
            .parquet(f"{QUARANTINE_PATH}/order_items/")
        )
        print(f"[quarantine] wrote {items_fail_count} rows to {QUARANTINE_PATH}/order_items/")

    if items_pass_count > 0:
        keep_cols = [c for c in items_passed_df.columns if not c.startswith("DataQualityRules")]
        (
            items_passed_df.select(*keep_cols)
            .write.mode("append")
            .parquet(f"{CURATED_PATH}/order_items/")
        )
        print(f"[curated] wrote {items_pass_count} rows to {CURATED_PATH}/order_items/")
else:
    print("[DQ order_items] skipped — no new order_items rows this run.")

# ---------------------------------------------------------------------------
# 7. Write customers / products straight to curated/ (no DQ gate defined for
#    them in this phase, but same idempotent append pattern).
# ---------------------------------------------------------------------------
if customers_new_count > 0:
    customers_clean.write.mode("append").parquet(f"{CURATED_PATH}/customers/")
    print(f"[curated] wrote customers ({customers_new_count} rows)")

if products_new_count > 0:
    products_clean.write.mode("append").parquet(f"{CURATED_PATH}/products/")
    print(f"[curated] wrote products ({products_new_count} rows)")

# ---------------------------------------------------------------------------
# 8. Commit the job bookmark — advances the "last processed" marker. If this
#    line doesn't run (e.g. job errors out earlier), the bookmark does not
#    advance and the next run will safely reprocess.
# ---------------------------------------------------------------------------
job.commit()
