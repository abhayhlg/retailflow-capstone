# Phase 5 & 6 — Complete Step-by-Step Guide (Console Only)

> Uses the files generated for you: `glue_etl_job.py`, `orders_dq_ruleset.dqdl`,
> `order_items_dq_ruleset.dqdl`, `generate_retailflow_dataset.py`,
> `bookmarks_proof.md`. Replace `retailflow-abhay` / `988640945996` throughout.

---

# Phase 5 — Lake Formation Governance

## Step 24 — Register S3 + switch to LF-managed permissions

1. **Lake Formation console → Data lake locations → Register location**
   Path: `s3://retailflow-abhay/raw/` → **Register**.
2. **Administrative roles and tasks → Data lake administrators → Add administrators** → add yourself (first time only).
3. **Databases → retailflow_raw → Actions → View permissions**
   Find principal `IAMAllowedPrincipals` → select → **Revoke**.
4. Repeat for tables: **Tables** → filter by `retailflow_raw` → select all → **Actions → View permissions** → revoke `IAMAllowedPrincipals` there too.

The database is now Lake-Formation-governed instead of IAM-governed.

## Step 25 — Create LF-Tags

1. **LF-Tags → Add LF-tag**
   - Key: `sensitivity` → Values: `PII`, `Confidential`, `Public` → **Add**.
2. **Add LF-tag** again
   - Key: `department` → Values: `analytics`, `engineering` → **Add**.

## Step 26 — Tag columns/tables

1. **Databases → retailflow_raw → Tables → customers → Schema tab**
   → check the `email` column only → **Edit LF-tags** → `sensitivity = PII` → **Save**.
2. Same table, **Details tab** → **Edit LF-tags** → `sensitivity = Public` → **Save**.
   (Column-level tag wins for `email`; everything else is `Public`.)
3. Once your Phase 6 curated database exists: **Databases → retailflow_curated → Edit LF-tags** → `sensitivity = Confidential` → **Save**.
4. **Databases → retailflow_raw → Edit LF-tags** → `department = engineering` → **Save**.
   (This is the tag `data_engineer`'s grant in Step 27 matches against — do it here, not as an afterthought.)

## Step 27 — Two IAM roles, granted via LF-Tags

**These are IAM *roles*, not IAM users** — the correct way to represent
personas here: no long-lived access keys, and you test each one via
**Switch Role** in Step 28 instead of logging in as a separate user.

1. **IAM console → Roles → Create role → AWS account → This account → Next**
   Attach `AmazonAthenaFullAccess` + `AWSGlueConsoleFullAccess` → name it `data_analyst` → **Create**.
   (These two policies only grant the ability to run queries / browse the catalog — zero data access on their own. Actual table/column access comes entirely from the Lake Formation grants below.)
2. Repeat, name it `data_engineer`.
3. **Lake Formation → Data lake permissions → Grant**
   - Principal: `data_analyst` → Resources: **Resources matched by LF-Tags** → `sensitivity = Public` → Permissions: `Select`, `Describe` → **Grant**.
4. **Grant** again
   - Principal: `data_engineer` → Resources: `department = engineering` → Permissions: `Select`, `Describe`, `Alter`, `Insert` → **Grant**.

## Step 28 — Prove it in Athena

1. Top-right account menu → **Switch role** → Account: `988640945996`, Role: `data_analyst` → **Switch Role**.
2. **Athena → Query editor**, database `retailflow_raw`:
   ```sql
   SELECT customer_id, email FROM customers LIMIT 10;
   ```
   → **Fails / denies access to `email`**. Screenshot this.
3. Top-right → **Switch role** back, then **Switch role** into `data_engineer` → run the same query → **Succeeds**, `email` visible. Screenshot this.
4. Put both screenshots + a one-line result summary into `lake_formation_governance.md`.

**Phase 5 deliverable = the tag list above + these 2 screenshots.**

---

# Phase 6 — Glue ETL: Raw → Silver → Gold

## Data note — actual field names

The dataset (`generate_retailflow_dataset.py`) uses these exact field
names — `glue_etl_job.py` is written to match them:

| Table | Fields |
|---|---|
| `orders` | `order_id, customer_id, order_ts, store_region, status`, (+`discount_code` on day2 only) |
| `order_items` | `order_id, product_id, quantity, unit_price, line_total` |
| `customers` | `customer_id, first_name, last_name, email, signup_date, city, segment` |
| `products` | `product_id, product_name, category, unit_price, active_flag` |

Two things worth calling out because they're easy to get wrong:
- Raw `orders` has **no `order_total` column** — the job computes it by
  summing `order_items.line_total` per `order_id` and joining it in.
- Raw `order_items` **already has `unit_price`** — cast it directly, don't
  derive it from `line_total / quantity`.

## Step 29 — Create the Glue job

1. **Glue console → Glue Studio → Jobs → Script editor → Create**
2. Paste in the contents of **`glue_etl_job.py`**.
3. **Job details → Name**: `retailflow-raw-to-curated`
4. **Job details → Advanced properties → Referenced files path**: upload
   **both** `orders_dq_ruleset.dqdl` and `order_items_dq_ruleset.dqdl` to
   `s3://retailflow-abhay/scripts/`, then list both paths here
   (comma-separated), e.g.
   `s3://retailflow-abhay/scripts/orders_dq_ruleset.dqdl,s3://retailflow-abhay/scripts/order_items_dq_ruleset.dqdl`
5. **Job details → IAM Role**: pick/create a role with Glue + S3 read/write (raw, curated, quarantine) + CloudWatch `PutMetricData`.
6. **Job parameters**, add:

   | Key | Value |
   |---|---|
   | `--raw_orders_path` | `s3://retailflow-abhay/raw/orders/` |
   | `--raw_order_items_path` | `s3://retailflow-abhay/raw/order_items/` |
   | `--raw_customers_path` | `s3://retailflow-abhay/raw/customers/` |
   | `--raw_products_path` | `s3://retailflow-abhay/raw/products/` |
   | `--curated_s3_path` | `s3://retailflow-abhay/curated/` |
   | `--quarantine_s3_path` | `s3://retailflow-abhay/quarantine/` |
   | `--consumption_path` | `s3://retailflow-abhay/quarantine/` |   

7. **Save**.

## Step 30 — Job Bookmarks (day1 → day2 proof)

1. **Job details → Advanced properties → Job bookmark → Enable** → **Save**.
2. Run `generate_retailflow_dataset.py`, upload only the day1 files (`order_date=2024-06-01/` under `raw/orders/` and `raw/order_items/`, plus `customers.csv`/`products.csv`).
3. **Run** the job. Wait for **Succeeded**. Open the run's **CloudWatch logs** → note the two lines:
   ```
   [bookmark check] new order_items rows this run: N
   [bookmark check] new orders rows this run: N
   ```
   This is your "before" count.
4. Upload the day2 files (`order_date=2024-06-02/`).
5. **Run** the job again (same job, bookmarks still on). Check the logs again — both counts should reflect **only day2's rows**, not day1+day2.
6. Write all four numbers into **`bookmarks_proof.md`**.

## Step 31 — Schema evolution (discount_code)

Already handled in the script — nothing extra to do in the console:
- `spark.conf.set("spark.sql.parquet.mergeSchema", "true")` is set at the top.
- `safe_select()` auto-fills `discount_code` as `NULL` for day1-shaped rows, so day1 and day2 union cleanly regardless of which columns each day's raw file actually has.
- To verify: **Athena** → query the curated `orders` table → confirm day1 rows show `discount_code = NULL` and day2 rows show real values, with no error.

## Step 32 — DQDL rules (two gates, two files)

The single-ruleset design from earlier had a real bug: the referential
integrity rule (`order_items.product_id` vs `products.product_id`) can only
be evaluated against the `order_items` frame — `orders` has no `product_id`
column. So this is now **two separate rulesets, two separate
`EvaluateDataQuality` calls**:

| Ruleset | Evaluated against | Rules |
|---|---|---|
| `orders_dq_ruleset.dqdl` | `orders` frame | `IsComplete` on `order_id, customer_id, status, order_total` + `IsUnique "order_id"` |
| `order_items_dq_ruleset.dqdl` | `order_items` frame | `IsComplete` on `order_id, product_id, quantity, line_total` + `ReferentialIntegrity "product_id" "products_ref.{product_id}"` |

Nothing to build in the console — both files are read at runtime by the
script. (Optional: **Glue → Data Quality → Rulesets** to view/visualize either one if you want a screenshot of the rules themselves.)

## Step 33 — Quarantine + CloudWatch

Also already handled — each gate independently routes its own failing rows:
- Failing `orders` rows → `quarantine/orders/`
- Failing `order_items` rows → `quarantine/order_items/`
- Passing rows → the matching `curated/` table
- Both `EvaluateDataQuality` calls set `enableDataQualityCloudWatchMetrics: True` under distinct contexts (`orders_dq_check`, `order_items_dq_check`), so you get two separate quality-score metrics.


## Consumption Layer

Daily Revenue is stored in `consumption/daily_revenue/`

To get your screenshot:

1. **S3 console** → confirm files landed under `quarantine/orders/` and/or `quarantine/order_items/` after a run with some bad rows.
2. **CloudWatch console → Metrics → All metrics → Glue Data Quality** namespace → find the metrics for `orders_dq_check` and `order_items_dq_check` → **Graph them** → screenshot.

**Phase 6 deliverables = `glue_etl_job.py` + both `.dqdl` files (already have all three) + `bookmarks_proof.md` filled in from Step 30 + the CloudWatch screenshot from Step 33.**
