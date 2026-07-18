# Phase 5 — Lake Formation Governance (AWS Console Walkthrough)

> Replace every `<FILL IN>` with your actual account ID / bucket name /
> screenshots once you run through these steps.

---

## 24. Register S3 Location + Switch `retailflow_raw` to LF-Managed Permissions

**24.1 Register the bucket**
1. Open **AWS Lake Formation console** → left sidebar **Data lake locations**.
2. Click **Register location**.
3. **Amazon S3 path:** `s3://<YOUR_BUCKET>/raw/`
4. **IAM role:** leave as the default service-linked role (`AWSServiceRoleForLakeFormationDataAccess`) unless you have a custom data access role.
5. Click **Register location**.

**24.2 Confirm you're a Data Lake Administrator (first-time setup only)**
1. Left sidebar → **Administrative roles and tasks**.
2. Under **Data lake administrators**, click **Add administrators**.
3. Select your own IAM user/role → **Confirm**.
   *(Skipping this can lock you out of managing LF permissions later.)*

**24.3 Remove the `IAMAllowedPrincipals` fallback on `retailflow_raw`**
This is the step that actually switches the database from "IAM decides
access" to "Lake Formation decides access."
1. Left sidebar → **Databases** → select **`retailflow_raw`**.
2. **Actions → View permissions**.
3. Find the row where **Principal = `IAMAllowedPrincipals`**.
4. Check it → click **Revoke**.
5. Go to **Tables**, filter by database `retailflow_raw`, select **all tables** (checkbox in header).
6. **Actions → View permissions** → find `IAMAllowedPrincipals` → **Revoke**.

**24.4 Verify**
- Go back to **Databases → retailflow_raw → Actions → View permissions** — `IAMAllowedPrincipals` should no longer be listed. Screenshot this for your own records.

---

## 25. Create LF-Tags

1. Lake Formation console → left sidebar **LF-Tags**.
2. Click **Add LF-tag**.
3. **Key:** `sensitivity` → **Values:** type `PII`, press enter, type `Confidential`, press enter, type `Public`, press enter → **Add LF-tag**.
4. Click **Add LF-tag** again.
5. **Key:** `department` → **Values:** `analytics`, `engineering` → **Add LF-tag**.
6. Confirm both rows appear in the **LF-Tags** table with the correct value lists.

| Tag key | Values |
|---|---|
| `sensitivity` | `PII`, `Confidential`, `Public` |
| `department` | `analytics`, `engineering` |

---

## 26. Assign LF-Tags to Columns/Tables

**26.1 Tag `customers.email` as PII (column-level)**
1. **Databases → retailflow_raw → Tables → customers**.
2. Open the **Schema** tab — this lists every column.
3. Check the box next to the **`email`** row only.
4. Click **Edit LF-tags** (button above the column list).
5. **Assign new LF-tag** → Key: `sensitivity`, Value: `PII` → **Save**.

**26.2 Tag the rest of `customers` as Public (table-level)**
1. On the same table page, go to the **Details** tab.
2. **Edit LF-tags** → **Assign new LF-tag** → Key: `sensitivity`, Value: `Public` → **Save**.
   *(Column-level tags win over table-level tags for the column they're attached to — so `email` stays `PII`, everything else follows `Public`.)*

**26.3 Tag department ownership on the database**
1. **Databases → retailflow_raw → Actions → Edit LF-tags** (or the **LF-tags** tab on the database detail page).
2. **Assign new LF-tag** → Key: `department`, Value: `engineering` → **Save**.

**26.4 Tag curated/gold tables as Confidential (once Phase 6 tables exist)**
1. **Databases → retailflow_curated → Actions → Edit LF-tags**.
2. Assign `sensitivity = Confidential` and `department = analytics`.
3. For finer control, repeat at the individual table level (e.g. `orders`, `gold_orders_summary`) instead of the whole database.

**26.5 Tag assignment summary**

| Resource | Tag | Value |
|---|---|---|
| `retailflow_raw.customers.email` | `sensitivity` | `PII` |
| `retailflow_raw.customers` (other columns) | `sensitivity` | `Public` |
| `retailflow_raw` (database) | `department` | `engineering` |
| `retailflow_curated` (database) | `sensitivity` | `Confidential` |
| `retailflow_curated` (database) | `department` | `analytics` |

**Screenshot to capture:** the `customers` table's **Schema** tab showing `email` tagged `PII` while other columns show `Public`.

---

## 27. Create Two IAM Personas + Grant via LF-Tag Expressions

**27.1 Create the roles (IAM console)**
1. **IAM console → Roles → Create role**.
2. Trusted entity type: **AWS account** → **This account**.
3. **Next** → attach `AmazonAthenaFullAccess` and `AWSGlueConsoleFullAccess` (baseline access to run queries/browse the catalog — Lake Formation governs the actual data access on top of this).
4. Role name: `data_analyst` → **Create role**.
5. Repeat for role name `data_engineer`.

**27.2 Grant `data_analyst` access to `sensitivity=Public` only**
1. Lake Formation console → **Data lake permissions** → **Grant**.
2. **Principals:** select the `data_analyst` IAM role.
3. **LF-Tags or catalog resources:** choose **Resources matched by LF-Tags**.
4. **Add LF-Tag** → Key: `sensitivity`, Values: `Public`.
5. **Table permissions:** check `Select`, `Describe`.
6. Click **Grant**.

**27.3 Grant `data_engineer` full access via department tag**
1. **Data lake permissions → Grant**.
2. **Principals:** select `data_engineer`.
3. **Resources matched by LF-Tags** → Key: `department`, Values: `engineering`.
4. **Table permissions:** check `Select`, `Describe`, `Alter`, `Insert`.
5. Click **Grant**.

**27.4 Verify grants**
- **Data lake permissions** page → filter by principal → confirm `data_analyst` only shows the `sensitivity=Public` grant and `data_engineer` shows the `department=engineering` grant.

**27.5 Permission matrix**

| Role | LF-Tag expression matched | Result on `customers` |
|---|---|---|
| `data_analyst` | `sensitivity = Public` | Sees `customer_id, first_name, last_name, signup_date, city, segment`. **Cannot** see `email`. |
| `data_engineer` | `department = engineering` | Sees all columns, including `email`. |

---

## 28. Prove the Access Boundary via Athena (Console)

**28.1 Switch into each role in the console**
1. Top-right account menu → **Switch role**.
2. **Account:** `<ACCOUNT_ID>` **Role:** `data_analyst` → give it a display color (helps you tell sessions apart) → **Switch Role**.
3. You're now browsing the console as `data_analyst`.

**28.2 Run the query as `data_analyst`**
1. Go to **Athena console → Query editor**.
2. Make sure the **Workgroup** and **query result location** are set (Settings, if first time).
3. Database: `retailflow_raw`. Run:
   ```sql
   SELECT customer_id, first_name, last_name, email, city
   FROM customers
   LIMIT 10;
   ```
4. Expected: Athena returns an **`Insufficient permissions to execute the query`** / **`Permission denied on column email`** error — because `data_analyst` has no grant covering the `PII`-tagged column.
5. **Screenshot this error.**
6. Optional secondary proof: run `SELECT customer_id, first_name, last_name, city FROM customers LIMIT 10;` (omitting `email`) — this **succeeds**, confirming the boundary is specifically on the `email` column, not the whole table.

**28.3 Switch to `data_engineer` and re-run**
1. Top-right → **Switch role** → back to your own user, then **Switch role** again into `data_engineer`.
2. Run the exact same query:
   ```sql
   SELECT customer_id, first_name, last_name, email, city
   FROM customers
   LIMIT 10;
   ```
3. Expected: query **succeeds**, `email` column is fully visible.
4. **Screenshot this successful result.**

**28.4 Results**

| Role | Query | Outcome |
|---|---|---|
| `data_analyst` | `SELECT ... email ... FROM customers` | `<FILL IN — e.g. "Permission denied on column email">` |
| `data_engineer` | Same query | `<FILL IN — e.g. "Succeeded, email column returned">` |

**Screenshots:**
- `![analyst-blocked](./screenshots/28_analyst_no_email.png)`
- `![engineer-full-access](./screenshots/28_engineer_full_access.png)`

**Conclusion:** `<FILL IN — 1–2 sentences: the sensitivity=PII column-level
LF-Tag enforced the boundary purely through tag-based grants, with no
per-user IAM S3/Glue policy edits needed as new PII columns get tagged
in the future.>`

---

## Sample Datasets

Same datasets from Phase 4 (`sample_data.zip` — `customers.csv` with the
`email` column used in step 26, `products.csv`, `order_items.csv`,
`orders_partitioned/`). Upload via **S3 console → your bucket → raw/ →
Upload**, preserving folder names, before starting step 24.
