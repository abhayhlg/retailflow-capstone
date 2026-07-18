# Lake Formation Governance — RetailFlow Data Platform

**Phase:** 5 — Lake Formation Governance
**Database governed:** `retailflow_raw` (and `retailflow_curated` once Phase 6 lands)
**Status:** `<FILL IN — In Progress / Complete>`
**Date:** `<FILL IN>`
**Author:** `<FILL IN>`

---

## 1. Resource Registration

| Item | Value |
|---|---|
| S3 location registered | `s3://retailflow-abhay/raw/` |
| Registration method | Lake Formation console → Data lake locations → Register location |
| Data access IAM role |
| `IAMAllowedPrincipals` revoked on `retailflow_raw` (database) | `Yes` |
| `IAMAllowedPrincipals` revoked on all tables in `retailflow_raw` | `Yes` |

**Verification:** `<FILL IN — e.g. "Databases → retailflow_raw → View permissions no longer lists IAMAllowedPrincipals as of <date>.">`

---

## 2. LF-Tag Definitions

| Tag key | Allowed values |
|---|---|
| `sensitivity` | `PII`, `Confidential`, `Public` |
| `department` | `analytics`, `engineering` |

---

## 3. LF-Tag Assignments

| Resource | Tag key | Tag value | Level |
|---|---|---|---|
| `retailflow_raw.customers.email` | `sensitivity` | `PII` | Column |
| `retailflow_raw.customers` (all other columns) | `sensitivity` | `Public` | Table |
| `retailflow_raw` (database) | `department` | `engineering` | Database |
| `retailflow_curated` (database) | `sensitivity` | `public` | Database |
| `retailflow_curated` (database) | `department` | `analytics` | Database |

> Column-level tags override table-level tags for the specific column they're
> attached to. `email` is the only column in `customers` tagged `PII`;
> everything else inherits `Public` from the table-level tag. Untagged
> columns inherit their tag values from the nearest tagged ancestor
> (column → table → database), which is why `email` — tagged `sensitivity`
> but not `department` — still inherits `department=engineering` from the
> `retailflow_raw` database.

---

## 4. IAM Personas

Both are IAM **roles** (not IAM users) — this avoids long-lived credentials
and lets each persona be tested via **Switch Role** with temporary
credentials rather than a separate login.

| Role name | Trust policy | Baseline IAM policies attached |
|---|---|---|
| `data_analyst` | This account (`sts:AssumeRole`) | `AmazonAthenaFullAccess`, `AWSGlueConsoleFullAccess` |
| `data_engineer` | This account (`sts:AssumeRole`) | `AmazonAthenaFullAccess`, `AWSGlueConsoleFullAccess` |

These IAM policies grant **zero data access** on their own — they only
allow running Athena queries and browsing the Glue catalog UI. Actual
table/column access is governed entirely by the Lake Formation grants
below.

---

## 5. LF-Tag-Based Grants

| Principal | LF-Tag expression matched | Permissions granted | Practical effect |
|---|---|---|---|
| `data_analyst` |  `department = analyst` | `sensitivity = Public` | `SELECT`, `DESCRIBE` | Can query `customers` columns tagged `Public`. Cannot query `email` (tagged `PII`, no matching grant). |
| `data_engineer` | `department = engineering` | `SELECT`, `DESCRIBE`, `ALTER`, `INSERT` | Matches the whole `retailflow_raw` database (tagged `department=engineering`), including `email` (which inherits `department=engineering` since it has no overriding `department` tag of its own). |

---

## 6. Access Boundary Proof

**Method:** Switch Role into each persona in the AWS Console, then run the
identical Athena query against `retailflow_raw.customers` from each session.

**Screenshot 1 — `data_engineer` sees `email`:**
`![engineer-full-access](./screenshots/data_engineer.png)`

**Method:** Switch Role into each persona in the AWS Console, then run the
identical Athena query against `retailflow_curated.customers` from each session.

**Screenshot 2 — `data_analyst` denied/omits `email`:**
`![analyst-blocked](./screenshots/data_analyst.png)`

