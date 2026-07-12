# ARCHITECTURE.md

## 1. Overview

This document specifies the storage topology and data organization standards for the cloud data lake built on Amazon S3. The design implements a centralized **Medallion Architecture** pattern to orchestrate data maturity, quality boundaries, and structural transformations across three stages: **Raw (Bronze)**, **Curated (Silver)**, and **Consumption (Gold)**.

---

## 2. Solution Architecture Topology

```text
+------------------------+
|  Ingestion Utility     |
|  (Boto3 Ingest Client) |
+------------------------+
            |
            | Writes data using Hive-style partitioning
            v
+----------------------------------------------------------------------------------+
|                           Amazon S3 Data Lake                                    |
|                                                                                  |
|  raw/ (Bronze)  -->  curated/ (Silver)  -->  consumption/ (Gold)                |
|  • Raw records       • Cleaned columnar      • Optimized aggregates             |
|  • 30-day IA policy  • Type-cast schemas     • Star schema models               |
|  • Multipart cleanup • Validated constraints • Analytics-ready datasets         |
+----------------------------------------------------------------------------------+
            |
            | Monitors `raw/` via `s3:ObjectCreated:*`
            v
+------------------------+        Invokes        +-------------------------+
|   Amazon EventBridge   | --------------------> |  Glue Job / Lambda      |
|   (Event Router)       |                       |  (ETL Pipelines)        |
+------------------------+                       +-------------------------+
```

---

## 3. Data Lake Tiers and Zones

| Prefix | Tier | Description |
|---------|------|-------------|
| `raw/` | Bronze | Immutable landing area containing data exactly as received from source systems. Acts as the audit trail and recovery point. |
| `curated/` | Silver | Cleanses, transforms, and standardizes raw data into schema-enforced columnar formats such as Parquet. |
| `consumption/` | Gold | Stores business-ready analytical models, star schemas, and aggregates optimized for BI and SQL analytics. |

---

## 4. Partitioning and Naming Conventions

To minimize scan costs and enable partition pruning, the Bronze layer uses Hive-style partitions based on ingestion date.

### Folder Structure

```text
raw/<dataset_name>/dt=YYYY-MM-DD/<filename>
```

### Path Variables

- **`<dataset_name>`**: Lowercase business domain (for example, `orders` or `transactions`).
- **`dt=YYYY-MM-DD`**: Ingestion date partition.
- **`<filename>`**: Original file name, including any source timestamp.

### Example

```text
s3://my-capstone-data-lake/raw/orders/dt=2026-07-12/orders_20260712.csv
```