# Phase 4 — Glue Data Catalog, Crawlers & Athena
## Cost Comparison: Pruned vs. Unpruned Queries


## 1. Athena Queries — Bytes Scanned (Pruned vs. Unpruned)

Run each query in the Athena console, then screenshot the **"Data
scanned"** value shown at the top of the results pane (or in Query
history). Also record it via CLI as a backup:


### Query 1 — Unpruned full table scan

- **Screenshot:** `![unpruned-query](./screenshots/athena_unpruned_scans1.png)`

### Query 2 — Pruned full table scan

- **Screenshot:** `![pruned-query](./screenshots/athena_pruned_scans1.png)`

### Query 3 — Unpruned with additional condition

- **Screenshot:** `![upruned-condition-query](./screenshots/athena_unpruned_scans2.png)`


### Query 4 — Pruned with additional condition

- **Screenshot:** `![upruned-condition-query](./screenshots/athena_pruned_scans2.png)`

### Query 5 — Unpruned with order by

- **Screenshot:** `![upruned-condition-query](./screenshots/athena_unpruned_scans3.png)`

### Query 6 — Pruned with order by

- **Screenshot:** `![upruned-condition-query](./screenshots/athena_pruned_scans3.png)`


## 22. Partition Projection on `orders`

Partition projection tells Athena how to *compute* partition values
(e.g. from a date range or an enumerated list) instead of looking them
up in the Glue Data Catalog / running a crawler. This removes the need
for `MSCK REPAIR TABLE` or crawler re-runs whenever new partitions land.


### 22.2 Trade-off: Crawler-based discovery vs. Partition Projection

| | Crawler-based partition discovery | Partition projection |
|---|---|---|
| **New partition visibility** | Only after next crawler run (or manual `MSCK REPAIR TABLE` / `ALTER TABLE ADD PARTITION`) — can lag by hours | Immediate — Athena computes valid partition values at query time, no catalog sync needed |
| **Cost** | Crawler runs cost money/time (DPU-hours) each schedule, even when nothing changed | No crawler needed for this table at all — $0 ongoing discovery cost |
| **Query planning speed** | Athena reads partition metadata from Glue Catalog — fast for small partition counts, degrades as partition count grows into the tens of thousands | Athena computes candidate partitions mathematically from the projection config, so it stays fast even with a huge number of partitions |
| **Schema drift protection** | Crawler can silently change column types (see §21) on every run unless schema-change policy is locked down | No crawler touching the table at all — schema only changes when you explicitly `ALTER TABLE`, safer but fully manual |
| **Flexibility** | Handles irregular/inconsistent partition layouts, missing partitions, non-uniform naming | Requires a *predictable, well-formed* key space (fixed range/interval/enum) — breaks if real S3 layout has gaps or naming isn't fully consistent with the template |
| **Setup effort** | Low — point crawler at S3, it figures out the rest | Higher — must know and hand-declare the exact range, interval, and format up front |
| **Best fit here** | Tables with unpredictable/irregular partitioning, or where you want the catalog to reflect exactly what physically exists | `orders`, since it partitions cleanly by a daily `order_date` with a known start date and ongoing daily cadence |

**Conclusion used for this project:** `"Kept the crawler
for customers/products (irregular updates), switched orders to partition
projection since it's append-only by date and the projection removes
daily MSCK repair overhead.">`

---