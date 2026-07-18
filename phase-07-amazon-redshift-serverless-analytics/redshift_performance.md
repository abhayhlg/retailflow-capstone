# Phase 7 — Redshift Serverless Performance Notes

**Environment:** Redshift Serverless, Base 8 RPU, Auto-pause enabled
**Query under test:** Customer-level revenue rollup joining `fact_order_items` to `dim_customer` and `dim_date`, filtered to a 30-day window.

```sql
EXPLAIN
SELECT
    c.segment,
    c.country,
    SUM(f.line_revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count
FROM analytics.fact_order_items f
JOIN analytics.dim_customer c ON f.customer_id = c.customer_id
JOIN analytics.dim_date d      ON f.order_date_id = d.date_id
WHERE d.full_date BETWEEN '2026-06-01' AND '2026-06-30'
GROUP BY c.segment, c.country
ORDER BY total_revenue DESC;
```

## Mock EXPLAIN Plan Output

```
XN Merge  (cost=1000234.56..1000235.06 rows=200 width=52)
  Merge Key: sum(f.line_revenue)
  ->  XN Network  (cost=1000234.56..1000235.06 rows=200 width=52)
        Send to leader
        ->  XN Sort  (cost=1000234.56..1000234.81 rows=200 width=52)
              Sort Key: sum(f.line_revenue)
              ->  XN HashAggregate  (cost=1000210.11..1000212.11 rows=200 width=52)
                    ->  XN Hash Join DS_DIST_NONE  (cost=45.20..980112.44 rows=3852201 width=48)
                          Hash Cond: ("outer".customer_id = "inner".customer_id)
                          ->  XN Hash Join DS_DIST_NONE  (cost=12.40..812004.10 rows=3852201 width=40)
                                Hash Cond: ("outer".order_date_id = "inner".date_id)
                                ->  XN Seq Scan on fact_order_items f  (cost=0.00..620044.00 rows=15872004 width=32)
                                      Filter: (order_date_id sortkey range pruned via zone map)
                                ->  XN Hash  (cost=8.60..8.60 rows=304 width=8)
                                      ->  XN Seq Scan on dim_date d  (cost=0.00..8.60 rows=304 width=8)
                                            Filter: (full_date >= '2026-06-01' AND full_date <= '2026-06-30')
                          ->  XN Hash  (cost=32.10..32.10 rows=214000 width=24)
                                ->  XN Seq Scan on dim_customer c  (cost=0.00..32.10 rows=214000 width=24)

----- Execution Summary (svl_query_summary) -----
step_type          rows_in      rows_out     bytes      workmem     is_diskbased
scan (fact)         15,872,004   3,852,201    123 MB      64 MB       false
hash join (date)     3,852,201    3,852,201    98 MB       32 MB       false
hash join (customer)  3,852,201    3,852,201    92 MB       48 MB       false
hashaggregate            3,852,201       200      1 MB        8 MB       false
sort                          200          200    <1 MB      <1 MB       false
```

## Annotation / Deep-Dive Analysis

Both hash joins report **`DS_DIST_NONE`**, meaning Redshift performed no network redistribution before joining — this is the direct payoff of the distribution design in `redshift_ddl.sql`: `dim_customer` and `dim_date` are `DISTSTYLE ALL` (already replicated on every slice), so the join to `fact_order_items` is fully **collocated** and avoids the costly `DS_DIST_BOTH` / `DS_BCAST_INNER` steps that would otherwise appear when one side of a join must be broadcast or redistributed across the cluster. The `XN Seq Scan on fact_order_items` is a full columnar block scan, but because the compound sort key leads with `order_date_id`, the `full_date BETWEEN` predicate lets Redshift's zone maps skip the vast majority of 1MB blocks outside the 30-day window before any row is read, which is why `rows_out` from that scan (3.85M) is far smaller than the fact table's total row count (15.87M) despite no `WHERE` filter appearing directly on the fact scan node. The `is_diskbased = false` flag across every step in `svl_query_summary` confirms the working set fit entirely in memory at 8 RPU for this window size — if a wider date range or an unfiltered scan pushed a hash table past available `workmem`, we'd expect `is_diskbased = true` and a sharp latency spike, which is the primary trigger for scaling base RPU capacity or narrowing the query's date predicate. Finally, the terminal `XN Merge`/`XN Network`/`XN Sort` steps are cheap relative to the joins (only 200 aggregated rows are being sorted and returned to the leader node), so tuning effort here should stay focused on the scan/join steps rather than the final aggregation.

## Operational Guardrails Tied to This Plan

| Concern | Mitigation | Where configured |
|---|---|---|
| Runaway full-table scans / cartesian joins | 60s workgroup Query Monitoring Rule (abort) | `redshift_governance.sql` §1 |
| Cold-partition scans outside the sort key range | Compound `SORTKEY (order_date_id, customer_id)` on the fact table | `redshift_ddl.sql` |
| Repeated identical category rollups | `mv_daily_revenue_by_category` with `AUTO REFRESH YES` and optimizer query rewrite | `redshift_ddl.sql` |
| Broadcast/redistribution cost on the customer join | `DISTSTYLE ALL` on `dim_customer` / `dim_date`, `DISTKEY(customer_id)` on the fact table | `redshift_ddl.sql` |
| Compute cost during idle periods | Auto-pause enabled at 8 base RPU | Workgroup configuration (console) |

## How to Capture the Real Plan (once loaded)

```sql
EXPLAIN <query>;                          -- plan only
SELECT * FROM svl_query_summary
WHERE query = pg_last_query_id()
ORDER BY seg, step;                       -- actual runtime breakdown per step

SELECT * FROM svl_query_report
WHERE query = pg_last_query_id()
ORDER BY segment, step;                   -- per-slice row/byte counts
```
