SELECT 
  store_region, 
  SUM(daily_revenue) AS regional_revenue,
  SUM(total_order_count) AS aggregate_orders
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY store_region
ORDER BY regional_revenue DESC;