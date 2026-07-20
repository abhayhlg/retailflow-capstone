SELECT 
  SUM(total_order_count) AS total_processed_orders
FROM retailflow_catalog.gold.daily_regional_revenue;