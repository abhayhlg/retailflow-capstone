SELECT 
  order_date, 
  SUM(daily_revenue) AS global_revenue
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY order_date
ORDER BY order_date ASC;