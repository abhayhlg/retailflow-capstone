SELECT 
  category, 
  SUM(daily_revenue) AS categorical_revenue
FROM retailflow_catalog.gold.daily_regional_revenue
GROUP BY category
ORDER BY categorical_revenue DESC;