-- 1. Create an isolated open data share asset
CREATE SHARE IF NOT EXISTS retailflow_external_share
COMMENT 'Exposes verified aggregation logs to external partner organizations';

-- 2. Register the targeted gold table into the share envelope
ALTER SHARE retailflow_external_share ADD TABLE retailflow_catalog.gold.daily_regional_revenue
COMMENT 'Aggregated regional store sales volume analytics tracking';

-- 3. Create a unique consumer profile profile for the recipient
CREATE RECIPIENT IF NOT EXISTS partner_analytics_recipient
COMMENT 'External analytics vendor account endpoint access token identifier';