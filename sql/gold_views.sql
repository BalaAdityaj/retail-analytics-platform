-- Gold Layer Governed Views
-- Capstone: Cloud-Native Omni-Channel Retail Analytics Platform

CREATE OR REPLACE VIEW medallion.gold.v_channel_performance AS
  SELECT * FROM medallion.gold.agg_channel_performance;

CREATE OR REPLACE VIEW medallion.gold.v_product_performance AS
  SELECT * FROM medallion.gold.agg_product_performance;

CREATE OR REPLACE VIEW medallion.gold.v_store_performance AS
  SELECT * FROM medallion.gold.agg_store_performance;

CREATE OR REPLACE VIEW medallion.gold.v_monthly_trend AS
  SELECT * FROM medallion.gold.agg_monthly_trend;

CREATE OR REPLACE VIEW medallion.gold.v_loyalty_analysis AS
  SELECT * FROM medallion.gold.agg_loyalty_performance;

CREATE OR REPLACE VIEW medallion.gold.v_inventory_risk AS
  SELECT * FROM medallion.gold.agg_inventory_risk;