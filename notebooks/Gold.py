# Databricks notebook source
# The missing piece: Importing the Spark SQL functions
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round, when

print("1. Loading Cleaned Data from Silver Schema...")

# Dimensions
silver_customer = spark.table("medallion.silver.customer_data")
silver_product = spark.table("medallion.silver.product_master")
silver_store = spark.table("medallion.silver.store_master")

# Facts
silver_sales = spark.table("medallion.silver.sales_transactions")
silver_inventory = spark.table("medallion.silver.inventory_data")
silver_clickstream = spark.table("medallion.silver.clickstream_events")

# COMMAND ----------

print("2. Writing Dimension Tables (Lookup Tables)...")

silver_customer.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.dim_customer")
silver_product.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.dim_product")
silver_store.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.dim_store")

print("-> dim_customer, dim_product, and dim_store are created.")

# COMMAND ----------

print("3. Building Enriched fact_sales Table...")

fact_sales_df = silver_sales.alias("s") \
    .join(spark.table("medallion.gold.dim_product").alias("p"), col("s.product_id") == col("p.product_id"), "left") \
    .join(spark.table("medallion.gold.dim_store").alias("st"), col("s.store_id") == col("st.store_id"), "left") \
    .join(spark.table("medallion.gold.dim_customer").alias("c"), col("s.customer_id") == col("c.customer_id"), "left") \
    .select("s.*", "p.brand", "p.category", "st.city", "st.state", "c.loyalty_status")

fact_sales_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("medallion.gold.fact_sales")

print("-> fact_sales created (left joins — no sales records dropped).")

# COMMAND ----------

print("4. Processing Secondary Facts...")

# Inventory Fact (left joins to keep all inventory records)
fact_inventory_df = silver_inventory.alias("i") \
    .join(spark.table("medallion.gold.dim_product").alias("p"), "product_id", "left") \
    .join(spark.table("medallion.gold.dim_store").alias("st"), "store_id", "left") \
    .select("i.*", "p.brand", "st.city", "st.state")
fact_inventory_df.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.fact_inventory")

# Clickstream Fact (overwriteSchema handles the event_timestamp date→timestamp change)
fact_clickstream_df = silver_clickstream.alias("clk") \
    .join(spark.table("medallion.gold.dim_product").alias("p"), "product_id", "left") \
    .select("clk.*", "p.brand", "p.category")
fact_clickstream_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medallion.gold.fact_clickstream")

print("-> fact_inventory and fact_clickstream created.")

# COMMAND ----------

print("5. Creating Aggregated KPI Tables...")

# Channel-wise Performance
channel_agg = spark.table("medallion.gold.fact_sales").groupBy("channel").agg(
    count("transaction_id").alias("total_orders"),
    spark_round(spark_sum("net_sales"), 2).alias("total_revenue"),
    spark_round(spark_sum("gross_margin"), 2).alias("total_profit")
)
channel_agg.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_channel_performance")

# Store-wise Performance (using col for the order by)
store_agg = spark.table("medallion.gold.fact_sales").groupBy("city", "state").agg(
    spark_round(spark_sum("net_sales"), 2).alias("total_revenue")
).orderBy(col("total_revenue").desc())
store_agg.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_store_performance")

print("-> KPI tables ready.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM medallion.gold.agg_channel_performance;

# COMMAND ----------

# DBTITLE 1,BI Agg 1 — Customer Loyalty ROI
from pyspark.sql.functions import avg as spark_avg

print("6. Advanced BI Aggregations...")

# ── 1. Customer Loyalty ROI ──────────────────────────────────────────────────
# Business Q: Are Gold members more profitable per transaction than Standard?
fact_sales = spark.table("medallion.gold.fact_sales")

agg_loyalty = fact_sales.groupBy("loyalty_status").agg(
    count("transaction_id").alias("total_orders"),
    spark_round(spark_sum("net_sales"), 2).alias("total_revenue"),
    spark_round(spark_avg("gross_margin"), 2).alias("average_margin_per_order")
).orderBy(col("total_revenue").desc())

agg_loyalty.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_loyalty_performance")
print("-> agg_loyalty_performance created.")

# COMMAND ----------

# DBTITLE 1,BI Agg 1 — Loyalty Sample
# MAGIC %sql
# MAGIC -- Customer Loyalty ROI: Gold vs Standard profitability per order
# MAGIC SELECT * FROM medallion.gold.agg_loyalty_performance
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,BI Agg 2 — Regional Product Demand
# ── 2. Regional Product Demand ───────────────────────────────────────────────
# Business Q: Which categories dominate in Maharashtra vs Karnataka?
agg_regional = fact_sales.groupBy("state", "category").agg(
    spark_round(spark_sum("net_sales"), 2).alias("total_net_sales")
).orderBy(col("total_net_sales").desc())

agg_regional.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_regional_category_sales")
print("-> agg_regional_category_sales created.")

# COMMAND ----------

# DBTITLE 1,BI Agg 2 — Regional Sample
# MAGIC %sql
# MAGIC -- Regional Product Demand: Top state-category combinations by revenue
# MAGIC SELECT * FROM medallion.gold.agg_regional_category_sales
# MAGIC ORDER BY total_net_sales DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# DBTITLE 1,BI Agg 3 — Inventory Turnover and Risk
from pyspark.sql.functions import avg as spark_avg

# ── 3. Inventory Turnover & Risk ─────────────────────────────────────────────
# Business Q: Which cities are overstocked on brands that don't sell there?
fact_inv = spark.table("medallion.gold.fact_inventory")

agg_inv_health = fact_inv.groupBy("city", "brand").agg(
    spark_round(spark_sum("stock_on_hand"), 0).alias("total_stock_on_hand"),
    spark_round(spark_avg("reorder_level"), 0).alias("avg_reorder_level")
).withColumn(
    "overstock_flag",
    when(col("total_stock_on_hand") > col("avg_reorder_level") * 3, "OVERSTOCK")
    .when(col("total_stock_on_hand") < col("avg_reorder_level"), "LOW STOCK")
    .otherwise("HEALTHY")
).orderBy(col("total_stock_on_hand").desc())

agg_inv_health.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_inventory_health")
print("-> agg_inventory_health created.")

# COMMAND ----------

# DBTITLE 1,BI Agg 3 — Inventory Sample
# MAGIC %sql
# MAGIC -- Inventory Health: Overstock risk by city and brand
# MAGIC SELECT * FROM medallion.gold.agg_inventory_health
# MAGIC WHERE overstock_flag = 'OVERSTOCK'
# MAGIC ORDER BY total_stock_on_hand DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# DBTITLE 1,BI Agg 4 — Digital Behavior Analysis
# ── 4. Digital Behavior Analysis ──────────────────────────────────────────────
# Business Q: Which categories get the most digital engagement by event type?
fact_click = spark.table("medallion.gold.fact_clickstream")

agg_click = fact_click.groupBy("category", "event_type").agg(
    count("event_id").alias("total_events")
).orderBy("category", col("total_events").desc())

agg_click.write.format("delta").mode("overwrite").saveAsTable("medallion.gold.agg_clickstream_conversion")
print("-> agg_clickstream_conversion created.")
print("\n✅ All 4 advanced BI aggregations saved to medallion.gold.")

# COMMAND ----------

# DBTITLE 1,BI Agg 4 — Clickstream Sample
# MAGIC %sql
# MAGIC -- Digital Behavior: Event engagement by category
# MAGIC SELECT category, event_type, total_events,
# MAGIC        ROUND(total_events * 100.0 / SUM(total_events) OVER (PARTITION BY category), 1) AS pct_of_category
# MAGIC FROM medallion.gold.agg_clickstream_conversion
# MAGIC ORDER BY category, total_events DESC;

# COMMAND ----------

# ─── KPI: Product & Category Performance ─────────────────────────────────────
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, desc, countDistinct

print("Building product performance KPI...")

fact_sales = spark.table("medallion.gold.fact_sales")

agg_product = fact_sales \
    .groupBy("product_id", "category", "brand") \
    .agg(
        spark_round(spark_sum("net_sales"),    2).alias("total_revenue"),
        spark_round(spark_sum("gross_margin"), 2).alias("total_margin"),
        spark_sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("total_orders")
    ) \
    .orderBy(desc("total_revenue"))

agg_product.write.format("delta").mode("overwrite") \
    .saveAsTable("medallion.gold.agg_product_performance")

print("✅ agg_product_performance created")
display(agg_product.limit(10))

# COMMAND ----------

# ─── KPI: Store Performance ───────────────────────────────────────────────────
print("Building store performance KPI...")

agg_store = fact_sales \
    .groupBy("store_id", "city", "state") \
    .agg(
        spark_round(spark_sum("net_sales"),    2).alias("total_revenue"),
        spark_round(spark_sum("gross_margin"), 2).alias("total_margin"),
        count("transaction_id").alias("total_orders"),
        spark_round(avg("net_sales"),          2).alias("avg_order_value")
    ) \
    .orderBy(desc("total_revenue"))

agg_store.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medallion.gold.agg_store_performance")

print("✅ agg_store_performance created")
display(agg_store.limit(10))

# COMMAND ----------

# ─── KPI: Monthly Revenue Trend ───────────────────────────────────────────────
from pyspark.sql.functions import date_format

print("Building monthly trend KPI...")

agg_monthly = fact_sales \
    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("year_month", "channel") \
    .agg(
        spark_round(spark_sum("net_sales"),    2).alias("monthly_revenue"),
        spark_round(spark_sum("gross_margin"), 2).alias("monthly_margin"),
        count("transaction_id").alias("monthly_orders")
    ) \
    .orderBy("year_month", "channel")

agg_monthly.write.format("delta").mode("overwrite") \
    .saveAsTable("medallion.gold.agg_monthly_trend")

print("✅ agg_monthly_trend created")
display(agg_monthly.limit(12))

# COMMAND ----------

# ─── KPI: Inventory Stock-out Risk ────────────────────────────────────────────
print("Building inventory risk KPI...")

fact_inventory = spark.table("medallion.gold.fact_inventory")

agg_inventory = fact_inventory \
    .withColumn(
        "stock_status",
        col("stock_on_hand").cast("int")
    ) \
    .selectExpr(
        "product_id",
        "store_id",
        "brand",
        "city",
        "stock_on_hand",
        "reorder_level",
        """CASE 
            WHEN stock_on_hand = 0              THEN 'Out of Stock'
            WHEN stock_on_hand < reorder_level  THEN 'Below Reorder'
            ELSE                                     'Healthy'
           END AS stock_status"""
    )

agg_inventory.write.format("delta").mode("overwrite") \
    .saveAsTable("medallion.gold.agg_inventory_risk")

print("✅ agg_inventory_risk created")
display(
    agg_inventory.groupBy("stock_status")
    .count()
    .orderBy("stock_status")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ─── GOVERNED SQL VIEWS ──────────────────────────────────────────────────────
# MAGIC -- These views are the ONLY layer that should be queried for reporting
# MAGIC -- Never query raw Silver or Gold tables directly from BI tools
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_channel_performance AS
# MAGIC   SELECT * FROM medallion.gold.agg_channel_performance;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_product_performance AS
# MAGIC   SELECT * FROM medallion.gold.agg_product_performance;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_store_performance AS
# MAGIC   SELECT * FROM medallion.gold.agg_store_performance;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_monthly_trend AS
# MAGIC   SELECT * FROM medallion.gold.agg_monthly_trend;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_loyalty_analysis AS
# MAGIC   SELECT * FROM medallion.gold.agg_loyalty_performance;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_inventory_risk AS
# MAGIC   SELECT * FROM medallion.gold.agg_inventory_risk;
# MAGIC
# MAGIC SELECT 'All Gold views created successfully' AS status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ─── OPTIMIZE GOLD ───────────────────────────────────────────────────────────
# MAGIC OPTIMIZE medallion.gold.fact_sales
# MAGIC   ZORDER BY (order_date, channel, product_id);
# MAGIC
# MAGIC OPTIMIZE medallion.gold.agg_product_performance
# MAGIC   ZORDER BY (category, brand);
# MAGIC
# MAGIC OPTIMIZE medallion.gold.agg_store_performance
# MAGIC   ZORDER BY (city, state);
# MAGIC
# MAGIC OPTIMIZE medallion.gold.agg_monthly_trend
# MAGIC   ZORDER BY (year_month, channel);
# MAGIC
# MAGIC SELECT 'Gold OPTIMIZE complete' AS status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ─── VERIFY ALL GOLD VIEWS ───────────────────────────────────────────────────
# MAGIC SHOW TABLES IN medallion.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipeline monitoring dashboard view
# MAGIC CREATE OR REPLACE VIEW medallion.gold.v_pipeline_monitor AS
# MAGIC SELECT
# MAGIC     i.run_id,
# MAGIC     i.table_name,
# MAGIC     i.rows_loaded,
# MAGIC     i.loaded_at,
# MAGIC     i.status                AS ingestion_status
# MAGIC FROM medallion.bronze.ingestion_log i
# MAGIC ORDER BY i.loaded_at DESC;
# MAGIC
# MAGIC SELECT 'Pipeline monitor view created' AS status;