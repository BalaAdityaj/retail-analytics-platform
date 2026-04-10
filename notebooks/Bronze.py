# Databricks notebook source
# 1. Configure Databricks to unlock your Azure Storage Account
storage_account_name = "adityacapstonestorage" 
account_key = "YourStorageAccountKeyHere"  # Replace with your actual key
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Securely pass the key to the Spark engine
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    account_key
)

print("1. Reading raw CSV files from your Azure 'rawdataset' container...")
raw_path = f"abfss://rawdataset@{storage_account_name}.dfs.core.windows.net/"

# Read incremental sales data (month-wise partitions loaded by ADF)
incremental_path = f"abfss://rawdataset@{storage_account_name}.dfs.core.windows.net/incremental/year=2024/"

sales_df = spark.read.option("header", True).option("inferSchema", True).csv(incremental_path)
product_df = spark.read.csv(raw_path + "product_master.csv", header=True, inferSchema=True)
store_df = spark.read.csv(raw_path + "store_master.csv", header=True, inferSchema=True)
customer_df = spark.read.csv(raw_path + "customer_data.csv", header=True, inferSchema=True)
inventory_df = spark.read.csv(raw_path + "inventory_data.csv", header=True, inferSchema=True)
clickstream_df = spark.read.csv(raw_path + "clickstream_events.csv", header=True, inferSchema=True)

# Add ingestion timestamp for watermark-based incremental processing
sales_df = sales_df.withColumn("ingestion_timestamp", current_timestamp())

# Deduplicate sales before loading
sales_df = sales_df.dropDuplicates(["transaction_id"])

print(f"   Incremental sales records read: {sales_df.count():,}")

# ═══ SALES — Incremental Merge (insert new transactions only) ═════════════════
print("2. Loading sales incrementally via MERGE...")

if spark.catalog.tableExists("medallion.bronze.sales_transactions"):
    delta_table = DeltaTable.forName(spark, "medallion.bronze.sales_transactions")

    delta_table.alias("t").merge(
        sales_df.alias("s"),
        "t.transaction_id = s.transaction_id"
    ).whenNotMatchedInsertAll() \
     .execute()

    print(f"   Merged into existing table.")
else:
    # First time load
    sales_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("order_date", "channel") \
        .saveAsTable("medallion.bronze.sales_transactions")
    print(f"   Created new table.")

# ═══ DIMENSION TABLES — Full Refresh ═════════════════════════════════════════
print("3. Saving dimension tables to medallion.bronze...")

product_df.write.format("delta").mode("overwrite").saveAsTable("medallion.bronze.product_master")
store_df.write.format("delta").mode("overwrite").saveAsTable("medallion.bronze.store_master")
customer_df.write.format("delta").mode("overwrite").saveAsTable("medallion.bronze.customer_data")
inventory_df.write.format("delta").mode("overwrite").saveAsTable("medallion.bronze.inventory_data")
clickstream_df.write.format("delta").mode("overwrite").saveAsTable("medallion.bronze.clickstream_events")

print("✅ Success! Raw data read from Azure and safely loaded into medallion.bronze.")

# COMMAND ----------

# ─── INGESTION METADATA LOG ───────────────────────────────────────────────────
from datetime import datetime
from pyspark.sql import Row

print("Logging ingestion metadata...")

# Count rows from each table we just loaded
log_rows = [
    Row(run_id="bronze_run_001", table_name="sales_transactions",  rows_loaded=sales_df.count(),      source_path=raw_path + "sales_transactions.csv",  loaded_at=str(datetime.now()), status="success"),
    Row(run_id="bronze_run_001", table_name="product_master",      rows_loaded=product_df.count(),    source_path=raw_path + "product_master.csv",       loaded_at=str(datetime.now()), status="success"),
    Row(run_id="bronze_run_001", table_name="store_master",        rows_loaded=store_df.count(),      source_path=raw_path + "store_master.csv",         loaded_at=str(datetime.now()), status="success"),
    Row(run_id="bronze_run_001", table_name="customer_data",       rows_loaded=customer_df.count(),   source_path=raw_path + "customer_data.csv",        loaded_at=str(datetime.now()), status="success"),
    Row(run_id="bronze_run_001", table_name="inventory_data",      rows_loaded=inventory_df.count(),  source_path=raw_path + "inventory_data.csv",       loaded_at=str(datetime.now()), status="success"),
    Row(run_id="bronze_run_001", table_name="clickstream_events",  rows_loaded=clickstream_df.count(),source_path=raw_path + "clickstream_events.csv",   loaded_at=str(datetime.now()), status="success"),
]

log_df = spark.createDataFrame(log_rows)

log_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("medallion.bronze.ingestion_log")

print("✅ Ingestion log saved to medallion.bronze.ingestion_log")
log_df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact the small files created by partitioning + speed up joins
# MAGIC OPTIMIZE medallion.bronze.sales_transactions
# MAGIC   ZORDER BY (product_id, customer_id);
# MAGIC
# MAGIC OPTIMIZE medallion.bronze.clickstream_events
# MAGIC   ZORDER BY (customer_id, product_id);
# MAGIC
# MAGIC -- Turn on auto-optimize for future loads so you don't have to run this manually again
# MAGIC ALTER TABLE medallion.bronze.sales_transactions
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE medallion.bronze.clickstream_events
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC   );
# MAGIC
# MAGIC SELECT 'Bronze OPTIMIZE complete' AS status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM medallion.bronze.sales_transactions LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM medallion.bronze.customer_data 
# MAGIC WHERE age IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM medallion.bronze.sales_transactions 
# MAGIC WHERE quantity < 0 OR unit_price = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT transaction_id, COUNT(*) as duplicate_count 
# MAGIC FROM medallion.bronze.sales_transactions 
# MAGIC GROUP BY transaction_id 
# MAGIC HAVING duplicate_count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM medallion.bronze.inventory_data 
# MAGIC WHERE stock_on_hand < 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT city, state 
# MAGIC FROM medallion.bronze.store_master 
# MAGIC ORDER BY city;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT brand, category, sub_category 
# MAGIC FROM medallion.bronze.product_master 
# MAGIC ORDER BY brand;