# Databricks notebook source
from pyspark.sql.functions import col, count
from datetime import datetime

# COMMAND ----------

silver_customer = spark.table("medallion.silver.customer_data")
silver_product = spark.table("medallion.silver.product_master")
silver_store = spark.table("medallion.silver.store_master")
silver_sales = spark.table("medallion.silver.sales_transactions")
silver_inventory = spark.table("medallion.silver.inventory_data")
silver_clickstream = spark.table("medallion.silver.clickstream_events")

# COMMAND ----------

invalid_qty = silver_sales.filter(col("quantity") <= 0).count()

# COMMAND ----------

invalid_total = silver_sales.filter(col("total_amount") < 0).count()

# COMMAND ----------

invalid_price = silver_product.filter(col("selling_price") < col("cost_price")).count()

# COMMAND ----------

invalid_stock = silver_inventory.filter(col("stock_on_hand") < 0).count()

# COMMAND ----------

enriched_sales = silver_sales \
    .join(silver_product, "product_id", "left") \
    .join(silver_store, "store_id", "left")

null_product = enriched_sales.filter(col("product_name").isNull()).count()
null_store = enriched_sales.filter(col("store_name").isNull()).count()
null_customer = silver_sales.filter(col("customer_id").isNull()).count()

# COMMAND ----------

duplicates = silver_sales.groupBy("transaction_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

# COMMAND ----------

join_issues = enriched_sales.filter(
    col("product_name").isNull() |
    col("store_name").isNull()
).count()

# COMMAND ----------

invalid_category = silver_product.filter(
    col("category").isin("Electroncs", "Eletronics")
).count()

invalid_state = silver_store.filter(
    col("state").isin("Tn", "Tamilnadu")
).count()

# COMMAND ----------

validation_results = {
    "invalid_qty": invalid_qty,
    "invalid_total": invalid_total,
    "invalid_price": invalid_price,
    "invalid_stock": invalid_stock,
    "null_product": null_product,
    "null_store": null_store,
    "null_customer": null_customer,
    "duplicates": duplicates,
    "join_issues": join_issues,
    "invalid_category": invalid_category,
    "invalid_state": invalid_state
}

failed_checks = {k: v for k, v in validation_results.items() if v > 0}

if len(failed_checks) == 0:
    status = "PASS"
    print("✅ VALIDATION PASSED")
else:
    status = "FAIL"
    print("❌ VALIDATION FAILED")
    print("Issues:", failed_checks)

# COMMAND ----------

log_df = spark.createDataFrame(
    [(
        "validation_stage",
        invalid_qty,
        invalid_total,
        invalid_price,
        invalid_stock,
        null_product,
        null_store,
        null_customer,
        duplicates,
        join_issues,
        invalid_category,
        invalid_state,
        status,
        datetime.now()
    )],
    [
        "stage",
        "invalid_qty",
        "invalid_total",
        "invalid_price",
        "invalid_stock",
        "null_product",
        "null_store",
        "null_customer",
        "duplicates",
        "join_issues",
        "invalid_category",
        "invalid_state",
        "status",
        "timestamp"
    ]
)

log_df.write.format("delta") \
    .mode("append") \
    .save("/mnt/logs/validation_logs")

# COMMAND ----------

if status == "FAIL":
    raise Exception("Validation failed. Stopping pipeline.")