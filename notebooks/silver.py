# Databricks notebook source
# DBTITLE 1,Imports and Bronze Load
# === SILVER LAYER: Production-Grade Data Profiling & Cleaning Pipeline ===
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, when, round as spark_round, mean, trim, initcap,
    to_date, to_timestamp, coalesce, count, lit, sum as spark_sum,
    min as spark_min, max as spark_max, avg, countDistinct, isnan, upper,
    desc, abs as spark_abs, current_timestamp
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from functools import reduce
from delta.tables import DeltaTable

# ═══ WATERMARK-BASED INCREMENTAL PROCESSING ═════════════════════════════════
print("Determining incremental watermark...")

if spark.catalog.tableExists("medallion.silver.sales_transactions"):
    last_watermark = spark.table("medallion.silver.sales_transactions") \
        .select(spark_max(col("ingestion_timestamp")).alias("max_ts")) \
        .first()["max_ts"]
    if last_watermark is None:
        last_watermark = "1900-01-01"
else:
    last_watermark = "1900-01-01"

print(f"   Last watermark: {last_watermark}")

# ═══ Load Bronze Tables ═════════════════════════════════════════════════════
datasets = {}

# Sales: read from bronze and filter only NEW records (after watermark)
bronze_sales_full = spark.table("medallion.bronze.sales_transactions")
datasets["sales"] = bronze_sales_full.filter(col("ingestion_timestamp") > lit(last_watermark))

print(f"   New sales records to process: {datasets['sales'].count():,}")

# Dimension tables: full load (unchanged)
datasets["product"]     = spark.table("medallion.bronze.product_master")
datasets["customer"]    = spark.table("medallion.bronze.customer_data")
datasets["store"]       = spark.table("medallion.bronze.store_master")
datasets["inventory"]   = spark.table("medallion.bronze.inventory_data")
datasets["clickstream"] = spark.table("medallion.bronze.clickstream_events")

# Convenience aliases
sales_df, product_df = datasets["sales"], datasets["product"]
customer_df, store_df = datasets["customer"], datasets["store"]
inventory_df, clickstream_df = datasets["inventory"], datasets["clickstream"]

# ═══ Capture PRE-cleaning baselines (for Step 4 comparison) ═════════════════
pre_row_counts = {name: df.count() for name, df in datasets.items()}

def null_profile(df):
    """Return {col_name: null_count} for every column."""
    return {c: df.filter(col(c).isNull()).count() for c in df.columns}

pre_nulls = {name: null_profile(df) for name, df in datasets.items()}

def dup_count(df, key_col):
    return df.count() - df.dropDuplicates([key_col]).count()

pre_duplicates = {
    "sales":       dup_count(sales_df, "transaction_id"),
    "product":     dup_count(product_df, "product_id"),
    "customer":    dup_count(customer_df, "customer_id"),
    "store":       dup_count(store_df, "store_id"),
    "inventory":   inventory_df.count() - inventory_df.dropDuplicates(["product_id", "store_id"]).count(),
    "clickstream": dup_count(clickstream_df, "event_id")
}

print("✅ Bronze tables loaded  |  Pre-cleaning baselines captured")

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC # 📊 STEP 1: Initial EDA — Before Cleaning
# MAGIC > Profile every Bronze dataset to document data quality issues **before** any transformations are applied.

# COMMAND ----------

# DBTITLE 1,EDA — Schema, Samples, Row Counts
# ── 1a. Schema, Sample Records & Row Counts ──────────────────────────────────
print("\n" + "=" * 70)
print("   ROW COUNTS (Bronze Layer)")
print("=" * 70)
for name, cnt in pre_row_counts.items():
    print(f"   {name:<15} {cnt:>10,} rows")

# Schema + sample for each dataset
for name, df in datasets.items():
    print(f"\n{'=' * 70}")
    print(f"   SCHEMA: {name.upper()}")
    print(f"{'=' * 70}")
    df.printSchema()
    print(f"   Sample records ({name}):")
    display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,EDA — Null Analysis
# ── 1b. NULL Analysis (all columns, all datasets) ──────────────────────────
from pyspark.sql import Row

for name, null_map in pre_nulls.items():
    total = pre_row_counts[name]
    print(f"\n{'=' * 70}")
    print(f"   NULL ANALYSIS: {name.upper()}  ({total:,} rows)")
    print(f"{'=' * 70}")
    rows = [
        Row(column=c, null_count=n, pct_null=round(n / total * 100, 2))
        for c, n in null_map.items() if n > 0
    ]
    if rows:
        display(spark.createDataFrame(rows))
    else:
        print("   ✅ No nulls found.")

# COMMAND ----------

# DBTITLE 1,EDA — Duplicates and Data Quality
# ── 1c. Duplicate Records ─────────────────────────────────────────────────
print("=" * 70)
print("   DUPLICATE RECORDS (Bronze Layer)")
print("=" * 70)
for name, cnt in pre_duplicates.items():
    flag = "⚠️" if cnt > 0 else "✅"
    print(f"   {flag} {name:<15} {cnt:>6} duplicates")

# ── 1d. Data Quality Issues (business-rule violations) ──────────────────
print(f"\n{'=' * 70}")
print("   DATA QUALITY ISSUES")
print(f"{'=' * 70}")

# Sales issues
qty_bad   = sales_df.filter(col("quantity") <= 0).count()
price_bad = sales_df.filter(col("unit_price") <= 0).count()
total_bad = sales_df.filter(col("total_amount") < 0).count()
print(f"\n   💰 SALES:")
print(f"      quantity <= 0:      {qty_bad}")
print(f"      unit_price <= 0:    {price_bad}")
print(f"      total_amount < 0:   {total_bad}")

# Product issues
loss_products = product_df.filter(col("selling_price") < col("cost_price")).count()
print(f"\n   📦 PRODUCT:")
print(f"      selling_price < cost_price: {loss_products}")

# Inventory issues
neg_stock = inventory_df.filter(col("stock_on_hand") < 0).count()
print(f"\n   📦 INVENTORY:")
print(f"      stock_on_hand < 0:  {neg_stock}")

# Store pre_quality for later comparison
pre_quality = {
    "sales_qty_bad": qty_bad, "sales_price_bad": price_bad,
    "sales_total_bad": total_bad, "product_loss": loss_products,
    "inv_neg_stock": neg_stock
}

# COMMAND ----------

# DBTITLE 1,EDA — Categoricals, Inconsistencies, Summary Stats
# ── 1e. Distinct Categorical Values & Inconsistencies ───────────────────
print("=" * 70)
print("   CATEGORICAL COLUMN AUDIT")
print("=" * 70)

categorical_checks = [
    ("product",   product_df,   ["category", "brand"]),
    ("store",     store_df,     ["city", "state"]),
    ("customer",  customer_df,  ["gender", "loyalty_status"]),
    ("sales",     sales_df,     ["channel", "payment_type"]),
]

pre_distinct = {}
for ds_name, df, cols in categorical_checks:
    for c in cols:
        vals = [row[c] for row in df.select(c).distinct().collect()]
        pre_distinct[f"{ds_name}.{c}"] = sorted([str(v) for v in vals if v])
        print(f"\n   {ds_name}.{c} ({len(vals)} distinct):")
        print(f"      {vals[:20]}")

# Flag inconsistencies
print(f"\n{'=' * 70}")
print("   ⚠️  KNOWN INCONSISTENCIES")
print(f"{'=' * 70}")
print("   product.category:  Look for 'Electroncs', 'Eletronics', 'Clths'")
print("   store.state:       Look for 'Tn', 'Tamilnadu', 'Karnataka '")
print("   customer.gender:   Look for mixed case (Male/MALE/male)")

# ── 1f. Summary Statistics (numeric columns) ───────────────────────────
print(f"\n{'=' * 70}")
print("   SUMMARY STATISTICS")
print(f"{'=' * 70}")

stats_checks = [
    ("sales",     sales_df,     ["quantity", "unit_price", "discount", "total_amount"]),
    ("product",   product_df,   ["cost_price", "selling_price"]),
    ("customer",  customer_df,  ["age"]),
    ("inventory", inventory_df, ["stock_on_hand", "reorder_level"]),
]

for ds_name, df, cols in stats_checks:
    agg_exprs = []
    for c in cols:
        agg_exprs += [
            spark_round(avg(c), 2).alias(f"{c}_mean"),
            spark_min(c).alias(f"{c}_min"),
            spark_max(c).alias(f"{c}_max")
        ]
    print(f"\n   {ds_name.upper()}:")
    display(df.agg(*agg_exprs))

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC # 🛠️ STEP 2: Data Cleaning & Transformation
# MAGIC > Apply retail business rules: deduplication, quality filters, master data mappings, financial enrichment.

# COMMAND ----------

# DBTITLE 1,Dimension and Master Data Cleaning
# ═══ CUSTOMER — One-Pass Cleaning ═════════════════════════════════════════════════
dynamic_avg_age = int(
    customer_df.select(spark_round(mean(col("age")), 0)).first()[0]
)
print(f"Dynamic mean customer age: {dynamic_avg_age}")

silver_customer = customer_df \
    .withColumn("gender", upper(trim(col("gender")))) \
    .withColumn("city", initcap(trim(col("city")))) \
    .withColumn(
        "loyalty_status",
        when(
            col("loyalty_status").isNull() | (col("loyalty_status") == "None"),
            "Standard"
        ).otherwise(col("loyalty_status"))
    ) \
    .withColumn("age", when(col("age").isNull(), lit(dynamic_avg_age)).otherwise(col("age"))) \
    .fillna({"city": "Unknown"})

# ═══ INVENTORY — Ghost Inventory Fix ══════════════════════════════════════════════
# Business rule: negative stock = system sync error → floor at 0
silver_inventory = inventory_df \
    .withColumn("stock_on_hand",
        when(col("stock_on_hand") < 0, 0).otherwise(col("stock_on_hand"))) \
    .withColumn("last_updated", to_timestamp(col("last_updated")))

# ═══ STORE MASTER — Geography Mapping ════════════════════════════════════════════
geo_mapping = [
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"),
    ("Bangalore", "Karnataka"), ("Delhi", "Delhi"),
    ("Hyderabad", "Telangana"), ("Chandigarh", "Chandigarh"),
    ("Jaipur", "Rajasthan")
]
geo_map_df = spark.createDataFrame(geo_mapping, ["city", "correct_state"])

silver_store = store_df \
    .withColumn("city", initcap(trim(col("city")))) \
    .withColumn("state", initcap(trim(col("state")))) \
    .withColumn("store_type", initcap(trim(col("store_type")))) \
    .withColumn("state",
        when(col("state").isin("Tn", "Tamilnadu"), "Tamil Nadu").otherwise(col("state"))) \
    .join(geo_map_df, on="city", how="left") \
    .withColumn("state", coalesce(col("correct_state"), col("state"))) \
    .drop("correct_state") \
    .fillna({"city": "Unknown", "state": "Unknown"})

# ═══ PRODUCT MASTER — Brand Hierarchy Correction ═════════════════════════════════
# NOTE: brand column is uppercased before the join, so mapping keys must be uppercase
brand_mapping = [
    ("APPLE", "Electronics", "Mobile"), ("SAMSUNG", "Electronics", "Mobile"),
    ("SONY", "Electronics", "Audio/Video"), ("LG", "Electronics", "Appliances"),
    ("NIKE", "Clothing/Sports", "Shoes"), ("ADIDAS", "Clothing/Sports", "Shoes"),
    ("PUMA", "Clothing/Sports", "Shoes")
]
brand_map_df = spark.createDataFrame(brand_mapping, ["brand", "correct_category", "correct_sub_category"])

# Standardize first, then apply brand hierarchy
silver_product = product_df \
    .withColumn("brand", upper(trim(col("brand")))) \
    .withColumn("category", initcap(trim(col("category")))) \
    .withColumn("sub_category", initcap(trim(col("sub_category")))) \
    .withColumn("category",
        when(col("category").isin("Electroncs", "Eletronics"), "Electronics")
        .when(col("category") == "Clths", "Clothing")
        .otherwise(col("category"))) \
    .join(brand_map_df, on="brand", how="left") \
    .withColumn("category", coalesce(col("correct_category"), col("category"))) \
    .withColumn("sub_category", coalesce(col("correct_sub_category"), col("sub_category"))) \
    .drop("correct_category", "correct_sub_category")

# ═══ PRODUCT MASTER — Price Swap Fix ═════════════════════════════════════════════
# Business rule: selling_price must be >= cost_price; swap if inverted
silver_product = silver_product \
    .withColumn("_orig_selling", col("selling_price")) \
    .withColumn("selling_price",
        when(col("selling_price") < col("cost_price"), col("cost_price"))
        .otherwise(col("selling_price"))) \
    .withColumn("cost_price",
        when(col("_orig_selling") < col("cost_price"), col("_orig_selling"))
        .otherwise(col("cost_price"))) \
    .drop("_orig_selling")

print("✅ Dimension tables cleaned (Customer, Inventory, Store, Product)")

# COMMAND ----------

# DBTITLE 1,Sales Pipeline and Clickstream Standardization
# ═══ SALES — Deduplication ═════════════════════════════════════════════════════
window_spec = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())

deduped_sales = sales_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# ═══ SALES — Quality Filter & Reject Table ═════════════════════════════════════
valid_condition = (col("quantity") > 0) & (col("unit_price") > 0) & (col("total_amount") >= 0)

clean_sales  = deduped_sales.filter(valid_condition)
reject_sales = deduped_sales.filter(~valid_condition)

print(f"Valid sales:    {clean_sales.count():,}")
print(f"Rejected sales: {reject_sales.count():,}")

# ═══ SALES — Type Standardization & Recalculation ═════════════════════════════
clean_sales = clean_sales \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("calculated_total", spark_round(col("quantity") * col("unit_price") - col("discount"), 2)) \
    .withColumn("total_amount",
        when(spark_abs(col("total_amount") - col("calculated_total")) > 1,
             col("calculated_total"))
        .otherwise(col("total_amount"))) \
    .drop("calculated_total")

# ═══ SALES — Financial Enrichment ═════════════════════════════════════════════
silver_sales = clean_sales \
    .withColumn("net_sales",
        spark_round(col("quantity") * col("unit_price") * (1 - col("discount")), 2)) \
    .withColumn("Order_Value_Band",
        when(col("total_amount") >= 2000, "High")
        .when(col("total_amount") >= 500, "Medium")
        .otherwise("Low"))

# Join with product for gross_margin = net_sales - (quantity * cost_price)
silver_sales = silver_sales.alias("s") \
    .join(silver_product.alias("p"), col("s.product_id") == col("p.product_id"), "left") \
    .withColumn("gross_margin",
        spark_round(col("s.net_sales") - (col("s.quantity") * col("p.cost_price")), 2)) \
    .select("s.*", "gross_margin")

# ═══ CLICKSTREAM — Standardize & Deduplicate ═══════════════════════════════════
silver_clickstream = clickstream_df \
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
    .dropDuplicates()

# Collect all silver DataFrames
silver_datasets = {
    "sales": silver_sales, "product": silver_product,
    "customer": silver_customer, "store": silver_store,
    "inventory": silver_inventory, "clickstream": silver_clickstream
}

print("✅ Sales pipeline, clickstream standardization & enrichment complete.")

# COMMAND ----------

# DBTITLE 1,Data Quality Audit
# ──────────────────────────────────────────────────────────────────────
# STEP 3: POST-CLEANING EDA
# ──────────────────────────────────────────────────────────────────────

# ── 3a. Null Analysis (post-cleaning) ───────────────────────────────────
post_nulls = {name: null_profile(df) for name, df in silver_datasets.items()}

print("=" * 70)
print("   POST-CLEANING: NULL ANALYSIS")
print("=" * 70)
for name, null_map in post_nulls.items():
    remaining = sum(null_map.values())
    print(f"   {name:<15} {remaining:>6} total nulls remaining")

# ── 3b. Duplicate Check (post-cleaning) ─────────────────────────────────
print(f"\n{'=' * 70}")
print("   POST-CLEANING: DUPLICATE CHECK")
print("=" * 70)
post_dups = {
    "sales":       dup_count(silver_sales, "transaction_id"),
    "product":     dup_count(silver_product, "product_id"),
    "customer":    dup_count(silver_customer, "customer_id"),
    "store":       dup_count(silver_store, "store_id"),
    "inventory":   silver_inventory.count() - silver_inventory.dropDuplicates(["product_id", "store_id"]).count(),
    "clickstream": dup_count(silver_clickstream, "event_id")
}
for name, cnt in post_dups.items():
    flag = "✅" if cnt == 0 else "⚠️"
    print(f"   {flag} {name:<15} {cnt:>6} duplicates")

# ── 3c. Business Rule Validation (post-cleaning) ───────────────────────
print(f"\n{'=' * 70}")
print("   POST-CLEANING: BUSINESS RULE VALIDATION")
print("=" * 70)
post_quality = {
    "sales_qty_bad":   silver_sales.filter(col("quantity") <= 0).count(),
    "sales_price_bad": silver_sales.filter(col("unit_price") <= 0).count(),
    "sales_total_bad": silver_sales.filter(col("total_amount") < 0).count(),
    "product_loss":    silver_product.filter(col("selling_price") < col("cost_price")).count(),
    "inv_neg_stock":   silver_inventory.filter(col("stock_on_hand") < 0).count()
}
for rule, cnt in post_quality.items():
    flag = "✅" if cnt == 0 else "⚠️"
    print(f"   {flag} {rule:<25} {cnt:>6} violations")

# ── 3d. Categorical Audit (post-cleaning) ───────────────────────────────
print(f"\n{'=' * 70}")
print("   POST-CLEANING: CATEGORICAL VALUES")
print("=" * 70)
post_categorical = [
    ("product",  silver_product,  ["category", "brand"]),
    ("store",    silver_store,    ["city", "state"]),
    ("customer", silver_customer, ["gender", "loyalty_status"]),
    ("sales",    silver_sales,    ["channel", "Order_Value_Band"]),
]
for ds_name, df, cols in post_categorical:
    for c in cols:
        vals = sorted([str(row[c]) for row in df.select(c).distinct().collect() if row[c]])
        print(f"   {ds_name}.{c}: {vals}")

# ── 3e. Summary Statistics (post-cleaning) ─────────────────────────────
print(f"\n{'=' * 70}")
print("   POST-CLEANING: SUMMARY STATISTICS")
print("=" * 70)
for ds_name, df, cols in [
    ("sales", silver_sales, ["quantity", "unit_price", "discount", "net_sales", "gross_margin"]),
    ("product", silver_product, ["cost_price", "selling_price"]),
    ("customer", silver_customer, ["age"]),
    ("inventory", silver_inventory, ["stock_on_hand"])
]:
    agg_exprs = []
    for c in cols:
        agg_exprs += [
            spark_round(avg(c), 2).alias(f"{c}_mean"),
            spark_min(c).alias(f"{c}_min"),
            spark_max(c).alias(f"{c}_max")
        ]
    print(f"\n   {ds_name.upper()}:")
    display(df.agg(*agg_exprs))

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC # 📊 STEP 4: Before vs After Comparison
# MAGIC > Quantify the exact data quality improvement from Bronze to Silver.

# COMMAND ----------

# DBTITLE 1,Before vs After Comparison
# ══════════════════════════════════════════════════════════════════════
# STEP 4: BEFORE vs AFTER COMPARISON
# ══════════════════════════════════════════════════════════════════════
from pyspark.sql import Row

post_row_counts = {name: df.count() for name, df in silver_datasets.items()}

# ── 4a. Row Count Comparison ─────────────────────────────────────────────
print("=" * 70)
print("   4a. ROW COUNT COMPARISON (Bronze → Silver)")
print("=" * 70)
row_rows = [
    Row(
        dataset=name,
        bronze_rows=pre_row_counts[name],
        silver_rows=post_row_counts[name],
        rows_removed=pre_row_counts[name] - post_row_counts[name],
        pct_retained=round(post_row_counts[name] / pre_row_counts[name] * 100, 1) if pre_row_counts[name] > 0 else 100.0
    )
    for name in pre_row_counts
]
row_comparison_df = spark.createDataFrame(row_rows)
display(row_comparison_df)
print(f"\n   Rejected sales (separate table): {reject_sales.count():,}")

# ── 4b. Null Reduction Per Column ───────────────────────────────────────
print(f"\n{'=' * 70}")
print("   4b. NULL REDUCTION (columns with nulls before cleaning)")
print("=" * 70)
null_rows = []
for name in pre_nulls:
    for col_name, before_count in pre_nulls[name].items():
        if before_count > 0:
            after_count = post_nulls.get(name, {}).get(col_name, 0)
            reduction = round((1 - after_count / before_count) * 100, 1) if before_count > 0 else 0
            null_rows.append(Row(
                dataset=name, column=col_name,
                nulls_before=before_count, nulls_after=after_count,
                pct_reduction=reduction
            ))
if null_rows:
    display(spark.createDataFrame(null_rows))

# ── 4c. Data Quality Improvements ──────────────────────────────────────
print(f"\n{'=' * 70}")
print("   4c. DATA QUALITY IMPROVEMENTS")
print("=" * 70)
quality_rows = [
    Row(rule=rule, before=pre_quality[rule], after=post_quality[rule],
        fixed=pre_quality[rule] - post_quality[rule])
    for rule in pre_quality
]
display(spark.createDataFrame(quality_rows))

# ── 4d. Duplicate Reduction ─────────────────────────────────────────────
print(f"\n{'=' * 70}")
print("   4d. DUPLICATE REDUCTION")
print("=" * 70)
dup_rows = [
    Row(dataset=name, dups_before=pre_duplicates[name], dups_after=post_dups[name],
        removed=pre_duplicates[name] - post_dups[name])
    for name in pre_duplicates
]
display(spark.createDataFrame(dup_rows))

# ── 4e. OVERALL IMPROVEMENT SUMMARY ─────────────────────────────────────
total_pre_nulls  = sum(sum(v.values()) for v in pre_nulls.values())
total_post_nulls = sum(sum(v.values()) for v in post_nulls.values())
total_pre_dups   = sum(pre_duplicates.values())
total_post_dups  = sum(post_dups.values())
total_pre_qual   = sum(pre_quality.values())
total_post_qual  = sum(post_quality.values())

print(f"\n{'=' * 70}")
print("   🏆 OVERALL IMPROVEMENT SUMMARY")
print(f"{'=' * 70}")
print(f"   Total null values:      {total_pre_nulls:>8} → {total_post_nulls:>8}  "
      f"({round((1 - total_post_nulls / max(total_pre_nulls,1)) * 100, 1)}% reduction)")
print(f"   Total duplicates:       {total_pre_dups:>8} → {total_post_dups:>8}  "
      f"({round((1 - total_post_dups / max(total_pre_dups,1)) * 100, 1)}% reduction)")
print(f"   Quality violations:     {total_pre_qual:>8} → {total_post_qual:>8}  "
      f"({round((1 - total_post_qual / max(total_pre_qual,1)) * 100, 1)}% reduction)")
total_bronze = sum(pre_row_counts.values())
total_silver = sum(post_row_counts.values())
print(f"   Total records:          {total_bronze:>8} → {total_silver:>8}  "
      f"({round(total_silver / max(total_bronze, 1) * 100, 1)}% retained)")

# COMMAND ----------

# DBTITLE 1,Step 5 Header
# MAGIC %md
# MAGIC # 💾 STEP 5: Output — Write Silver Tables
# MAGIC > Persist all cleaned DataFrames to `medallion.silver` as Delta tables.

# COMMAND ----------

# DBTITLE 1,Write All Tables to Silver
# ═══ Write Silver Tables ═══════════════════════════════════════════════════════
# NOTE: Sales MERGE is handled in Cell 16 (enriched sales with derived fields)
# This cell writes DIMENSION tables only (full refresh)

print("Writing dimension tables to medallion.silver...")

silver_customer.write.format("delta").mode("overwrite").saveAsTable("medallion.silver.customer_data")
silver_product.write.format("delta").mode("overwrite").saveAsTable("medallion.silver.product_master")
silver_store.write.format("delta").mode("overwrite").saveAsTable("medallion.silver.store_master")
silver_inventory.write.format("delta").mode("overwrite").saveAsTable("medallion.silver.inventory_data")
silver_clickstream.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("medallion.silver.clickstream_events")

print("✅ Dimension tables written to medallion.silver.")

# COMMAND ----------

# ─── DERIVED FIELDS: net_sales, gross_margin, order_value_band ────────────────
from pyspark.sql.functions import col, when, round as spark_round, lit, current_timestamp

print("Adding derived fields to Silver sales...")

# First check if cost_price already exists in silver_sales
# If it does, no need to join again
if "cost_price" in silver_sales.columns:
    # cost_price already in the table — use it directly
    silver_sales_enriched = silver_sales \
        .withColumn(
            "net_sales",
            spark_round(col("total_amount"), 2)
        ) \
        .withColumn(
            "gross_margin",
            spark_round(
                col("total_amount") - (col("quantity") * col("cost_price")),
                2
            )
        ) \
        .withColumn(
            "order_value_band",
            when(col("net_sales") < 500,  "Low")
           .when(col("net_sales") < 2000, "Medium")
           .otherwise("High")
        )
else:
    # cost_price not in silver_sales — need to join from product master
    silver_sales_enriched = silver_sales \
        .join(
            silver_product.select("product_id", "cost_price").distinct(),
            on="product_id",
            how="left"
        ) \
        .withColumn(
            "net_sales",
            spark_round(col("total_amount"), 2)
        ) \
        .withColumn(
            "gross_margin",
            spark_round(
                col("total_amount") - (col("quantity") * col("cost_price")),
                2
            )
        ) \
        .withColumn(
            "order_value_band",
            when(col("net_sales") < 500,  "Low")
           .when(col("net_sales") < 2000, "Medium")
           .otherwise("High")
        )

print("✅ Derived fields added. Sample:")
silver_sales_enriched.select(
    "transaction_id", "net_sales", "gross_margin", "order_value_band"
).show(5)

# COMMAND ----------

# ═══ WRITE ENRICHED SALES TO SILVER (Delta MERGE) ═════════════════════════
print("Writing enriched sales via Delta MERGE...")

if spark.catalog.tableExists("medallion.silver.sales_transactions"):
    silver_delta = DeltaTable.forName(spark, "medallion.silver.sales_transactions")

    silver_delta.alias("t").merge(
        silver_sales_enriched.alias("s"),
        "t.transaction_id = s.transaction_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("✅ medallion.silver.sales_transactions merged with derived fields")
else:
    silver_sales_enriched.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("order_date", "channel") \
        .saveAsTable("medallion.silver.sales_transactions")
    print("✅ medallion.silver.sales_transactions created with derived fields")

# COMMAND ----------

# ─── REJECT LOG: capture bad records with reason ─────────────────────────────
print("Running data quality checks and building reject log...")

silver_sales     = spark.table("medallion.silver.sales_transactions")
silver_inventory = spark.table("medallion.silver.inventory_data")
silver_product   = spark.table("medallion.silver.product_master")

from pyspark.sql.functions import col, lit, current_timestamp

rules = [
    ("quantity <= 0",         silver_sales.filter(col("quantity") <= 0)),
    ("unit_price <= 0",       silver_sales.filter(col("unit_price") <= 0)),
    ("total_amount < 0",      silver_sales.filter(col("total_amount") < 0)),
    ("gross_margin < 0",      silver_sales.filter(col("gross_margin") < 0)),
    ("stock_on_hand < 0",     silver_inventory.filter(col("stock_on_hand") < 0)),
    ("selling < cost_price",  silver_product.filter(
                                  col("selling_price") < col("cost_price"))),
]

for rule_name, bad_df in rules:
    count = bad_df.count()

    log_df = spark.createDataFrame([{
        "rule":          rule_name,
        "violation_count": count,
        "status":        "PASS" if count == 0 else "FAIL"
    }]).withColumn("checked_at", current_timestamp())

    log_df.write.format("delta").mode("append") \
       .option("mergeSchema", "true") \
       .saveAsTable("medallion.silver.dq_rule_log")

    if count > 0:
        bad_df.withColumn("reject_reason", lit(rule_name)) \
              .withColumn("rejected_at",   current_timestamp()) \
              .write.format("delta").mode("append") \
              .option("mergeSchema", "true") \
              .saveAsTable("medallion.silver.dq_reject_table")

        print(f"  ⚠️  Rule FAILED: {rule_name}  →  {count} bad records saved to reject table")
    else:
        print(f"  ✅ Rule PASSED: {rule_name}")

print("\nDone. Reject log summary:")
display(spark.table("medallion.silver.dq_rule_log"))

# COMMAND ----------

display(
    spark.table("medallion.silver.sales_transactions")
    .select("transaction_id", "product_id", "quantity", "unit_price", 
            "discount", "net_sales", "gross_margin", "order_value_band")
    .limit(100)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE medallion.silver.sales_transactions
# MAGIC   ZORDER BY (product_id, customer_id, store_id);
# MAGIC
# MAGIC OPTIMIZE medallion.silver.product_master  ZORDER BY (product_id);
# MAGIC OPTIMIZE medallion.silver.customer_data   ZORDER BY (customer_id);
# MAGIC OPTIMIZE medallion.silver.store_master    ZORDER BY (store_id);
# MAGIC
# MAGIC SELECT 'Silver OPTIMIZE complete' AS status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This query confirms that each city is mapped to exactly ONE correct state
# MAGIC SELECT city, state, COUNT(*) as store_count
# MAGIC FROM medallion.silver.store_master
# MAGIC GROUP BY city, state
# MAGIC ORDER BY city;