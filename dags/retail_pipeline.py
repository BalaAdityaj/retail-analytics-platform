# ─── retail_pipeline_dag.py ───────────────────────────────────────────────────
# Capstone Project: Cloud-Native Omni-Channel Retail Analytics Platform
# Orchestrates: Bronze → Silver → Validation → Gold pipeline
# Schedule: Daily at 6 AM
# ─────────────────────────────────────────────────────────────────────────────

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import timedelta
import pendulum
import logging

# ─── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden

default_args = {
    "owner":              "data-engineering",
    "depends_on_past":    False,
    "start_date" :pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    "schedule" : "0 8 * * *",
    "retries":            3,                        # retry 3 times on failure
    "retry_delay":        timedelta(minutes=5),     # wait 5 min between retries
    "email":              ["balaadityaj@email.com"],        
    "email_on_failure":   True,
    "email_on_retry":     False,
}

# ─── FAILURE CALLBACK ─────────────────────────────────────────────────────────
# This function runs automatically when any task fails

def on_failure_callback(context):
    task_id  = context["task_instance"].task_id
    dag_id   = context["task_instance"].dag_id
    run_id   = context["run_id"]
    exc      = context.get("exception", "Unknown error")

    logging.error(
        f"\n{'='*60}\n"
        f"PIPELINE FAILURE ALERT\n"
        f"{'='*60}\n"
        f"DAG     : {dag_id}\n"
        f"Task    : {task_id}\n"
        f"Run ID  : {run_id}\n"
        f"Error   : {exc}\n"
        f"{'='*60}"
    )
    # In production this would send a Slack or email alert
    # For now it logs the failure clearly

# ─── TASK FUNCTIONS ───────────────────────────────────────────────────────────
# Each function simulates triggering a Databricks notebook
# In production these would use DatabricksRunNowOperator
def task_incremental_ingestion(**context):
    logging.info("=" * 50)
    logging.info("TASK 1: Incremental Ingestion Started")
    logging.info("Action : File sensor checking ADLS rawdataset container")
    logging.info("Action : Detecting new/modified files since last watermark")
    logging.info("Files  : sales_transactions.csv   → detected ✅")
    logging.info("Files  : product_master.csv        → detected ✅")
    logging.info("Files  : store_master.csv          → detected ✅")
    logging.info("Files  : customer_data.csv         → detected ✅")
    logging.info("Files  : inventory_data.csv        → detected ✅")
    logging.info("Files  : clickstream_events.csv    → detected ✅")
    logging.info("Action : Watermark updated in ingestion_log")
    logging.info("Note   : ADF pipeline would be triggered here in production")
    logging.info("Note   : Learner account uses direct ADLS read instead")
    logging.info("Status : Incremental Ingestion Complete ✅")
    logging.info("=" * 50)
    return "ingestion_complete"

def task_bronze_load(**context):
    logging.info("=" * 50)
    logging.info("TASK 1: Bronze Load Started")
    logging.info("Action : Reading CSVs from ADLS rawdataset container")
    logging.info("Action : Writing Delta tables to medallion.bronze")
    logging.info("Tables : sales_transactions, product_master, store_master,")
    logging.info("         customer_data, inventory_data, clickstream_events")
    logging.info("Action : Writing ingestion_log")
    logging.info("Action : Running OPTIMIZE + ZORDER on sales + clickstream")
    logging.info("Status : Bronze Load Complete ✅")
    logging.info("=" * 50)
    return "bronze_complete"

def task_silver_transformation(**context):
    logging.info("=" * 50)
    logging.info("TASK 2: Silver Transformation Started")
    logging.info("Action : Loading Bronze tables")
    logging.info("Action : Removing 500 duplicate transactions")
    logging.info("Action : Removing 50 negative quantity records")
    logging.info("Action : Removing 50 zero unit price records")
    logging.info("Action : Fixing 20 negative stock values")
    logging.info("Action : Filling 200 null customer ages with median")
    logging.info("Action : Adding net_sales, gross_margin, order_value_band")
    logging.info("Action : Writing cleaned tables to medallion.silver")
    logging.info("Action : Running OPTIMIZE + ZORDER on all Silver tables")
    logging.info("Status : Silver Transformation Complete ✅")
    logging.info("=" * 50)
    return "silver_complete"

def task_validation_checks(**context):
    logging.info("=" * 50)
    logging.info("TASK 3: Validation Checks Started")
    logging.info("Rule 1 : quantity > 0              → checking...")
    logging.info("Rule 2 : unit_price > 0            → checking...")
    logging.info("Rule 3 : total_amount >= 0         → checking...")
    logging.info("Rule 4 : gross_margin threshold    → checking...")
    logging.info("Rule 5 : stock_on_hand >= 0        → checking...")
    logging.info("Rule 6 : selling_price >= cost     → checking...")
    logging.info("Action : Saving violations to dq_reject_table")
    logging.info("Action : Logging results to dq_rule_log")

    # Simulate a validation failure scenario
    # In production this calls your actual Silver DQ notebook
    validation_passed = True  # set to False to test failure path

    if not validation_passed:
        raise Exception(
            "Validation FAILED: Critical DQ rules violated. "
            "Gold layer build blocked. "
            "Check medallion.silver.dq_reject_table for details."
        )

    logging.info("Status : All critical rules PASSED ✅")
    logging.info("Note   : 30,558 negative margin records flagged (business issue)")
    logging.info("Note   : 10 mispriced products flagged (business issue)")
    logging.info("=" * 50)
    return "validation_complete"

def task_gold_aggregation(**context):
    logging.info("=" * 50)
    logging.info("TASK 4: Gold Aggregation Started")
    logging.info("Action : Building dim_customer, dim_product, dim_store")
    logging.info("Action : Building fact_sales with left joins")
    logging.info("Action : Building fact_inventory, fact_clickstream")
    logging.info("Action : Building agg_channel_performance")
    logging.info("Action : Building agg_product_performance")
    logging.info("Action : Building agg_store_performance")
    logging.info("Action : Building agg_loyalty_performance")
    logging.info("Action : Building agg_monthly_trend")
    logging.info("Action : Building agg_inventory_risk")
    logging.info("Action : Creating governed v_ SQL views")
    logging.info("Action : Running OPTIMIZE + ZORDER on Gold tables")
    logging.info("Status : Gold Aggregation Complete ✅")
    logging.info("=" * 50)
    return "gold_complete"

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────

with DAG(
    dag_id             = "retail_medallion_pipeline",
    default_args       = default_args,
    description        = "Capstone: Omni-Channel Retail Analytics Pipeline",
    schedule  = "0 6 * * *",    # every day at 6:00 AM
    catchup            = False,
    tags               = ["capstone", "retail", "medallion"],
    on_failure_callback= on_failure_callback,
) as dag:

    t0_ingest = PythonOperator(
        task_id             = "incremental_ingestion",
        python_callable     = task_incremental_ingestion,
        on_failure_callback = on_failure_callback,
        sla                 = timedelta(hours=2),
    )

    # ── Task 1: Bronze ─────────────────────────────────────────────────────
    t1_bronze = PythonOperator(
        task_id             = "bronze_load",
        python_callable     = task_bronze_load,
        on_failure_callback = on_failure_callback,
        sla                 = timedelta(hours=2),
    )

    # ── Task 2: Silver ─────────────────────────────────────────────────────
    t2_silver = PythonOperator(
        task_id             = "silver_transformation",
        python_callable     = task_silver_transformation,
        on_failure_callback = on_failure_callback,
        sla                 = timedelta(hours=2),
    )

    # ── Task 3: Validation ─────────────────────────────────────────────────
    t3_validate = PythonOperator(
        task_id             = "validation_checks",
        python_callable     = task_validation_checks,
        on_failure_callback = on_failure_callback,
        sla                 = timedelta(hours=2),
    )

    # ── Task 4: Gold ───────────────────────────────────────────────────────
    t4_gold = PythonOperator(
        task_id             = "gold_aggregation",
        python_callable     = task_gold_aggregation,
        on_failure_callback = on_failure_callback,
        sla                 = timedelta(hours=2),
    )

    success_email = EmailOperator(
    task_id="success_email",
    to="balaadityaj@gmail.com",
    subject="✅ Retail Pipeline Success",
    html_content="""
    <h3>Pipeline Completed Successfully</h3>
    <p>All layers (Bronze → Silver → Gold) executed successfully.</p>
    """,
)
    
    failure_email = EmailOperator(
    task_id="failure_email",
    to="balaadityaj@gmail.com",
    subject="❌ Retail Pipeline Failed",
    html_content="""
    <h3>Pipeline Failed</h3>
    <p>Please check Airflow logs for details.</p>
    """,
    trigger_rule="one_failed",  
)

    # ── Task Dependencies ──────────────────────────────────────────────────
    # This defines the execution order:
    # Bronze must finish before Silver starts
    # Silver must finish before Validation starts
    # Validation must PASS before Gold starts (blocked if validation fails)
    t0_ingest >> t1_bronze >> t2_silver >> t3_validate >> t4_gold >> success_email
    [t0_ingest, t1_bronze, t2_silver, t3_validate, t4_gold] >> failure_email
