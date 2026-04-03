 
markdown# Cloud-Native Omni-Channel Retail Analytics Platform
## Capstone Project — Stack Route / NIIT 2025-26

---

## Architecture
ADLS Gen2 (rawdataset container)
↓
medallion.bronze  →  Raw Delta tables + ingestion_log
↓
medallion.silver  →  Cleaned + enriched + DQ validated
↓
medallion.gold    →  Dimensional model + KPIs + governed views

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks (PySpark) |
| Table format | Delta Lake |
| Orchestration | Apache Airflow |
| Version control | GitHub |
| CI/CD | GitHub Actions |

---

## Pipeline Orchestration

DAG: `retail_medallion_pipeline`
Schedule: Daily 6:00 AM IST
Retries: 3 with 5 min delay
SLA: 2 hours per task

### Task Flow
incremental_ingestion
↓
bronze_load
↓
silver_transformation
↓
validation_checks
↓
gold_aggregation
↓
success_email / failure_email

---

## Datasets

| Table | Rows |
|-------|------|
| sales_transactions | 1,000,500 |
| product_master | 500 |
| store_master | 100 |
| customer_data | 100,000 |
| inventory_data | 2,500 |
| clickstream_events | 1,500,000 |

---

## Data Quality Rules

| Rule | Result |
|------|--------|
| quantity > 0 | PASS |
| unit_price > 0 | PASS |
| total_amount >= 0 | PASS |
| stock_on_hand >= 0 | PASS |
| selling_price >= cost_price | FAIL — 10 records (business issue) |
| gross_margin threshold | FAIL — 30,558 records (business issue) |

---

## ADF Decision

ADF functions were implemented directly in PySpark:

| ADF Feature | Our Implementation |
|-------------|-------------------|
| Metadata logging | bronze.ingestion_log |
| Watermark tracking | ingestion_timestamp filter |
| Pipeline validation | dq_rule_log + dq_reject_table |
| Parameterization | Airflow Variables |

Reason: Tighter integration with Delta Lake and
Databricks. ADF would add complexity without benefit
given the current architecture.

In production with external sources (SAP, Salesforce,
on-premise SQL Server) ADF would be added as an
upstream data mover feeding into ADLS.

---

## Environment Separation

| Branch | Environment | Schema |
|--------|-------------|--------|
| feature/* | DEV | medallion_dev |
| main | TEST | medallion_test |
| release/* | PROD | medallion_prod |

---

## Incremental Ingestion

Infrastructure built and ready:
- Watermark stored in: `medallion.bronze.ingestion_log`
- Filter column: `ingestion_timestamp`
- Fact tables: incremental load pattern implemented
- Dimension tables: full load (no timestamp column)

Note: Static dataset used for capstone. Pattern is
ready for live source connection in production.

---

## Repository Structure