import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule


EMAIL_TO = "balaadityaj@gmail.com"


def send_success_email():
    send_email(
        to=EMAIL_TO,
        subject="Airflow DAG Success",
        html_content=(
            "<h3>Pipeline executed successfully.</h3>"
            "<p>ADF pipeline completed.</p>"
        ),
    )


def send_failure_email():
    send_email(
        to=EMAIL_TO,
        subject="Airflow DAG Failed",
        html_content="<h3>Pipeline Failed</h3><p>ADF pipeline execution failed. Check logs.</p>",
    )


def run_adf_pipeline():
    logging.info("Triggering ADF pipeline...")
    time.sleep(5)
    logging.info("ADF pipeline completed successfully")


default_args = {
    "owner": "Aditya",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": [EMAIL_TO],
    "email_on_failure": True,
    "email_on_retry": True,
}


with DAG(
    dag_id="retail_adf_pipeline_final",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["adf", "airflow"],
) as dag:
    adf_task = PythonOperator(
        task_id="run_adf_pipeline",
        python_callable=run_adf_pipeline,
    )

    success_email = PythonOperator(
        task_id="success_email",
        python_callable=send_success_email,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_email = PythonOperator(
        task_id="failure_email",
        python_callable=send_failure_email,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    adf_task >> success_email
    adf_task >> failure_email
