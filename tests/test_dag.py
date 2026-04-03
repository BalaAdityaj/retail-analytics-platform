# ─── tests/test_dag.py ────────────────────────────────────────────────────────
# Basic tests to validate DAG structure
# Runs automatically in CI pipeline on every push
# ─────────────────────────────────────────────────────────────────────────────

import pytest
import sys
import os

# Add dags folder to path so we can import the DAG
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

def test_dag_imports_successfully():
    """Test that DAG file has no syntax errors"""
    try:
        import retail_pipeline_dag
        assert True
    except ImportError as e:
        pytest.fail(f"DAG import failed: {e}")

def test_dag_has_correct_id():
    """Test DAG ID is correct"""
    import retail_pipeline_dag
    assert retail_pipeline_dag.dag.dag_id == "retail_medallion_pipeline"

def test_dag_has_five_tasks():
    """Test DAG has all required tasks"""
    import retail_pipeline_dag
    task_ids = list(retail_pipeline_dag.dag.task_ids)
    print(f"Tasks found: {task_ids}")

    required_tasks = [
        "incremental_ingestion",
        "bronze_load",
        "silver_transformation",
        "validation_checks",
        "gold_aggregation"
    ]
    for task in required_tasks:
        assert task in task_ids, f"Missing task: {task}"

def test_dag_retries_configured():
    """Test retries are set to 3"""
    import retail_pipeline_dag
    retries = retail_pipeline_dag.dag.default_args.get("retries")
    assert retries == 3, f"Expected retries=3, got {retries}"

def test_dag_has_correct_schedule():
    """Test DAG runs daily"""
    import retail_pipeline_dag
    schedule = retail_pipeline_dag.dag.schedule_interval
    assert schedule == "0 6 * * *", f"Unexpected schedule: {schedule}"

def test_task_dependencies():
    """Test tasks run in correct order"""
    import retail_pipeline_dag
    dag = retail_pipeline_dag.dag

    # Get downstream tasks for each task
    t0 = dag.get_task("incremental_ingestion")
    t1 = dag.get_task("bronze_load")
    t2 = dag.get_task("silver_transformation")
    t3 = dag.get_task("validation_checks")
    t4 = dag.get_task("gold_aggregation")

    # Check dependency chain
    assert "bronze_load" in [t.task_id for t in t0.downstream_list]
    assert "silver_transformation" in [t.task_id for t in t1.downstream_list]
    assert "validation_checks" in [t.task_id for t in t2.downstream_list]
    assert "gold_aggregation" in [t.task_id for t in t3.downstream_list]

    print("✅ All task dependencies correct")