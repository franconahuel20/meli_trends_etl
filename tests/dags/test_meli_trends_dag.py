"""
Test suite for the meli_trends_end_to_end DAG.

This test module validates:
1. DAG integrity (no import errors)
2. DAG structure and configuration
3. Task dependencies
4. Default arguments (retries, schedule, etc.)
5. Critical task existence

These tests are designed to ensure production readiness and prevent runtime failures.
"""

import os
import logging
from contextlib import contextmanager

import pytest
from airflow.models import DagBag


# =========================================================
# LOGGING SUPPRESSION (to avoid noisy Airflow logs in tests)
# =========================================================
@contextmanager
def suppress_logging(namespace):
    """
    Temporarily disables logging for a given namespace.
    Useful to keep pytest output clean.
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


# =========================================================
# DAG LOADING HELPERS
# =========================================================
def get_dagbag():
    """
    Loads all DAGs from the Airflow DAG folder.
    """
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def get_import_errors():
    """
    Returns all DAG import errors.
    Each error is returned as (file_path, error_message)
    """
    dag_bag = get_dagbag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", ""))

    return [(None, None)] + [
        (strip_path_prefix(k), v.strip())
        for k, v in dag_bag.import_errors.items()
    ]


def get_dags():
    """
    Returns all loaded DAGs as (dag_id, dag_object, file_path)
    """
    dag_bag = get_dagbag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", ""))

    return [
        (dag_id, dag, strip_path_prefix(dag.fileloc))
        for dag_id, dag in dag_bag.dags.items()
    ]


# =========================================================
# TEST 1: DAG IMPORT VALIDATION
# =========================================================
@pytest.mark.parametrize(
    "rel_path, error",
    get_import_errors(),
    ids=[x[0] for x in get_import_errors()],
)
def test_dag_imports(rel_path, error):
    """
    Ensures all DAG files can be imported without errors.
    """
    if rel_path and error:
        raise Exception(f"{rel_path} failed to import:\n{error}")


# =========================================================
# TEST 2: DAG EXISTENCE
# =========================================================
def test_meli_trends_dag_exists():
    """
    Validates that the target DAG is correctly loaded.
    """
    dag_bag = get_dagbag()
    dag = dag_bag.get_dag(dag_id="meli_trends_end_to_end")

    assert dag is not None, "DAG 'meli_trends_end_to_end' not found"


# =========================================================
# TEST 3: DAG BASIC CONFIGURATION
# =========================================================
def test_dag_configuration():
    """
    Validates DAG-level configuration such as schedule and catchup.
    """
    dag = get_dagbag().get_dag("meli_trends_end_to_end")

    assert dag.schedule_interval == "0 23 * * *"
    assert dag.catchup is False
    assert dag.start_date is not None


# =========================================================
# TEST 4: TASK EXISTENCE
# =========================================================
def test_task_existence():
    """
    Ensures all expected tasks are present in the DAG.
    """
    dag = get_dagbag().get_dag("meli_trends_end_to_end")

    expected_tasks = {
        "load_excel_inputs_to_postgres",
        "get_meli_access_token",
        "extract_meli_trends_to_l1",
        "transform_l1_to_l2",
        "fuzzy_match_l2_to_l3",
        "run_dbt_models",
    }

    dag_tasks = set(task.task_id for task in dag.tasks)

    assert expected_tasks == dag_tasks, "Mismatch in DAG tasks"


# =========================================================
# TEST 5: TASK DEPENDENCIES
# =========================================================
def test_task_dependencies():
    """
    Validates the DAG execution order (critical for ETL correctness).
    """
    dag = get_dagbag().get_dag("meli_trends_end_to_end")

    assert dag.get_task("load_excel_inputs_to_postgres").downstream_task_ids == {
        "get_meli_access_token"
    }

    assert dag.get_task("get_meli_access_token").downstream_task_ids == {
        "extract_meli_trends_to_l1"
    }

    assert dag.get_task("extract_meli_trends_to_l1").downstream_task_ids == {
        "transform_l1_to_l2"
    }

    assert dag.get_task("transform_l1_to_l2").downstream_task_ids == {
        "fuzzy_match_l2_to_l3"
    }

    assert dag.get_task("fuzzy_match_l2_to_l3").downstream_task_ids == {
        "run_dbt_models"
    }


# =========================================================
# TEST 6: RETRIES CONFIGURATION
# =========================================================
@pytest.mark.parametrize(
    "dag_id, dag, fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    Ensures that all DAGs have retries configured (>=2).
    This is critical for production robustness.
    """
    retries = dag.default_args.get("retries", 0)

    assert retries >= 2, f"{dag_id} in {fileloc} must have retries >= 2"


# =========================================================
# TEST 7: TAG VALIDATION
# =========================================================
APPROVED_TAGS = {"etl", "meli", "trends", "data-engineering"}


@pytest.mark.parametrize(
    "dag_id, dag, fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    Ensures DAG has tags and that they belong to an approved list.
    """
    assert dag.tags, f"{dag_id} has no tags"

    invalid_tags = set(dag.tags) - APPROVED_TAGS
    assert not invalid_tags, f"{dag_id} has invalid tags: {invalid_tags}"


# =========================================================
# TEST 8: DBT TASK VALIDATION
# =========================================================
def test_dbt_task_configuration():
    """
    Validates the dbt execution command.
    Ensures critical commands are present.
    """
    dag = get_dagbag().get_dag("meli_trends_end_to_end")
    task = dag.get_task("run_dbt_models")

    command = task.bash_command

    assert "dbt run" in command
    assert "dbt deps" in command