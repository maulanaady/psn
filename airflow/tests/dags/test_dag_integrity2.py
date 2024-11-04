import glob
import importlib.util
import os
import sys

import pytest
from unittest.mock import patch, Mock
from airflow.models import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.dag_cycle_tester import check_cycle

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config")
sys.path.append(CONFIG_PATH)

DAG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "dags/**/*.py"
)  # Note the double asterisk
DAG_FILES = glob.glob(DAG_PATH, recursive=True)  # Ensure recursive search


@pytest.fixture
def mock_telegram_connection():
    mock_conn = Mock()
    mock_conn.password = "mock_token"
    mock_conn.host = "mock_chat_id"
    mock_conn.extra = '{"message_thread_id": 1234}'

    with patch.object(BaseHook, "get_connection", return_value=mock_conn):
        yield mock_conn


def import_dag(dag_file):
    """Import a DAG file."""
    module_name, _ = os.path.splitext(os.path.basename(dag_file))
    mod_spec = importlib.util.spec_from_file_location(module_name, dag_file)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file, mock_telegram_connection):
    """Import DAG files and check for DAG integrity."""
    try:
        module = import_dag(dag_file)
    except Exception as e:
        pytest.fail(f"Failed to import DAG from {dag_file}: {str(e)}")

    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]

    assert dag_objects, f"No DAG found in {dag_file}"

    for dag in dag_objects:
        check_cycle(dag)
        assert (
            dag.schedule_interval is not None
        ), f"DAG {dag.dag_id} should have a schedule interval defined."
        assert (
            dag.start_date is not None
        ), f"DAG {dag.dag_id} should have a start date defined."

        assert len(dag.tasks) > 0, f"DAG {dag.dag_id} should have at least one task."

        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == len(
            set(task_ids)
        ), f"Task IDs should be unique in DAG {dag.dag_id}."
