import os
import sys
import pytest
from unittest.mock import patch, Mock
from airflow.models import DagBag
from airflow.hooks.base import BaseHook
from airflow.utils.dag_cycle_tester import check_cycle

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config")
sys.path.append(CONFIG_PATH)

DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "dags")


@pytest.fixture
def mock_telegram_connection():
    mock_conn = Mock()
    mock_conn.password = "mock_token"
    mock_conn.host = "mock_chat_id"
    mock_conn.extra = '{"message_thread_id": 1234}'

    with patch.object(BaseHook, "get_connection", return_value=mock_conn):
        yield mock_conn


def test_dag_integrity(mock_telegram_connection):
    """Load DAGs and check for integrity using DagBag."""
    try:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
    except ImportError as e:
        pytest.fail(f"ImportError encountered: {e}")

    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"

    # Loop through each loaded DAG to validate its properties
    for dag_id, dag in dag_bag.dags.items():
        check_cycle(dag)

        # Check if the schedule interval is defined
        assert (
            dag.schedule_interval is not None
        ), f"DAG {dag_id} should have a schedule interval defined."

        # Check if the start date is defined
        assert (
            dag.start_date is not None
        ), f"DAG {dag_id} should have a start date defined."

        # Check that the DAG has at least one task
        assert len(dag.tasks) > 0, f"DAG {dag_id} should have at least one task."

        # Check for unique task IDs within the DAG
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == len(
            set(task_ids)
        ), f"Task IDs should be unique in DAG {dag_id}."
