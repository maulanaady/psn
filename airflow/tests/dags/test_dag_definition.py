"""
This test is to check:
- If DAG definition using custom DAG class with context manager or not.
  Pass test if dag definition is using context manager, and failed if not.
- If task_id naming (hardcoded) style using camel_case.
  Pass test if it using camel_case style, and failed if not
"""

import os
import sys
import glob
import re
import ast
import pytest

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config")
sys.path.append(CONFIG_PATH)

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")

DAG_FILES = glob.glob(DAG_PATH, recursive=True)  # Ensure recursive search

# Exclude airflow_log_cleanup.py DAG file
DAG_FILES = [
    dag_file
    for dag_file in DAG_FILES
    if not dag_file.endswith("airflow_log_cleanup.py")
]

# Updated regex pattern to allow snake_case, and patterns like xxx_xx
TASK_ID_REGEX = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


def is_dag_defined_in_context_manager(file_path):
    """Check if the DAG in the given file is defined using a context manager with DefaultDAG."""
    with open(file_path, "r") as file:
        tree = ast.parse(file.read(), filename=file_path)
        for node in ast.walk(tree):
            # Check for 'with' statements specifically using DefaultDAG
            if isinstance(node, ast.With):
                for item in node.items:
                    if (
                        isinstance(item.context_expr, ast.Call)
                        and getattr(item.context_expr.func, "id", "") == "DefaultDAG"
                    ):
                        return True
        # If we find any assignment to 'dag' of type DefaultDAG outside a 'with' block, return False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                if any(
                    isinstance(target, ast.Name) and target.id == "dag"
                    for target in node.targets
                ):
                    if (
                        isinstance(node.value, ast.Call)
                        and getattr(node.value.func, "id", "") == "DefaultDAG"
                    ):
                        return False
    return None


def check_task_id_naming_convention(file_path):
    """Check if each task_id in the DAG file uses snake_case."""
    with open(file_path, "r") as file:
        tree = ast.parse(file.read(), filename=file_path)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                for keyword in node.keywords:
                    if keyword.arg == "task_id":
                        task_id_value = (
                            keyword.value.s
                            if isinstance(keyword.value, ast.Constant)
                            else ""
                        )
                        if not TASK_ID_REGEX.match(task_id_value):
                            return False, task_id_value

            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if (
                        isinstance(decorator, ast.Call)
                        and isinstance(decorator.func, ast.Name)
                        and decorator.func.id == "task"
                    ):
                        if not decorator.keywords:
                            continue
                        for kw in decorator.keywords:
                            if kw.arg == "task_id":
                                task_id_value = (
                                    kw.value.s
                                    if isinstance(kw.value, ast.Constant)
                                    else ""
                                )
                                if not TASK_ID_REGEX.match(task_id_value):
                                    return False, task_id_value

    return True, None


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_definition(dag_file):
    """Test if each DAG file is using a context manager for DefaultDAG definition."""
    result = is_dag_defined_in_context_manager(dag_file)
    assert result is True, f"{dag_file} does not use a context manager for DefaultDAG."


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_task_id_naming_convention(dag_file):
    """Test if each DAG file is following task_id naming conventions."""
    result, invalid_task_id = check_task_id_naming_convention(dag_file)
    assert result is True, (
        f"{dag_file} contains task_id '{invalid_task_id}' that does"
        "  not follow naming conventions. Use snake_case naming instead"
    )
