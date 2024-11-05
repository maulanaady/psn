"""
This test is to check if DAG definition using custom DAG class
with context manager or not.
Pass test if dag definition is using context manager, and failed if not.
"""

import os
import sys
import glob
import ast
import pytest

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config")
sys.path.append(CONFIG_PATH)

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")

DAG_FILES = glob.glob(DAG_PATH, recursive=True)  # Ensure recursive search


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


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_definition(dag_file):
    """Test if each DAG file is using a context manager for DefaultDAG definition."""
    result = is_dag_defined_in_context_manager(dag_file)
    assert result is True, f"{dag_file} does not use a context manager for DefaultDAG."
