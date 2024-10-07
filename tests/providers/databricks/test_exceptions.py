import pytest
from airflow.providers.databricks.exceptions import AirflowTaskExecutionError, AirflowTaskExecutionTimeout


def test_airflow_task_execution_error():
    """Test if AirflowTaskExecutionError can be raised correctly."""
    with pytest.raises(AirflowTaskExecutionError, match="Task execution failed"):
        raise AirflowTaskExecutionError("Task execution failed")


def test_airflow_task_execution_timeout():
    """Test if AirflowTaskExecutionTimeout can be raised correctly."""
    with pytest.raises(AirflowTaskExecutionTimeout, match="Task execution timed out"):
        raise AirflowTaskExecutionTimeout("Task execution timed out")
