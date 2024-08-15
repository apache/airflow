# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.hooks.databricks import RunLifeCycleState
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
    _CreateDatabricksWorkflowOperator,
    _flatten_node,
    WorkflowRunMetadata,
)
from airflow.utils import timezone

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2021, 1, 1)


@pytest.fixture
def mock_databricks_hook():
    """Provide a mock DatabricksHook."""
    with patch("airflow.providers.databricks.operators.databricks_workflow.DatabricksHook") as mock_hook:
        yield mock_hook


@pytest.fixture
def context():
    """Provide a mock context object."""
    return MagicMock()


@pytest.fixture
def mock_task_group():
    """Provide a mock DatabricksWorkflowTaskGroup with necessary attributes."""
    mock_group = MagicMock(spec=DatabricksWorkflowTaskGroup)
    mock_group.group_id = "test_group"
    return mock_group

@pytest.fixture
def mock_workflow_run_metadata():
    return MagicMock(spec=WorkflowRunMetadata)


def test_flatten_node():
    """Test that _flatten_node returns a flat list of operators."""
    task_group = MagicMock(spec=DatabricksWorkflowTaskGroup)
    base_operator = MagicMock(spec=BaseOperator)
    task_group.children = {"task1": base_operator, "task2": base_operator}

    result = _flatten_node(task_group)
    assert result == [base_operator, base_operator]


def test_create_workflow_json(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator.create_workflow_json returns the expected JSON."""
    operator = _CreateDatabricksWorkflowOperator(
        task_id="test_task",
        databricks_conn_id="databricks_default",
    )
    operator.task_group = mock_task_group

    task = MagicMock(spec=BaseOperator)
    task._convert_to_databricks_workflow_task = MagicMock(return_value={})
    operator.add_task(task)

    workflow_json = operator.create_workflow_json(context=context)

    assert ".test_group" in workflow_json["name"]
    assert "tasks" in workflow_json
    assert workflow_json["format"] == "MULTI_TASK"
    assert workflow_json["email_notifications"] == {"no_alert_for_skipped_runs": False}
    assert workflow_json["job_clusters"] == []
    assert workflow_json["max_concurrent_runs"] == 1
    assert workflow_json["timeout_seconds"] == 0


def test_create_or_reset_job_existing(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator._create_or_reset_job resets the job if it already exists."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = mock_task_group
    operator._hook.list_jobs.return_value = [{"job_id": 123}]
    operator._hook.create_job.return_value = 123

    job_id = operator._create_or_reset_job(context)
    assert job_id == 123
    operator._hook.reset_job.assert_called_once()


def test_create_or_reset_job_new(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator._create_or_reset_job creates a new job if it does not exist."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = mock_task_group
    operator._hook.list_jobs.return_value = []
    operator._hook.create_job.return_value = 456

    job_id = operator._create_or_reset_job(context)
    assert job_id == 456
    operator._hook.create_job.assert_called_once()


def test_wait_for_job_to_start(mock_databricks_hook):
    """Test that _CreateDatabricksWorkflowOperator._wait_for_job_to_start waits for the job to start."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    mock_hook_instance = mock_databricks_hook.return_value
    mock_hook_instance.get_run_state.side_effect = [
        MagicMock(life_cycle_state=RunLifeCycleState.PENDING.value),
        MagicMock(life_cycle_state=RunLifeCycleState.RUNNING.value),
    ]

    operator._wait_for_job_to_start(123)
    mock_hook_instance.get_run_state.assert_called()


def test_execute(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator.execute runs the task group."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = mock_task_group
    mock_task_group.jar_params = {}
    mock_task_group.python_params = {}
    mock_task_group.spark_submit_params = {}

    mock_hook_instance = mock_databricks_hook.return_value
    mock_hook_instance.run_now.return_value = 789
    mock_hook_instance.list_jobs.return_value = [{"job_id": 123}]
    mock_hook_instance.get_run_state.return_value = MagicMock(
        life_cycle_state=RunLifeCycleState.RUNNING.value
    )

    task = MagicMock(spec=BaseOperator)
    task._convert_to_databricks_workflow_task = MagicMock(return_value={})
    operator.add_task(task)

    result = operator.execute(context)

    assert result == {
        "conn_id": "databricks_default",
        "job_id": 123,
        "run_id": 789,
    }
    mock_hook_instance.run_now.assert_called_once()


def test_execute_invalid_task_group(context):
    """Test that _CreateDatabricksWorkflowOperator.execute raises an exception if the task group is invalid."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = MagicMock()  # Not a DatabricksWorkflowTaskGroup

    with pytest.raises(AirflowException, match="Task group must be a DatabricksWorkflowTaskGroup"):
        operator.execute(context)


@pytest.fixture
def mock_databricks_workflow_operator():
    with patch(
        "airflow.providers.databricks.operators.databricks_workflow._CreateDatabricksWorkflowOperator"
    ) as mock_operator:
        yield mock_operator


def test_task_group_initialization():
    """Test that DatabricksWorkflowTaskGroup initializes correctly."""
    with DAG(dag_id="example_databricks_workflow_dag", schedule=None, start_date=DEFAULT_DATE) as example_dag:
        with DatabricksWorkflowTaskGroup(
            group_id="test_databricks_workflow", databricks_conn_id="databricks_conn"
        ) as task_group:
            task_1 = EmptyOperator(task_id="task1")
            task_1._convert_to_databricks_workflow_task = MagicMock(return_value={})
        assert task_group.group_id == "test_databricks_workflow"
        assert task_group.databricks_conn_id == "databricks_conn"
        assert task_group.dag == example_dag


def test_task_group_exit_creates_operator(mock_databricks_workflow_operator):
    """Test that DatabricksWorkflowTaskGroup creates a _CreateDatabricksWorkflowOperator on exit."""
    with DAG(dag_id="example_databricks_workflow_dag", schedule=None, start_date=DEFAULT_DATE) as example_dag:
        with DatabricksWorkflowTaskGroup(
            group_id="test_databricks_workflow",
            databricks_conn_id="databricks_conn",
        ) as task_group:
            task1 = MagicMock(task_id="task1")
            task1._convert_to_databricks_workflow_task = MagicMock(return_value={})
            task2 = MagicMock(task_id="task2")
            task2._convert_to_databricks_workflow_task = MagicMock(return_value={})

            task_group.add(task1)
            task_group.add(task2)

            task1.set_downstream(task2)

    mock_databricks_workflow_operator.assert_called_once_with(
        dag=example_dag,
        task_group=task_group,
        task_id="launch",
        databricks_conn_id="databricks_conn",
        existing_clusters=[],
        extra_job_params={},
        job_clusters=[],
        max_concurrent_runs=1,
        notebook_params={},
    )


def test_task_group_root_tasks_set_upstream_to_operator(mock_databricks_workflow_operator):
    """Test that tasks added to a DatabricksWorkflowTaskGroup are set upstream to the operator."""
    with DAG(dag_id="example_databricks_workflow_dag", schedule=None, start_date=DEFAULT_DATE):
        with DatabricksWorkflowTaskGroup(
            group_id="test_databricks_workflow1",
            databricks_conn_id="databricks_conn",
        ) as task_group:
            task1 = MagicMock(task_id="task1")
            task1._convert_to_databricks_workflow_task = MagicMock(return_value={})
            task_group.add(task1)

    create_operator_instance = mock_databricks_workflow_operator.return_value
    task1.set_upstream.assert_called_once_with(create_operator_instance)


def test_on_kill(mock_databricks_hook, context, mock_workflow_run_metadata):
    """Test that _CreateDatabricksWorkflowOperator.execute runs the task group."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.workflow_run_metadata = mock_workflow_run_metadata

    RUN_ID = 789

    mock_workflow_run_metadata.conn_id = operator.databricks_conn_id
    mock_workflow_run_metadata.job_id = "123"
    mock_workflow_run_metadata.run_id = RUN_ID

    operator.on_kill()

    operator._hook.cancel_run.assert_called_once_with(RUN_ID)
