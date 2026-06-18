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

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

# Do not run the tests when FAB / Flask is not installed
pytest.importorskip("flask_session")

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.databricks.hooks.databricks import RunLifeCycleState
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
    WorkflowRunMetadata,
    _CreateDatabricksWorkflowOperator,
    _flatten_node,
)
from airflow.providers.standard.operators.empty import EmptyOperator

DEFAULT_DATE = timezone.datetime(2021, 1, 1)

ACCESS_CONTROL_LIST = [
    {
        "user_name": "jsmith@example.com",
        "permission_level": "CAN_MANAGE",
    }
]


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

    task = MagicMock(spec=BaseOperator, task_id="task_1")
    task._convert_to_databricks_workflow_task = MagicMock(return_value={})
    operator.add_task(task.task_id, task)

    workflow_json = operator.create_workflow_json(context=context)

    assert ".test_group" in workflow_json["name"]
    assert "tasks" in workflow_json
    assert workflow_json["format"] == "MULTI_TASK"
    assert workflow_json["email_notifications"] == {"no_alert_for_skipped_runs": False}
    assert workflow_json["job_clusters"] == []
    assert workflow_json["max_concurrent_runs"] == 1
    assert workflow_json["timeout_seconds"] == 0

    assert "access_control_list" not in workflow_json


@pytest.mark.parametrize("access_control_list", [ACCESS_CONTROL_LIST, []])
def test_create_workflow_json_access_control_list(
    mock_databricks_hook, context, mock_task_group, access_control_list
):
    """Test that _CreateDatabricksWorkflowOperator.create_workflow_json includes access_control_list."""
    operator = _CreateDatabricksWorkflowOperator(
        task_id="test_task",
        databricks_conn_id="databricks_default",
        access_control_list=access_control_list,
    )
    operator.task_group = mock_task_group

    task = MagicMock(spec=BaseOperator, task_id="task_1")
    task._convert_to_databricks_workflow_task = MagicMock(return_value={})
    operator.add_task(task.task_id, task)

    workflow_json = operator.create_workflow_json(context=context)

    # Only validate the access_control_list parameter; everything else has been tested above
    assert workflow_json["access_control_list"] == access_control_list


def test_create_or_reset_job_empty_access_control_list(mock_databricks_hook, context, mock_task_group):
    """Test that access_control_list=[] reaches reset_job unchanged."""
    operator = _CreateDatabricksWorkflowOperator(
        task_id="test_task",
        databricks_conn_id="databricks_default",
        access_control_list=[],
    )
    operator.task_group = mock_task_group
    operator._hook.list_jobs.return_value = [{"job_id": 123}]

    operator._create_or_reset_job(context)

    operator._hook.reset_job.assert_called_once()
    _, job_spec = operator._hook.reset_job.call_args.args

    assert "access_control_list" in job_spec
    assert job_spec["access_control_list"] == []


def test_create_or_reset_job_existing(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator._create_or_reset_job resets the job if it already exists."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = mock_task_group
    operator._hook.list_jobs.return_value = [{"job_id": 123}]

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


@pytest.mark.db_test
def test_execute(mock_databricks_hook, context, mock_task_group):
    """Test that _CreateDatabricksWorkflowOperator.execute runs the task group."""
    operator = _CreateDatabricksWorkflowOperator(task_id="test_task", databricks_conn_id="databricks_default")
    operator.task_group = mock_task_group

    mock_hook_instance = mock_databricks_hook.return_value
    mock_hook_instance.run_now.return_value = 789
    mock_hook_instance.list_jobs.return_value = [{"job_id": 123}]
    mock_hook_instance.get_run_state.return_value = MagicMock(
        life_cycle_state=RunLifeCycleState.RUNNING.value
    )

    task = MagicMock(spec=BaseOperator, task_id="task_1")
    task._convert_to_databricks_workflow_task = MagicMock(return_value={})
    operator.add_task(task.task_id, task)

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
        access_control_list=None,
        existing_clusters=[],
        extra_job_params={},
        jar_params=[],
        job_clusters=[],
        max_concurrent_runs=1,
        notebook_params={},
        python_params=[],
        spark_submit_params=[],
    )


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Uses Airflow 3 task SDK TaskGroupContext layout")
def test_task_group_context_cleaned_up_on_internal_exception():
    """
    Regression test for GH-42164.

    When DatabricksWorkflowTaskGroup.__exit__ raises (e.g. an added task does not
    support conversion), super().__exit__ must still run so TaskGroupContext does
    not leak the workflow group onto the global stack and break later DAGs with
    "Cannot mix TaskGroups from different DAGs".
    """
    from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

    TaskGroupContext._context.clear()

    with pytest.raises(AirflowException, match="does not support conversion"):  # noqa: PT012 raise happens on context exit
        with DAG(dag_id="example_databricks_workflow_dag_err", schedule=None, start_date=DEFAULT_DATE):
            with DatabricksWorkflowTaskGroup(
                group_id="test_databricks_workflow_err", databricks_conn_id="databricks_conn"
            ):
                # EmptyOperator does not implement _convert_to_databricks_workflow_task,
                # which makes DatabricksWorkflowTaskGroup.__exit__ raise mid-way.
                EmptyOperator(task_id="not_convertible")

    assert not TaskGroupContext._context, "TaskGroupContext leaked the workflow task group"


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


class TestWorkflowDependsOn:
    """End-to-end coverage that ``depends_on`` references the *parent's* ``task_key``.

    Regression coverage for issue apache/airflow#47614 (root cause fixed by #48492).
    Each test builds a real ``DAG`` + ``DatabricksWorkflowTaskGroup`` populated with
    real ``DatabricksNotebookOperator`` tasks (no operator mocks), then drives
    ``_CreateDatabricksWorkflowOperator.create_workflow_json`` and asserts the
    resulting ``tasks[*]['depends_on']`` payload.
    """

    DAG_ID = "test_depends_on_dag"
    GROUP_ID = "wf_group"
    CONN_ID = "databricks_conn"

    @staticmethod
    def _build_notebook(task_id: str, **kwargs) -> DatabricksNotebookOperator:
        return DatabricksNotebookOperator(
            task_id=task_id,
            notebook_path=f"/path/{task_id}",
            source="WORKSPACE",
            **kwargs,
        )

    def _expected_default_key(self, group_task_id: str) -> str:
        full_task_id = f"{self.GROUP_ID}.{group_task_id}"
        return hashlib.md5(f"{self.DAG_ID}__{full_task_id}".encode()).hexdigest()

    def _launch_task(self, dag: DAG) -> _CreateDatabricksWorkflowOperator:
        launch = dag.task_dict[f"{self.GROUP_ID}.launch"]
        assert isinstance(launch, _CreateDatabricksWorkflowOperator)
        return launch

    @staticmethod
    def _tasks_by_key(workflow_json: dict) -> dict:
        return {t["task_key"]: t for t in workflow_json["tasks"]}

    def test_depends_on_uses_parent_key_default_keys(self):
        """``task_A >> task_B`` — ``task_B.depends_on`` references ``task_A``'s key."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a")
                task_b = self._build_notebook("task_b")
                task_a >> task_b

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        a_key = self._expected_default_key("task_a")
        b_key = self._expected_default_key("task_b")

        assert set(tasks_by_key) == {a_key, b_key}
        assert tasks_by_key[a_key]["depends_on"] == []
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": a_key}]

    def test_depends_on_uses_parent_key_custom_parent_key(self):
        """An explicit ``databricks_task_key`` on the parent flows into ``depends_on``."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a", databricks_task_key="custom_a")
                task_b = self._build_notebook("task_b")
                task_a >> task_b

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        b_key = self._expected_default_key("task_b")

        assert "custom_a" in tasks_by_key
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": "custom_a"}]

    def test_depends_on_falls_back_to_hash_when_parent_key_too_long(self):
        """A >100-char explicit key is rejected; both task and ``depends_on`` use the hash."""
        too_long_key = "x" * 101
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a", databricks_task_key=too_long_key)
                task_b = self._build_notebook("task_b")
                task_a >> task_b

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        a_key = self._expected_default_key("task_a")
        b_key = self._expected_default_key("task_b")

        assert too_long_key not in tasks_by_key
        assert a_key in tasks_by_key
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": a_key}]

    def test_depends_on_diamond_dependency(self):
        """``A >> [B, C] >> D`` — D depends on both B and C; B and C each depend only on A."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a")
                task_b = self._build_notebook("task_b")
                task_c = self._build_notebook("task_c")
                task_d = self._build_notebook("task_d")
                task_a >> [task_b, task_c] >> task_d

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        a_key = self._expected_default_key("task_a")
        b_key = self._expected_default_key("task_b")
        c_key = self._expected_default_key("task_c")
        d_key = self._expected_default_key("task_d")

        assert tasks_by_key[a_key]["depends_on"] == []
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": a_key}]
        assert tasks_by_key[c_key]["depends_on"] == [{"task_key": a_key}]
        d_parent_keys = {entry["task_key"] for entry in tasks_by_key[d_key]["depends_on"]}
        assert d_parent_keys == {b_key, c_key}

    def test_depends_on_fan_out_dependency(self):
        """``A >> [B, C]`` — both downstreams reference A's key only."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a")
                task_b = self._build_notebook("task_b")
                task_c = self._build_notebook("task_c")
                task_a >> [task_b, task_c]

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        a_key = self._expected_default_key("task_a")
        b_key = self._expected_default_key("task_b")
        c_key = self._expected_default_key("task_c")

        assert tasks_by_key[a_key]["depends_on"] == []
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": a_key}]
        assert tasks_by_key[c_key]["depends_on"] == [{"task_key": a_key}]

    def test_root_tasks_have_empty_depends_on(self):
        """Root tasks' Airflow upstream is the launch task; that must never appear in ``depends_on``."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                root_a = self._build_notebook("root_a")
                root_b = self._build_notebook("root_b")
                self._build_notebook("downstream").set_upstream([root_a, root_b])

        launch_task = self._launch_task(dag)
        # Sanity: both roots actually have the launch task as an Airflow upstream.
        for root_task_id in (f"{self.GROUP_ID}.root_a", f"{self.GROUP_ID}.root_b"):
            assert launch_task.task_id in dag.task_dict[root_task_id].upstream_task_ids

        tasks_by_key = self._tasks_by_key(launch_task.create_workflow_json())
        root_a_key = self._expected_default_key("root_a")
        root_b_key = self._expected_default_key("root_b")

        assert tasks_by_key[root_a_key]["depends_on"] == []
        assert tasks_by_key[root_b_key]["depends_on"] == []

    def test_depends_on_filters_out_external_upstream(self):
        """An Airflow upstream outside the workflow group must not appear in ``depends_on``."""
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            external_op = EmptyOperator(task_id="external_op")
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                dbx_task = self._build_notebook("dbx_task")
            external_op >> dbx_task

        tasks_by_key = self._tasks_by_key(self._launch_task(dag).create_workflow_json())
        dbx_key = self._expected_default_key("dbx_task")

        assert tasks_by_key[dbx_key]["depends_on"] == []


class TestWorkflowDependsOnWirePayload:
    """Wire-boundary coverage: the spec sent to the Databricks Jobs API carries ``depends_on``.

    :class:`TestWorkflowDependsOn` asserts the in-process ``create_workflow_json`` payload.
    These tests assert the *wire* payload — what ``_create_or_reset_job`` actually hands to
    ``DatabricksHook.create_job`` (new job) or ``DatabricksHook.reset_job`` (existing job),
    which is what the Databricks REST API receives.
    """

    DAG_ID = "test_depends_on_wire_dag"
    GROUP_ID = "wf_group"
    CONN_ID = "databricks_conn"

    @staticmethod
    def _build_notebook(task_id: str, **kwargs) -> DatabricksNotebookOperator:
        return DatabricksNotebookOperator(
            task_id=task_id,
            notebook_path=f"/path/{task_id}",
            source="WORKSPACE",
            **kwargs,
        )

    def _expected_default_key(self, group_task_id: str) -> str:
        full_task_id = f"{self.GROUP_ID}.{group_task_id}"
        return hashlib.md5(f"{self.DAG_ID}__{full_task_id}".encode()).hexdigest()

    def _launch_task(self, dag: DAG) -> _CreateDatabricksWorkflowOperator:
        launch = dag.task_dict[f"{self.GROUP_ID}.launch"]
        assert isinstance(launch, _CreateDatabricksWorkflowOperator)
        return launch

    @staticmethod
    def _tasks_by_key(workflow_json: dict) -> dict:
        return {t["task_key"]: t for t in workflow_json["tasks"]}

    def _build_two_task_dag(self) -> DAG:
        with DAG(dag_id=self.DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
            with DatabricksWorkflowTaskGroup(group_id=self.GROUP_ID, databricks_conn_id=self.CONN_ID):
                task_a = self._build_notebook("task_a")
                task_b = self._build_notebook("task_b")
                task_a >> task_b
        return dag

    def _assert_parent_depends_on(self, job_spec: dict) -> None:
        tasks_by_key = self._tasks_by_key(job_spec)
        a_key = self._expected_default_key("task_a")
        b_key = self._expected_default_key("task_b")

        assert len(job_spec["tasks"]) == 2
        assert set(tasks_by_key) == {a_key, b_key}
        assert tasks_by_key[a_key]["depends_on"] == []
        assert tasks_by_key[b_key]["depends_on"] == [{"task_key": a_key}]

    def test_create_job_payload_carries_parent_depends_on(self, mock_databricks_hook):
        """No existing job → ``create_job`` receives a spec whose ``depends_on`` references the parent key."""
        launch_task = self._launch_task(self._build_two_task_dag())
        launch_task._hook.list_jobs.return_value = []
        launch_task._hook.create_job.return_value = 999

        launch_task._create_or_reset_job(context=MagicMock())

        launch_task._hook.create_job.assert_called_once()
        launch_task._hook.reset_job.assert_not_called()
        (job_spec,) = launch_task._hook.create_job.call_args.args
        self._assert_parent_depends_on(job_spec)

    def test_reset_job_payload_carries_parent_depends_on(self, mock_databricks_hook):
        """Existing job → ``reset_job`` receives a spec whose ``depends_on`` references the parent key."""
        launch_task = self._launch_task(self._build_two_task_dag())
        launch_task._hook.list_jobs.return_value = [{"job_id": 42}]

        launch_task._create_or_reset_job(context=MagicMock())

        launch_task._hook.reset_job.assert_called_once()
        launch_task._hook.create_job.assert_not_called()
        job_id, job_spec = launch_task._hook.reset_job.call_args.args
        assert job_id == 42
        self._assert_parent_depends_on(job_spec)
