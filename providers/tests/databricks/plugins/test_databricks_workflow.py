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

from unittest.mock import MagicMock, Mock, patch

import pytest
from flask import url_for
from tests_common import RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES

from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstanceKey
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.databricks.plugins.databricks_workflow import (
    DatabricksWorkflowPlugin,
    RepairDatabricksTasks,
    WorkflowJobRepairSingleTaskLink,
    WorkflowJobRunLink,
    _get_dagrun,
    _get_databricks_task_id,
    _get_launch_task_key,
    _repair_task,
    get_databricks_task_ids,
    get_launch_task_id,
    get_task_instance,
)
from airflow.www.app import create_app

DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "test_run_1"
TASK_INSTANCE_KEY = TaskInstanceKey(dag_id=DAG_ID, task_id=TASK_ID, run_id=RUN_ID, try_number=1)
DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_RUN_ID = 12345
GROUP_ID = "test_group"
TASK_MAP = {
    "task1": MagicMock(dag_id=DAG_ID, task_id="task1"),
    "task2": MagicMock(dag_id=DAG_ID, task_id="task2"),
}
LOG = MagicMock()


@pytest.mark.parametrize(
    "task, expected_id",
    [
        (MagicMock(dag_id="dag1", task_id="task.1"), "dag1__task__1"),
        (MagicMock(dag_id="dag2", task_id="task_1"), "dag2__task_1"),
    ],
)
def test_get_databricks_task_id(task, expected_id):
    result = _get_databricks_task_id(task)

    assert result == expected_id


def test_get_databricks_task_ids():
    result = get_databricks_task_ids(GROUP_ID, TASK_MAP, LOG)

    expected_ids = ["test_dag__task1", "test_dag__task2"]
    assert result == expected_ids


def test_get_dagrun():
    session = MagicMock()
    dag = MagicMock(dag_id=DAG_ID)
    session.query.return_value.filter.return_value.first.return_value = DagRun()

    result = _get_dagrun(dag, RUN_ID, session=session)

    assert isinstance(result, DagRun)


@patch("airflow.providers.databricks.plugins.databricks_workflow.DatabricksHook")
def test_repair_task(mock_databricks_hook):
    mock_hook_instance = mock_databricks_hook.return_value
    mock_hook_instance.get_latest_repair_id.return_value = 100
    mock_hook_instance.repair_run.return_value = 200

    tasks_to_repair = ["task1", "task2"]
    result = _repair_task(DATABRICKS_CONN_ID, DATABRICKS_RUN_ID, tasks_to_repair, LOG)

    assert result == 200
    mock_hook_instance.get_latest_repair_id.assert_called_once_with(DATABRICKS_RUN_ID)
    mock_hook_instance.repair_run.assert_called_once()


def test_get_launch_task_id_no_launch_task():
    task_group = MagicMock(get_child_by_label=MagicMock(side_effect=KeyError))
    task_group.parent_group = None

    with pytest.raises(AirflowException):
        get_launch_task_id(task_group)


def test_get_launch_task_key():
    result = _get_launch_task_key(TASK_INSTANCE_KEY, "launch_task")

    assert isinstance(result, TaskInstanceKey)
    assert result.dag_id == TASK_INSTANCE_KEY.dag_id
    assert result.task_id == "launch_task"
    assert result.run_id == TASK_INSTANCE_KEY.run_id


@pytest.fixture(scope="session")
def app():
    app = create_app(testing=True)
    app.config["SERVER_NAME"] = "localhost"

    with app.app_context():
        yield app


@pytest.mark.db_test
def test_get_task_instance(app):
    with app.app_context():
        operator = Mock()
        operator.dag.dag_id = "dag_id"
        operator.task_id = "task_id"
        dttm = "2022-01-01T00:00:00Z"
        session = Mock()
        dag_run = Mock()
        session.query().filter().one_or_none.return_value = dag_run

        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow.DagRun.find", return_value=[dag_run]
        ):
            result = get_task_instance(operator, dttm, session)
            assert result == dag_run


@pytest.mark.db_test
def test_get_return_url_dag_id_run_id(app):
    dag_id = "example_dag"
    run_id = "example_run"

    expected_url = url_for("Airflow.grid", dag_id=dag_id, dag_run_id=run_id)

    with app.app_context():
        actual_url = RepairDatabricksTasks._get_return_url(dag_id, run_id)
    assert actual_url == expected_url, f"Expected {expected_url}, got {actual_url}"


@pytest.mark.db_test
def test_workflow_job_run_link(app):
    with app.app_context():
        link = WorkflowJobRunLink()
        operator = Mock()
        ti_key = Mock()
        ti_key.dag_id = "dag_id"
        ti_key.task_id = "task_id"
        ti_key.run_id = "run_id"
        ti_key.try_number = 1

        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow.get_task_instance"
        ) as mock_get_task_instance:
            with patch(
                "airflow.providers.databricks.plugins.databricks_workflow.get_xcom_result"
            ) as mock_get_xcom_result:
                with patch(
                    "airflow.providers.databricks.plugins.databricks_workflow.airflow_app.dag_bag.get_dag"
                ) as mock_get_dag:
                    mock_connection = Mock()
                    mock_connection.extra_dejson = {"host": "mockhost"}

                    with patch(
                        "airflow.providers.databricks.hooks.databricks.DatabricksHook.get_connection",
                        return_value=mock_connection,
                    ):
                        mock_get_task_instance.return_value = Mock(key=ti_key)
                        mock_get_xcom_result.return_value = Mock(conn_id="conn_id", run_id=1, job_id=1)
                        mock_get_dag.return_value.get_task = Mock(return_value=Mock(task_id="task_id"))

                        result = link.get_link(operator, ti_key=ti_key)
                        assert "https://mockhost/#job/1/run/1" in result


@pytest.mark.skipif(
    RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES, reason="Web plugin test doesn't work when not against sources"
)
@pytest.mark.db_test
def test_workflow_job_repair_single_failed_link(app):
    with app.app_context():
        link = WorkflowJobRepairSingleTaskLink()
        operator = Mock()
        operator.task_group = Mock()
        operator.task_group.group_id = "group_id"
        operator.task_group.get_child_by_label = Mock()
        ti_key = Mock()
        ti_key.dag_id = "dag_id"
        ti_key.task_id = "task_id"
        ti_key.run_id = "run_id"
        ti_key.try_number = 1

        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow.get_task_instance"
        ) as mock_get_task_instance:
            with patch(
                "airflow.providers.databricks.plugins.databricks_workflow.get_xcom_result"
            ) as mock_get_xcom_result:
                with patch(
                    "airflow.providers.databricks.plugins.databricks_workflow.airflow_app.dag_bag.get_dag"
                ) as mock_get_dag:
                    mock_get_task_instance.return_value = Mock(key=ti_key)
                    mock_get_xcom_result.return_value = Mock(conn_id="conn_id", run_id=1)
                    mock_get_dag.return_value.get_task = Mock(return_value=Mock(task_id="task_id"))

                    result = link.get_link(operator, ti_key=ti_key)
                    assert result.startswith("http://localhost/repair_databricks_job")


@pytest.fixture
def plugin():
    return DatabricksWorkflowPlugin()


def test_plugin_is_airflow_plugin(plugin):
    assert isinstance(plugin, AirflowPlugin)


def test_operator_extra_links(plugin):
    for link in plugin.operator_extra_links:
        assert hasattr(link, "get_link")


def test_appbuilder_views(plugin):
    assert plugin.appbuilder_views is not None
    assert len(plugin.appbuilder_views) == 1

    repair_view = plugin.appbuilder_views[0]["view"]
    assert isinstance(repair_view, RepairDatabricksTasks)
    assert repair_view.default_view == "repair"
