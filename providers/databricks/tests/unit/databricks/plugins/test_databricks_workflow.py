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

import logging
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest

pytest.importorskip("airflow.providers.fab")

from flask import url_for

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.common.compat.sdk import AirflowException, AirflowPlugin
from airflow.providers.databricks.plugins.databricks_workflow import (
    REPAIR_URL_PREFIX,
    DatabricksWorkflowPlugin,
    WorkflowJobRepairAllFailedLink,
    WorkflowJobRepairSingleTaskLink,
    WorkflowJobRunLink,
    _build_repair_url,
    _get_launch_task_key,
    _repair_task,
    get_databricks_task_ids,
    get_launch_task_id,
    store_databricks_job_run_link,
)

from tests_common import RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    from airflow.providers.databricks.plugins.databricks_workflow import (
        RepairDatabricksTasks,
    )

DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "test_run_1"
TASK_INSTANCE_KEY = TaskInstanceKey(dag_id=DAG_ID, task_id=TASK_ID, run_id=RUN_ID, try_number=1)
DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_RUN_ID = 12345
GROUP_ID = "test_group"
LOG = MagicMock()
TASK_MAP = {
    "task1": MagicMock(dag_id=DAG_ID, task_id="task1", databricks_task_key="task_key1"),
    "task2": MagicMock(dag_id=DAG_ID, task_id="task2", databricks_task_key="task_key2"),
}

logger = logging.getLogger(__name__)


def test_get_databricks_task_ids():
    result = get_databricks_task_ids(GROUP_ID, TASK_MAP, LOG)

    expected_ids = ["task_key1", "task_key2"]
    assert result == expected_ids


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
def test_get_dagrun_airflow2():
    from airflow.providers.databricks.plugins.databricks_workflow import _get_dagrun

    session = MagicMock()
    dag = MagicMock(dag_id=DAG_ID)
    session.scalars.return_value.one.return_value = DagRun()

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
    # downstream dependents must also be rerun so upstream-failed tasks resume
    assert mock_hook_instance.repair_run.call_args[0][0]["rerun_dependent_tasks"] is True


@patch("airflow.providers.databricks.plugins.databricks_workflow.DatabricksHook")
def test_repair_task_with_params(mock_databricks_hook):
    mock_hook_instance = mock_databricks_hook.return_value
    mock_hook_instance.get_latest_repair_id.return_value = 100
    mock_hook_instance.repair_run.return_value = 200
    mock_hook_instance.get_run.return_value = {
        "overriding_parameters": {
            "key1": "value1",
            "key2": "value2",
        },
    }

    tasks_to_repair = ["task1", "task2"]
    result = _repair_task(DATABRICKS_CONN_ID, DATABRICKS_RUN_ID, tasks_to_repair, LOG)

    expected_payload = {
        "run_id": DATABRICKS_RUN_ID,
        "rerun_tasks": tasks_to_repair,
        "latest_repair_id": 100,
        "rerun_dependent_tasks": True,
        "overriding_parameters": {
            "key1": "value1",
            "key2": "value2",
        },
    }
    assert result == 200
    mock_hook_instance.get_latest_repair_id.assert_called_once_with(DATABRICKS_RUN_ID)
    mock_hook_instance.get_run.assert_called_once_with(DATABRICKS_RUN_ID)
    mock_hook_instance.repair_run.assert_called_once_with(expected_payload)


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


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
@pytest.mark.db_test
def test_get_task_instance_airflow2():
    from airflow.providers.databricks.plugins.databricks_workflow import get_task_instance
    from airflow.www.app import create_app

    app = create_app(testing=True)
    app.config["SERVER_NAME"] = "localhost"

    with app.app_context():
        operator = Mock()
        operator.dag.dag_id = "dag_id"
        operator.task_id = "task_id"
        dttm = "2022-01-01T00:00:00Z"
        session = Mock()
        dag_run = Mock()
        session.scalars().one_or_none.return_value = dag_run

        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow.DagRun.find", return_value=[dag_run]
        ):
            result = get_task_instance(operator, dttm, session=session)
            assert result == dag_run


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
@pytest.mark.db_test
def test_get_return_url_dag_id_run_id_airflow2():
    from airflow.www.app import create_app

    dag_id = "example_dag"
    run_id = "example_run"

    app = create_app(testing=True)
    app.config["SERVER_NAME"] = "localhost"
    with app.app_context():
        expected_url = url_for("Airflow.grid", dag_id=dag_id, dag_run_id=run_id)
        actual_url = RepairDatabricksTasks._get_return_url(dag_id, run_id)
    assert actual_url == expected_url, f"Expected {expected_url}, got {actual_url}"


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
@pytest.mark.db_test
def test_workflow_job_run_link_airflow2():
    from airflow.www.app import create_app

    app = create_app(testing=True)
    app.config["SERVER_NAME"] = "localhost"

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
                    "airflow.providers.databricks.plugins.databricks_workflow._get_dag"
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


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
@pytest.mark.skipif(
    RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES, reason="Web plugin test doesn't work when not against sources"
)
@pytest.mark.db_test
def test_workflow_job_repair_single_failed_link_airflow2():
    from airflow.www.app import create_app

    app = create_app(testing=True)
    app.config["SERVER_NAME"] = "localhost"
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
                    "airflow.providers.databricks.plugins.databricks_workflow.DagBag.get_dag"
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


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
def test_appbuilder_views_airflow2(plugin):
    assert plugin.appbuilder_views is not None
    assert len(plugin.appbuilder_views) == 1

    repair_view = plugin.appbuilder_views[0]["view"]
    assert isinstance(repair_view, RepairDatabricksTasks)
    assert repair_view.default_view == "repair"


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3.0+")
class TestDatabricksWorkflowPluginAirflow3:
    """Test Databricks Workflow Plugin functionality specific to Airflow 3.x."""

    def test_plugin_operator_extra_links_include_repair(self):
        """All three extra links (incl. repair) are registered in Airflow 3.x."""
        plugin = DatabricksWorkflowPlugin()

        link_types = {type(link).__name__ for link in plugin.operator_extra_links}
        assert link_types == {
            "WorkflowJobRunLink",
            "WorkflowJobRepairAllFailedLink",
            "WorkflowJobRepairSingleTaskLink",
        }

    def test_plugin_registers_fastapi_repair_app(self):
        """Repair is served by a FastAPI app (not Flask-AppBuilder) in Airflow 3.x."""
        plugin = DatabricksWorkflowPlugin()

        assert not getattr(plugin, "appbuilder_views", [])
        assert len(plugin.fastapi_apps) == 1
        app = plugin.fastapi_apps[0]
        assert app["url_prefix"] == REPAIR_URL_PREFIX
        assert app["app"] is not None

    def test_build_repair_url_single_task(self):
        url = _build_repair_url("my_dag", "run 1", "databricks_default", 999, tasks_to_repair=["abc", "def"])
        assert url.startswith(f"{REPAIR_URL_PREFIX}/my_dag/run%201") or REPAIR_URL_PREFIX in url
        assert "databricks_conn_id=databricks_default" in url
        assert "databricks_run_id=999" in url
        assert "tasks_to_repair=abc%2Cdef" in url
        assert "repair_all" not in url

    def test_build_repair_url_repair_all(self):
        url = _build_repair_url("my_dag", "run1", "databricks_default", 999, repair_all=True)
        assert "repair_all=true" in url
        assert "tasks_to_repair" not in url

    def test_repair_single_task_link_uses_endpoint_url(self):
        """The single-task repair link points at the FastAPI endpoint, built from XCom only."""
        link = WorkflowJobRepairSingleTaskLink()
        operator = Mock(databricks_task_key="abc123")
        ti_key = TaskInstanceKey(dag_id="my_dag", task_id="grp.nb", run_id="run1", try_number=1)
        metadata = Mock(conn_id="databricks_default", run_id=555)
        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow._get_launch_metadata_v3",
            return_value=metadata,
        ):
            url = link.get_link(operator, ti_key=ti_key)
        assert REPAIR_URL_PREFIX in url
        assert "tasks_to_repair=abc123" in url
        assert "databricks_run_id=555" in url

    def test_repair_all_link_uses_endpoint_url(self):
        link = WorkflowJobRepairAllFailedLink()
        operator = Mock()
        ti_key = TaskInstanceKey(dag_id="my_dag", task_id="grp.nb", run_id="run1", try_number=1)
        metadata = Mock(conn_id="databricks_default", run_id=555)
        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow._get_launch_metadata_v3",
            return_value=metadata,
        ):
            url = link.get_link(operator, ti_key=ti_key)
        assert REPAIR_URL_PREFIX in url
        assert "repair_all=true" in url

    def test_repair_link_returns_empty_without_metadata(self):
        """When the launch XCom isn't available yet, the link renders empty (no crash)."""
        link = WorkflowJobRepairSingleTaskLink()
        ti_key = TaskInstanceKey(dag_id="my_dag", task_id="grp.nb", run_id="run1", try_number=1)
        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow._get_launch_metadata_v3",
            return_value=None,
        ):
            assert link.get_link(Mock(databricks_task_key="x"), ti_key=ti_key) == ""

    def test_repair_endpoint_calls_repair_and_clear(self):
        """The FastAPI endpoint resolves failed tasks, repairs, clears, and redirects."""
        from fastapi.testclient import TestClient

        from airflow.providers.databricks.plugins import databricks_workflow as m

        m.repair_app.dependency_overrides[m._require_dag_run_edit] = lambda: Mock()
        try:
            with (
                patch.object(m, "DatabricksHook") as mock_hook,
                patch.object(m, "_repair_task") as mock_repair,
                patch.object(m, "_clear_repaired_and_downstream") as mock_clear,
            ):
                mock_hook.return_value.get_run_failed_task_keys.return_value = ["k1", "k2"]
                client = TestClient(m.repair_app)
                resp = client.get(
                    "/my_dag/run1",
                    params={
                        "databricks_conn_id": "databricks_default",
                        "databricks_run_id": 999,
                        "repair_all": "true",
                    },
                    follow_redirects=False,
                )
            assert resp.status_code == 303
            assert mock_repair.call_args.kwargs["tasks_to_repair"] == ["k1", "k2"]
            mock_clear.assert_called_once()
        finally:
            m.repair_app.dependency_overrides.clear()

    def test_repair_endpoint_returns_502_on_databricks_error(self):
        """A Databricks/hook failure surfaces as a clean 502, not a bare 500."""
        from fastapi.testclient import TestClient

        from airflow.providers.databricks.plugins import databricks_workflow as m

        m.repair_app.dependency_overrides[m._require_dag_run_edit] = lambda: Mock()
        try:
            with patch.object(m, "DatabricksHook") as mock_hook:
                mock_hook.return_value.get_run_failed_task_keys.side_effect = Exception("Invalid Token")
                client = TestClient(m.repair_app, raise_server_exceptions=False)
                resp = client.get(
                    "/my_dag/run1",
                    params={
                        "databricks_conn_id": "databricks_default",
                        "databricks_run_id": 1,
                        "repair_all": "true",
                    },
                    follow_redirects=False,
                )
            assert resp.status_code == 502
        finally:
            m.repair_app.dependency_overrides.clear()

    def test_store_databricks_job_run_link_function_works(self):
        """Test that store_databricks_job_run_link works correctly in Airflow 3.x."""
        ti_mock = Mock()
        ti_mock.xcom_push = Mock()

        context = {
            "ti": ti_mock,
            "dag": Mock(dag_id="test_dag"),
            "dag_run": Mock(run_id="test_run"),
            "task": Mock(task_id="test_task"),
        }

        metadata = Mock(conn_id="databricks_default", job_id=12345, run_id=67890)

        with patch("airflow.providers.databricks.plugins.databricks_workflow.DatabricksHook") as mock_hook:
            mock_hook_instance = Mock()
            mock_hook_instance.host = "test-databricks-host"
            mock_hook.return_value = mock_hook_instance

            store_databricks_job_run_link(context, metadata, logger)

            ti_mock.xcom_push.assert_called_once()

            call_args = ti_mock.xcom_push.call_args
            assert call_args[1]["key"] == "databricks_job_run_link"
            assert "test-databricks-host" in call_args[1]["value"]
            assert "12345" in call_args[1]["value"]
            assert "67890" in call_args[1]["value"]
            assert ti_mock.xcom_push.call_count == 1

    def test_workflow_job_run_link_uses_xcom(self):
        """Test that WorkflowJobRunLink.get_link uses XCom in Airflow 3.x."""
        link = WorkflowJobRunLink()
        operator = Mock()
        ti_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        expected_link = "https://test-host/#job/123/run/456"

        with patch("airflow.providers.databricks.plugins.databricks_workflow.XCom") as mock_xcom:
            mock_xcom.get_value.return_value = expected_link

            result = link.get_link(operator, ti_key=ti_key)

            mock_xcom.get_value.assert_called_once_with(ti_key=ti_key, key="databricks_job_run_link")

            assert result == expected_link

    def test_store_databricks_job_run_link_exception_handling(self):
        """Test that exceptions are properly handled in store_databricks_job_run_link."""
        ti_mock = Mock()
        ti_mock.xcom_push = Mock()

        context = {
            "ti": ti_mock,
            "dag": Mock(dag_id="test_dag"),
            "dag_run": Mock(run_id="test_run"),
            "task": Mock(task_id="test_task"),
        }

        metadata = Mock(conn_id="databricks_default", job_id=12345, run_id=67890)

        with patch("airflow.providers.databricks.plugins.databricks_workflow.DatabricksHook") as mock_hook:
            mock_hook_instance = Mock()
            type(mock_hook_instance).host = PropertyMock(side_effect=Exception("Connection failed"))
            mock_hook.return_value = mock_hook_instance

            store_databricks_job_run_link(context, metadata, logger)

            # Verify no XCom was pushed due to the exception
            ti_mock.xcom_push.assert_not_called()


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow < 3.0")
class TestDatabricksWorkflowPluginAirflow2:
    """Test Databricks Workflow Plugin functionality specific to Airflow 2.x."""

    def test_plugin_operator_extra_links_full_functionality(self):
        """Test that all operator_extra_links are present in Airflow 2.x."""
        plugin = DatabricksWorkflowPlugin()

        # In Airflow 2.x, all links should be present including repair links
        assert len(plugin.operator_extra_links) >= 2  # At least job run link + repair links
        link_types = [type(link).__name__ for link in plugin.operator_extra_links]
        assert "WorkflowJobRunLink" in link_types
        # Should have repair links in 2.x
        assert any("Repair" in link_type for link_type in link_types)

    def test_plugin_has_appbuilder_views(self):
        """Test that appbuilder_views are configured for repair functionality in Airflow 2.x."""
        plugin = DatabricksWorkflowPlugin()

        # In Airflow 2.x, appbuilder_views should be present for repair functionality
        assert hasattr(plugin, "appbuilder_views")
        assert plugin.appbuilder_views is not None

    def test_store_databricks_job_run_link_returns_early(self):
        """Test that store_databricks_job_run_link returns early in Airflow 2.x."""
        ti_mock = Mock()
        ti_mock.xcom_push = Mock()

        context = {
            "ti": ti_mock,
            "dag": Mock(dag_id="test_dag"),
            "dag_run": Mock(run_id="test_run"),
            "task": Mock(task_id="test_task"),
        }

        metadata = Mock(conn_id="databricks_default", job_id=12345, run_id=67890)

        store_databricks_job_run_link(context, metadata, logger)

        ti_mock.xcom_push.assert_not_called()

    def test_workflow_job_run_link_uses_legacy_method(self):
        """Test that WorkflowJobRunLink.get_link uses legacy method in Airflow 2.x."""
        link = WorkflowJobRunLink()
        operator = Mock()
        operator.task_group = Mock()
        operator.task_group.group_id = "test_group"

        ti_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        with patch(
            "airflow.providers.databricks.plugins.databricks_workflow.get_task_instance"
        ) as mock_get_ti:
            with patch(
                "airflow.providers.databricks.plugins.databricks_workflow.get_xcom_result"
            ) as mock_get_xcom:
                with patch(
                    "airflow.providers.databricks.plugins.databricks_workflow._get_dag"
                ) as mock_get_dag:
                    with patch(
                        "airflow.providers.databricks.plugins.databricks_workflow.DatabricksHook"
                    ) as mock_hook:
                        mock_get_ti.return_value = Mock(key=ti_key)
                        mock_get_xcom.return_value = Mock(conn_id="conn_id", run_id=1, job_id=1)
                        mock_get_dag.return_value.get_task.return_value = Mock(task_id="test_task")

                        mock_hook_instance = Mock()
                        mock_hook_instance.host = "test-host"
                        mock_hook.return_value = mock_hook_instance

                        result = link.get_link(operator, ti_key=ti_key)

                        # Verify legacy method was used (should contain databricks host)
                        assert "test-host" in result
                        assert "#job/1/run/1" in result
