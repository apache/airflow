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

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.constants import __version__
from packaging.version import parse

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.utils.openlineage import (
    _get_parent_run_metadata,
    generate_openlineage_events_from_dbt_cloud_run,
)
from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

TASK_ID = "dbt_test"
DAG_ID = "dbt_dag"
TASK_UUID = "01481cfa-0ff7-3692-9bba-79417cf498c2"
DAG_UUID = "01481cfa-1a1a-2b2b-3c3c-79417cf498c2"


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data


def emit_event(event):
    # since 1.15.0 there was v2 facets introduced
    if parse(__version__) >= parse("1.15.0"):
        assert event.run.facets["parent"].run.runId == TASK_UUID
        assert event.run.facets["parent"].job.name == f"{DAG_ID}.{TASK_ID}"
    else:
        assert event.run.facets["parent"].run["runId"] == TASK_UUID
        assert event.run.facets["parent"].job["name"] == f"{DAG_ID}.{TASK_ID}"
    assert event.job.namespace == "default"
    assert event.job.name.startswith("SANDBOX.TEST_SCHEMA.test_project")

    if len(event.inputs) > 0:
        assert event.inputs[0].facets["dataSource"].name == "snowflake://gp21411.us-east-1.aws"
        assert event.inputs[0].facets["dataSource"].uri == "snowflake://gp21411.us-east-1.aws"
        assert event.inputs[0].facets["schema"].fields[0].name.upper() == "ID"
        if event.inputs[0].name == "SANDBOX.TEST_SCHEMA.my_first_dbt_model":
            assert event.inputs[0].facets["schema"].fields[0].type.upper() == "NUMBER"
    if len(event.outputs) > 0:
        assert event.outputs[0].facets["dataSource"].name == "snowflake://gp21411.us-east-1.aws"
        assert event.outputs[0].facets["dataSource"].uri == "snowflake://gp21411.us-east-1.aws"
        assert event.outputs[0].facets["schema"].fields[0].name.upper() == "ID"
        if event.outputs[0].name == "SANDBOX.TEST_SCHEMA.my_first_dbt_model":
            assert event.outputs[0].facets["schema"].fields[0].type.upper() == "NUMBER"


def read_file_json(file):
    with open(file) as f:
        json_data = json.loads(f.read())
        return json_data


def get_dbt_artifact(*args, **kwargs):
    json_file = None
    if "catalog" in kwargs["path"]:
        json_file = Path(__file__).parents[1] / "test_data" / "catalog.json"
    elif "manifest" in kwargs["path"]:
        json_file = Path(__file__).parents[1] / "test_data" / "manifest.json"
    elif "run_results" in kwargs["path"]:
        json_file = Path(__file__).parents[1] / "test_data" / "run_results.json"

    if json_file is not None:
        return MockResponse(read_file_json(json_file))
    return None


@pytest.mark.parametrize(
    ("value", "is_error"),
    [
        ("1.99.0", True),
        ("2.0.0", True),
        ("2.3.0", True),
        ("2.5.0", False),
        ("2.99.0", False),
    ],
)
def test_previous_version_openlineage_provider(value, is_error):
    """When using OpenLineage, the dbt-cloud provider now depends on openlineage provider >= 2.4"""

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return value
        raise Exception("Unexpected package")

    mock_operator = MagicMock()
    mock_task_instance = MagicMock()

    expected_err = (
        f"OpenLineage provider version `{value}` is lower than required `2.5.0`, "
        "skipping function `generate_openlineage_events_from_dbt_cloud_run` execution"
    )

    if is_error:
        with patch("importlib.metadata.version", side_effect=_mock_version):
            with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
                generate_openlineage_events_from_dbt_cloud_run(mock_operator, mock_task_instance)
    else:
        with patch("importlib.metadata.version", side_effect=_mock_version):
            # Error that would certainly not happen on version checking
            mock_operator.hook.get_job_run.side_effect = ZeroDivisionError("error for test")
            with pytest.raises(ZeroDivisionError, match="error for test"):
                generate_openlineage_events_from_dbt_cloud_run(mock_operator, mock_task_instance)


def test_get_parent_run_metadata():
    logical_date = timezone.datetime(2025, 1, 1)
    dr = MagicMock(logical_date=logical_date, clear_number=0)
    mock_ti = MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,
        dag_run=dr,
    )
    mock_ti.get_template_context.return_value = {"dag_run": dr}

    result = _get_parent_run_metadata(mock_ti)

    assert result.run_id == "01941f29-7c00-7087-8906-40e512c257bd"
    assert result.job_namespace == namespace()
    assert result.job_name == "dag_id.task_id"
    assert result.root_parent_run_id == "01941f29-7c00-743e-b109-28b18d0a19c5"
    assert result.root_parent_job_namespace == namespace()
    assert result.root_parent_job_name == "dag_id"


class TestGenerateOpenLineageEventsFromDbtCloudRun:
    @patch("importlib.metadata.version", return_value="3.0.0")
    @patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
    @patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.build_task_instance_run_id")
    @patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.build_dag_run_id")
    @patch.object(DbtCloudHook, "get_job_run")
    @patch.object(DbtCloudHook, "get_project")
    @patch.object(DbtCloudHook, "get_job_run_artifact")
    def test_generate_events(
        self,
        mock_get_job_run_artifact,
        mock_get_project,
        mock_get_job_run,
        mock_build_dag_run_id,
        mock_build_task_instance_run_id,
        mock_get_openlineage_listener,
        mock_version,
    ):
        mock_operator = MagicMock(spec=DbtCloudRunJobOperator)
        mock_operator.account_id = None

        mock_hook = DbtCloudHook()
        mock_operator.hook = mock_hook

        mock_get_job_run.return_value.json.return_value = read_file_json(
            Path(__file__).parents[1] / "test_data" / "job_run.json"
        )
        mock_get_project.return_value.json.return_value = {
            "data": {
                "connection": {
                    "type": "snowflake",
                    "name": "conn_name",
                    "details": {
                        "account": "gp21411.us-east-1",
                        "database": "SANDBOX",
                        "warehouse": "HUMANS",
                        "allow_sso": False,
                        "client_session_keep_alive": False,
                        "role": None,
                    },
                }
            }
        }
        mock_get_job_run_artifact.side_effect = get_dbt_artifact
        mock_operator.task_id = TASK_ID
        mock_operator.run_id = 188471607

        mock_task_instance = MagicMock()
        mock_task_instance.task_id = TASK_ID
        mock_task_instance.dag_id = DAG_ID
        mock_task_instance.dag_run.clear_number = 0

        mock_adapter = MagicMock()

        mock_adapter.emit.side_effect = emit_event
        mock_get_openlineage_listener.return_value.adapter = mock_adapter

        mock_build_task_instance_run_id.return_value = TASK_UUID
        mock_build_dag_run_id.return_value = DAG_UUID
        generate_openlineage_events_from_dbt_cloud_run(mock_operator, task_instance=mock_task_instance)
        assert mock_adapter.emit.call_count == 4

    def test_do_not_raise_error_if_runid_not_set_on_operator(self):
        operator = DbtCloudRunJobOperator(task_id="dbt-job-runid-taskid", job_id=1500)
        assert operator.run_id is None
        assert operator.get_openlineage_facets_on_complete(MagicMock()) == OperatorLineage()
