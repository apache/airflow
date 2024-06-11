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

from datetime import datetime
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.google.cloud.triggers.cloud_composer import (
    CloudComposerAirflowCLICommandTrigger,
    CloudComposerDAGRunTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_PROJECT_ID = "test-project-id"
TEST_LOCATION = "us-central1"
TEST_ENVIRONMENT_ID = "testenvname"
TEST_EXEC_CMD_INFO = {
    "execution_id": "test_id",
    "pod": "test_pod",
    "pod_namespace": "test_namespace",
    "error": "test_error",
}
TEST_COMPOSER_DAG_ID = "test_dag_id"
TEST_START_DATE = datetime(2024, 3, 22, 11, 0, 0)
TEST_END_DATE = datetime(2024, 3, 22, 12, 0, 0)
TEST_STATES = ["success"]
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_POLL_INTERVAL = 10
TEST_IMPERSONATION_CHAIN = "test_impersonation_chain"
TEST_EXEC_RESULT = {
    "output": [{"line_number": 1, "content": "test_content"}],
    "output_end": True,
    "exit_info": {"exit_code": 0, "error": ""},
}


@pytest.fixture
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id="test_conn"),
)
def cli_command_trigger(mock_conn):
    return CloudComposerAirflowCLICommandTrigger(
        project_id=TEST_PROJECT_ID,
        region=TEST_LOCATION,
        environment_id=TEST_ENVIRONMENT_ID,
        execution_cmd_info=TEST_EXEC_CMD_INFO,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        poll_interval=TEST_POLL_INTERVAL,
    )


@pytest.fixture
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id="test_conn"),
)
def dag_run_trigger(mock_conn):
    return CloudComposerDAGRunTrigger(
        project_id=TEST_PROJECT_ID,
        region=TEST_LOCATION,
        environment_id=TEST_ENVIRONMENT_ID,
        composer_dag_id=TEST_COMPOSER_DAG_ID,
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
        allowed_states=TEST_STATES,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        poll_interval=TEST_POLL_INTERVAL,
    )


class TestCloudComposerAirflowCLICommandTrigger:
    def test_serialize(self, cli_command_trigger):
        actual_data = cli_command_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerAirflowCLICommandTrigger",
            {
                "project_id": TEST_PROJECT_ID,
                "region": TEST_LOCATION,
                "environment_id": TEST_ENVIRONMENT_ID,
                "execution_cmd_info": TEST_EXEC_CMD_INFO,
                "gcp_conn_id": TEST_GCP_CONN_ID,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
                "poll_interval": TEST_POLL_INTERVAL,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.cloud_composer.CloudComposerAsyncHook.wait_command_execution_result"
    )
    async def test_run(self, mock_exec_result, cli_command_trigger):
        mock_exec_result.return_value = TEST_EXEC_RESULT

        expected_event = TriggerEvent(
            {
                "status": "success",
                "result": TEST_EXEC_RESULT,
            }
        )
        actual_event = await cli_command_trigger.run().asend(None)

        assert actual_event == expected_event


class TestCloudComposerDAGRunTrigger:
    def test_serialize(self, dag_run_trigger):
        actual_data = dag_run_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerDAGRunTrigger",
            {
                "project_id": TEST_PROJECT_ID,
                "region": TEST_LOCATION,
                "environment_id": TEST_ENVIRONMENT_ID,
                "composer_dag_id": TEST_COMPOSER_DAG_ID,
                "start_date": TEST_START_DATE,
                "end_date": TEST_END_DATE,
                "allowed_states": TEST_STATES,
                "gcp_conn_id": TEST_GCP_CONN_ID,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
                "poll_interval": TEST_POLL_INTERVAL,
            },
        )
        assert actual_data == expected_data
