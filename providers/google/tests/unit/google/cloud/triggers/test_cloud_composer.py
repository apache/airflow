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

from datetime import datetime, timezone
from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.google.cloud.triggers.cloud_composer import (
    CloudComposerAirflowCLICommandTrigger,
    CloudComposerDAGRunTrigger,
    CloudComposerExternalTaskTrigger,
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
TEST_COMPOSER_DAG_RUN_ID = "scheduled__2024-05-22T11:10:00+00:00"
TEST_COMPOSER_EXTERNAL_TASK_IDS = ["test_external_task_id"]
TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID = "test_external_task_group_id"
TEST_START_DATE = datetime(2024, 3, 22, 11, 0, 0)
TEST_END_DATE = datetime(2024, 3, 22, 12, 0, 0)
TEST_ALLOWED_STATES = ["success"]
TEST_SKIPPED_STATES = ["skipped"]
TEST_FAILED_STATES = ["failed"]
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_POLL_INTERVAL = 10
TEST_COMPOSER_AIRFLOW_VERSION = 3
TEST_USE_REST_API = True
TEST_IMPERSONATION_CHAIN = "test_impersonation_chain"
TEST_EXEC_RESULT = {
    "output": [{"line_number": 1, "content": "test_content"}],
    "output_end": True,
    "exit_info": {"exit_code": 0, "error": ""},
}


def build_dag_runs_result(composer_airflow_version: int, dag_runs: list[tuple[str, str]]) -> dict:
    date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
    return {
        "dag_runs": [
            {
                "dag_id": TEST_COMPOSER_DAG_ID,
                "dag_run_id": f"scheduled__{index}",
                "state": state,
                date_key: logical_date,
            }
            for index, (state, logical_date) in enumerate(dag_runs)
        ],
        "total_entries": len(dag_runs),
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
    with pytest.warns(AirflowProviderDeprecationWarning):
        return CloudComposerDAGRunTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_LOCATION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            composer_dag_run_id=TEST_COMPOSER_DAG_RUN_ID,
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
            allowed_states=TEST_ALLOWED_STATES,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poll_interval=TEST_POLL_INTERVAL,
            composer_airflow_version=TEST_COMPOSER_AIRFLOW_VERSION,
            use_rest_api=TEST_USE_REST_API,
        )


@pytest.fixture
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id="test_conn"),
)
def external_task_trigger(mock_conn):
    return CloudComposerExternalTaskTrigger(
        project_id=TEST_PROJECT_ID,
        region=TEST_LOCATION,
        environment_id=TEST_ENVIRONMENT_ID,
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
        allowed_states=TEST_ALLOWED_STATES,
        skipped_states=TEST_SKIPPED_STATES,
        failed_states=TEST_FAILED_STATES,
        composer_external_dag_id=TEST_COMPOSER_DAG_ID,
        composer_external_task_ids=TEST_COMPOSER_EXTERNAL_TASK_IDS,
        composer_external_task_group_id=TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        poll_interval=TEST_POLL_INTERVAL,
        composer_airflow_version=TEST_COMPOSER_AIRFLOW_VERSION,
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
                "composer_dag_run_id": TEST_COMPOSER_DAG_RUN_ID,
                "start_date": TEST_START_DATE,
                "end_date": TEST_END_DATE,
                "allowed_states": TEST_ALLOWED_STATES,
                "gcp_conn_id": TEST_GCP_CONN_ID,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
                "poll_interval": TEST_POLL_INTERVAL,
                "composer_airflow_version": TEST_COMPOSER_AIRFLOW_VERSION,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.cloud_composer.asyncio.sleep", new_callable=AsyncMock
    )
    async def test_trigger_keeps_polling_when_only_out_of_window_runs(
        self, mock_sleep, composer_airflow_version, use_rest_api
    ):
        hook = AsyncMock()
        environment = mock.Mock()
        environment.config.airflow_uri = "https://composer.example"
        hook.get_environment.return_value = environment
        hook.get_dag_runs.side_effect = [
            build_dag_runs_result(composer_airflow_version, [("success", "2024-03-23T11:10:00+00:00")]),
            build_dag_runs_result(composer_airflow_version, [("success", "2024-03-22T11:10:00+00:00")]),
        ]
        with pytest.warns(AirflowProviderDeprecationWarning):
            trigger = CloudComposerDAGRunTrigger(
                project_id=TEST_PROJECT_ID,
                region=TEST_LOCATION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id=TEST_COMPOSER_DAG_ID,
                start_date=datetime(2024, 3, 22, 11, 0, 0, tzinfo=timezone.utc),
                end_date=datetime(2024, 3, 22, 12, 0, 0, tzinfo=timezone.utc),
                allowed_states=TEST_ALLOWED_STATES,
                gcp_conn_id=TEST_GCP_CONN_ID,
                impersonation_chain=TEST_IMPERSONATION_CHAIN,
                poll_interval=TEST_POLL_INTERVAL,
                composer_airflow_version=composer_airflow_version,
                use_rest_api=use_rest_api,
            )

        with mock.patch.object(trigger, "_get_async_hook", return_value=hook):
            actual_event = await trigger.run().asend(None)

        assert actual_event == TriggerEvent({"status": "success"})
        assert hook.get_dag_runs.await_count == 2
        assert mock_sleep.await_count == 1

    @pytest.mark.parametrize("use_rest_api", [True, False])
    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.cloud_composer.asyncio.sleep", new_callable=AsyncMock
    )
    async def test_trigger_yields_success_on_in_window_allowed(
        self, mock_sleep, composer_airflow_version, use_rest_api
    ):
        hook = AsyncMock()
        environment = mock.Mock()
        environment.config.airflow_uri = "https://composer.example"
        hook.get_environment.return_value = environment
        hook.get_dag_runs.return_value = build_dag_runs_result(
            composer_airflow_version,
            [("success", "2024-03-22T11:10:00+00:00")],
        )
        with pytest.warns(AirflowProviderDeprecationWarning):
            trigger = CloudComposerDAGRunTrigger(
                project_id=TEST_PROJECT_ID,
                region=TEST_LOCATION,
                environment_id=TEST_ENVIRONMENT_ID,
                composer_dag_id=TEST_COMPOSER_DAG_ID,
                start_date=datetime(2024, 3, 22, 11, 0, 0, tzinfo=timezone.utc),
                end_date=datetime(2024, 3, 22, 12, 0, 0, tzinfo=timezone.utc),
                allowed_states=TEST_ALLOWED_STATES,
                gcp_conn_id=TEST_GCP_CONN_ID,
                impersonation_chain=TEST_IMPERSONATION_CHAIN,
                poll_interval=TEST_POLL_INTERVAL,
                composer_airflow_version=composer_airflow_version,
                use_rest_api=use_rest_api,
            )

        with mock.patch.object(trigger, "_get_async_hook", return_value=hook):
            actual_event = await trigger.run().asend(None)

        assert actual_event == TriggerEvent({"status": "success"})
        assert hook.get_dag_runs.await_count == 1
        assert mock_sleep.await_count == 0

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.cloud_composer.asyncio.sleep", new_callable=AsyncMock
    )
    async def test_trigger_uses_composer_dag_run_id_branch_when_set(
        self, mock_sleep, composer_airflow_version
    ):
        """When ``composer_dag_run_id`` is set, the trigger's run-id branch
        wins and the windowed ``_check_dag_runs_states`` is not consulted.

        Returning a run whose ``logical_date`` is *outside*
        ``[start_date, end_date]`` but whose ``dag_run_id`` matches is
        enough to yield success.
        """
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        hook = AsyncMock()
        environment = mock.Mock()
        environment.config.airflow_uri = "https://composer.example"
        hook.get_environment.return_value = environment
        hook.get_dag_runs.return_value = {
            "dag_runs": [
                {
                    "dag_id": TEST_COMPOSER_DAG_ID,
                    "dag_run_id": "manual__abc",
                    "state": "success",
                    date_key: "2099-01-01T00:00:00+00:00",
                }
            ],
            "total_entries": 1,
        }
        trigger = CloudComposerDAGRunTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_LOCATION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            composer_dag_run_id="manual__abc",
            start_date=datetime(2024, 3, 22, 11, 0, 0, tzinfo=timezone.utc),
            end_date=datetime(2024, 3, 22, 12, 0, 0, tzinfo=timezone.utc),
            allowed_states=TEST_ALLOWED_STATES,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poll_interval=TEST_POLL_INTERVAL,
            composer_airflow_version=composer_airflow_version,
        )

        with mock.patch.object(trigger, "_get_async_hook", return_value=hook):
            actual_event = await trigger.run().asend(None)

        assert actual_event == TriggerEvent({"status": "success"})
        assert hook.get_dag_runs.await_count == 1
        assert mock_sleep.await_count == 0

    @pytest.mark.parametrize("composer_airflow_version", [2, 3])
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.cloud_composer.asyncio.sleep", new_callable=AsyncMock
    )
    async def test_trigger_polls_until_composer_dag_run_id_appears(
        self, mock_sleep, composer_airflow_version
    ):
        """First poll sees an unrelated run; second poll sees the matching
        one. The trigger must keep polling and only yield success on the
        second iteration (no premature short-circuit).
        """
        date_key = "execution_date" if composer_airflow_version < 3 else "logical_date"
        hook = AsyncMock()
        environment = mock.Mock()
        environment.config.airflow_uri = "https://composer.example"
        hook.get_environment.return_value = environment
        hook.get_dag_runs.side_effect = [
            {
                "dag_runs": [
                    {
                        "dag_id": TEST_COMPOSER_DAG_ID,
                        "dag_run_id": "scheduled__not-the-one",
                        "state": "success",
                        date_key: "2024-03-22T11:30:00+00:00",
                    }
                ],
                "total_entries": 1,
            },
            {
                "dag_runs": [
                    {
                        "dag_id": TEST_COMPOSER_DAG_ID,
                        "dag_run_id": "manual__abc",
                        "state": "success",
                        date_key: "2024-03-22T11:30:00+00:00",
                    }
                ],
                "total_entries": 1,
            },
        ]
        trigger = CloudComposerDAGRunTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_LOCATION,
            environment_id=TEST_ENVIRONMENT_ID,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            composer_dag_run_id="manual__abc",
            start_date=datetime(2024, 3, 22, 11, 0, 0, tzinfo=timezone.utc),
            end_date=datetime(2024, 3, 22, 12, 0, 0, tzinfo=timezone.utc),
            allowed_states=TEST_ALLOWED_STATES,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            poll_interval=TEST_POLL_INTERVAL,
            composer_airflow_version=composer_airflow_version,
        )

        with mock.patch.object(trigger, "_get_async_hook", return_value=hook):
            actual_event = await trigger.run().asend(None)

        assert actual_event == TriggerEvent({"status": "success"})
        assert hook.get_dag_runs.await_count == 2
        assert mock_sleep.await_count == 1


class TestCloudComposerExternalTaskTrigger:
    def test_serialize(self, external_task_trigger):
        actual_data = external_task_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerExternalTaskTrigger",
            {
                "project_id": TEST_PROJECT_ID,
                "region": TEST_LOCATION,
                "environment_id": TEST_ENVIRONMENT_ID,
                "start_date": TEST_START_DATE,
                "end_date": TEST_END_DATE,
                "allowed_states": TEST_ALLOWED_STATES,
                "skipped_states": TEST_SKIPPED_STATES,
                "failed_states": TEST_FAILED_STATES,
                "composer_external_dag_id": TEST_COMPOSER_DAG_ID,
                "composer_external_task_ids": TEST_COMPOSER_EXTERNAL_TASK_IDS,
                "composer_external_task_group_id": TEST_COMPOSER_EXTERNAL_TASK_GROUP_ID,
                "gcp_conn_id": TEST_GCP_CONN_ID,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
                "poll_interval": TEST_POLL_INTERVAL,
                "composer_airflow_version": TEST_COMPOSER_AIRFLOW_VERSION,
            },
        )
        assert actual_data == expected_data
