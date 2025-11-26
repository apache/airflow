#
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
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.orchestration.airflow.service_v1 import EnvironmentsAsyncClient

from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerAsyncHook, CloudComposerHook

TEST_GCP_REGION = "global"
TEST_GCP_PROJECT = "test-project"
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_ENVIRONMENT_ID = "testenvname"
TEST_ENVIRONMENT = {
    "name": TEST_ENVIRONMENT_ID,
    "config": {
        "node_count": 3,
        "software_config": {"image_version": "composer-1.17.7-airflow-2.1.4"},
    },
}
TEST_COMMAND = "dags"
TEST_SUBCOMMAND = "list"
TEST_PARAMETERS = ["--verbose", "-o", "json"]
TEST_EXECUTION_ID = "test-execution-id"
TEST_POD = "test-pod"
TEST_POD_NAMESPACE = "test-namespace"

TEST_UPDATE_MASK = {"paths": ["labels.label1"]}
TEST_UPDATED_ENVIRONMENT = {
    "labels": {
        "label1": "testing",
    }
}
TEST_RETRY = DEFAULT
TEST_TIMEOUT = None
TEST_METADATA = [("key", "value")]
TEST_PARENT = "test-parent"
TEST_NAME = "test-name"

TEST_COMPOSER_AIRFLOW_URI = "test-composer-airflow-uri"
TEST_COMPOSER_DAG_ID = "test-composer-dag-id"
TEST_COMPOSER_DAG_CONF = {"test-key": "test-value"}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
COMPOSER_STRING = "airflow.providers.google.cloud.hooks.cloud_composer.{}"


def mock_init(*args, **kwargs):
    pass


class TestCloudComposerHook:
    def setup_method(self):
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = CloudComposerHook(gcp_conn_id="test")

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_create_environment(self, mock_client) -> None:
        self.hook.create_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment=TEST_ENVIRONMENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_environment.assert_called_once_with(
            request={
                "parent": self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                "environment": TEST_ENVIRONMENT,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_delete_environment(self, mock_client) -> None:
        self.hook.delete_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID)
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_get_environment(self, mock_client) -> None:
        self.hook.get_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID)
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_list_environments(self, mock_client) -> None:
        self.hook.list_environments(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_environments.assert_called_once_with(
            request={
                "parent": self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                "page_size": None,
                "page_token": None,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_update_environment(self, mock_client) -> None:
        self.hook.update_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_UPDATED_ENVIRONMENT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "environment": TEST_UPDATED_ENVIRONMENT,
                "update_mask": TEST_UPDATE_MASK,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_image_versions_client"))
    def test_list_image_versions(self, mock_client) -> None:
        self.hook.list_image_versions(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_image_versions.assert_called_once_with(
            request={
                "parent": self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                "page_size": None,
                "page_token": None,
                "include_past_releases": False,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_execute_airflow_command(self, mock_client) -> None:
        self.hook.execute_airflow_command(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            command=TEST_COMMAND,
            subcommand=TEST_SUBCOMMAND,
            parameters=TEST_PARAMETERS,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.execute_airflow_command.assert_called_once_with(
            request={
                "environment": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "command": TEST_COMMAND,
                "subcommand": TEST_SUBCOMMAND,
                "parameters": TEST_PARAMETERS,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.get_environment_client"))
    def test_poll_airflow_command(self, mock_client) -> None:
        self.hook.poll_airflow_command(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            execution_id=TEST_EXECUTION_ID,
            pod=TEST_POD,
            pod_namespace=TEST_POD_NAMESPACE,
            next_line_number=1,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.poll_airflow_command.assert_called_once_with(
            request={
                "environment": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "execution_id": TEST_EXECUTION_ID,
                "pod": TEST_POD,
                "pod_namespace": TEST_POD_NAMESPACE,
                "next_line_number": 1,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.make_composer_airflow_api_request"))
    def test_trigger_dag_run(self, mock_composer_airflow_api_request) -> None:
        self.hook.get_credentials = mock.MagicMock()
        self.hook.trigger_dag_run(
            composer_airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            composer_dag_conf=TEST_COMPOSER_DAG_CONF,
            timeout=TEST_TIMEOUT,
        )
        mock_composer_airflow_api_request.assert_called_once_with(
            method="POST",
            airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            path=f"/api/v1/dags/{TEST_COMPOSER_DAG_ID}/dagRuns",
            data=json.dumps(
                {
                    "conf": TEST_COMPOSER_DAG_CONF,
                }
            ),
            timeout=TEST_TIMEOUT,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.make_composer_airflow_api_request"))
    def test_get_dag_runs(self, mock_composer_airflow_api_request) -> None:
        self.hook.get_credentials = mock.MagicMock()
        self.hook.get_dag_runs(
            composer_airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            timeout=TEST_TIMEOUT,
        )
        mock_composer_airflow_api_request.assert_called_once_with(
            method="GET",
            airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            path=f"/api/v1/dags/{TEST_COMPOSER_DAG_ID}/dagRuns",
            timeout=TEST_TIMEOUT,
        )

    @pytest.mark.parametrize("query_parameters", [None, {"test_key": "test_value"}])
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook.make_composer_airflow_api_request"))
    def test_get_task_instances(self, mock_composer_airflow_api_request, query_parameters) -> None:
        query_string = "?test_key=test_value" if query_parameters else ""
        self.hook.get_credentials = mock.MagicMock()
        self.hook.get_task_instances(
            composer_airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            query_parameters=query_parameters,
            timeout=TEST_TIMEOUT,
        )
        mock_composer_airflow_api_request.assert_called_once_with(
            method="GET",
            airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            path=f"/api/v1/dags/{TEST_COMPOSER_DAG_ID}/dagRuns/~/taskInstances{query_string}",
            timeout=TEST_TIMEOUT,
        )


class TestCloudComposerAsyncHook:
    def setup_method(self, method):
        with mock.patch(BASE_STRING.format("GoogleBaseAsyncHook.__init__"), new=mock_init):
            self.hook = CloudComposerAsyncHook(gcp_conn_id="test")

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_create_environment(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.create_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment=TEST_ENVIRONMENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_environment.assert_called_once_with(
            request={
                "parent": self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                "environment": TEST_ENVIRONMENT,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_delete_environment(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.delete_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID)
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_update_environment(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.update_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_UPDATED_ENVIRONMENT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "environment": TEST_UPDATED_ENVIRONMENT,
                "update_mask": TEST_UPDATE_MASK,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_get_environment(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.get_environment(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_environment.assert_called_once_with(
            request={
                "name": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_execute_airflow_command(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.execute_airflow_command(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            command=TEST_COMMAND,
            subcommand=TEST_SUBCOMMAND,
            parameters=TEST_PARAMETERS,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.execute_airflow_command.assert_called_once_with(
            request={
                "environment": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "command": TEST_COMMAND,
                "subcommand": TEST_SUBCOMMAND,
                "parameters": TEST_PARAMETERS,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_poll_airflow_command(self, mock_client) -> None:
        mock_env_client = AsyncMock(EnvironmentsAsyncClient)
        mock_client.return_value = mock_env_client
        await self.hook.poll_airflow_command(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            execution_id=TEST_EXECUTION_ID,
            pod=TEST_POD,
            pod_namespace=TEST_POD_NAMESPACE,
            next_line_number=1,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_client.assert_called_once()
        mock_client.return_value.poll_airflow_command.assert_called_once_with(
            request={
                "environment": self.hook.get_environment_name(
                    TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID
                ),
                "execution_id": TEST_EXECUTION_ID,
                "pod": TEST_POD,
                "pod_namespace": TEST_POD_NAMESPACE,
                "next_line_number": 1,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.make_composer_airflow_api_request"))
    async def test_get_dag_runs(self, mock_composer_airflow_api_request) -> None:
        mock_composer_airflow_api_request.return_value = ({}, 200)
        await self.hook.get_dag_runs(
            composer_airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            timeout=TEST_TIMEOUT,
        )
        mock_composer_airflow_api_request.assert_called_once_with(
            method="GET",
            airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            path=f"/api/v1/dags/{TEST_COMPOSER_DAG_ID}/dagRuns",
            timeout=TEST_TIMEOUT,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("query_parameters", [None, {"test_key": "test_value"}])
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.make_composer_airflow_api_request"))
    async def test_get_task_instances(self, mock_composer_airflow_api_request, query_parameters) -> None:
        query_string = "?test_key=test_value" if query_parameters else ""
        mock_composer_airflow_api_request.return_value = ({}, 200)
        await self.hook.get_task_instances(
            composer_airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            composer_dag_id=TEST_COMPOSER_DAG_ID,
            query_parameters=query_parameters,
            timeout=TEST_TIMEOUT,
        )
        mock_composer_airflow_api_request.assert_called_once_with(
            method="GET",
            airflow_uri=TEST_COMPOSER_AIRFLOW_URI,
            path=f"/api/v1/dags/{TEST_COMPOSER_DAG_ID}/dagRuns/~/taskInstances{query_string}",
            timeout=TEST_TIMEOUT,
        )
