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

from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import TaskDeferred
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerCreateEnvironmentOperator,
    CloudComposerDeleteEnvironmentOperator,
    CloudComposerGetEnvironmentOperator,
    CloudComposerListEnvironmentsOperator,
    CloudComposerListImageVersionsOperator,
    CloudComposerUpdateEnvironmentOperator,
)
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerExecutionTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

TASK_ID = "task-id"
TEST_GCP_REGION = "global"
TEST_GCP_PROJECT = "test-project"
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_IMPERSONATION_CHAIN = None
TEST_ENVIRONMENT_ID = "testenvname"
TEST_ENVIRONMENT = {
    "name": TEST_ENVIRONMENT_ID,
    "config": {
        "node_count": 3,
        "software_config": {"image_version": "composer-1.17.7-airflow-2.1.4"},
    },
}

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

COMPOSER_STRING = "airflow.providers.google.cloud.operators.cloud_composer.{}"
COMPOSER_TRIGGERS_STRING = "airflow.providers.google.cloud.triggers.cloud_composer.{}"


class TestCloudComposerCreateEnvironmentOperator:
    @mock.patch(COMPOSER_STRING.format("Environment.to_dict"))
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_execute(self, mock_hook, to_dict_mode) -> None:
        op = CloudComposerCreateEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_ENVIRONMENT,
            gcp_conn_id=TEST_GCP_CONN_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_environment.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment=TEST_ENVIRONMENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("Environment.to_dict"))
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    @mock.patch(COMPOSER_TRIGGERS_STRING.format("CloudComposerAsyncHook"))
    def test_execute_deferrable(self, mock_trigger_hook, mock_hook, to_dict_mode):
        op = CloudComposerCreateEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_ENVIRONMENT,
            gcp_conn_id=TEST_GCP_CONN_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        assert isinstance(exc.value.trigger, CloudComposerExecutionTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestCloudComposerDeleteEnvironmentOperator:
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_execute(self, mock_hook) -> None:
        op = CloudComposerDeleteEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_environment.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    @mock.patch(COMPOSER_TRIGGERS_STRING.format("CloudComposerAsyncHook"))
    def test_execute_deferrable(self, mock_trigger_hook, mock_hook):
        op = CloudComposerDeleteEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        assert isinstance(exc.value.trigger, CloudComposerExecutionTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestCloudComposerUpdateEnvironmentOperator:
    @mock.patch(COMPOSER_STRING.format("Environment.to_dict"))
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_execute(self, mock_hook, to_dict_mode) -> None:
        op = CloudComposerUpdateEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_UPDATED_ENVIRONMENT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_environment.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_UPDATED_ENVIRONMENT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(COMPOSER_STRING.format("Environment.to_dict"))
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    @mock.patch(COMPOSER_TRIGGERS_STRING.format("CloudComposerAsyncHook"))
    def test_execute_deferrable(self, mock_trigger_hook, mock_hook, to_dict_mode):
        op = CloudComposerUpdateEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            environment=TEST_UPDATED_ENVIRONMENT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        assert isinstance(exc.value.trigger, CloudComposerExecutionTrigger)
        assert exc.value.method_name == GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME


class TestCloudComposerGetEnvironmentOperator:
    @mock.patch(COMPOSER_STRING.format("Environment.to_dict"))
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_execute(self, mock_hook, to_dict_mode) -> None:
        op = CloudComposerGetEnvironmentOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_environment.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            environment_id=TEST_ENVIRONMENT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudComposerListEnvironmentsOperator:
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudComposerListEnvironmentsOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_environments.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            page_size=None,
            page_token=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudComposerListImageVersionsOperator:
    @mock.patch(COMPOSER_STRING.format("CloudComposerHook"))
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = CloudComposerListImageVersionsOperator(
            task_id=TASK_ID,
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_image_versions.assert_called_once_with(
            project_id=TEST_GCP_PROJECT,
            region=TEST_GCP_REGION,
            include_past_releases=False,
            page_size=None,
            page_token=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
