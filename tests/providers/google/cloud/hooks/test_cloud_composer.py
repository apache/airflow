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

import unittest
from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerAsyncHook, CloudComposerHook

TEST_GCP_REGION = "global"
TEST_GCP_PROJECT = "test-project"
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_DELEGATE_TO = "test-delegate-to"
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

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
COMPOSER_STRING = "airflow.providers.google.cloud.hooks.cloud_composer.{}"


def mock_init(*args, **kwargs):
    pass


class TestCloudComposerHook(unittest.TestCase):
    def setUp(
        self,
    ) -> None:
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
                'parent': self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                'environment': TEST_ENVIRONMENT,
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
                'name': self.hook.get_environment_name(TEST_GCP_PROJECT, TEST_GCP_REGION, TEST_ENVIRONMENT_ID)
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
                'parent': self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                "page_size": None,
                "page_token": None,
                "include_past_releases": False,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestCloudComposerAsyncHook(unittest.TestCase):
    def setUp(
        self,
    ) -> None:
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = CloudComposerAsyncHook(gcp_conn_id="test")

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_create_environment(self, mock_client) -> None:
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
                'parent': self.hook.get_parent(TEST_GCP_PROJECT, TEST_GCP_REGION),
                'environment': TEST_ENVIRONMENT,
            },
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @pytest.mark.asyncio
    @mock.patch(COMPOSER_STRING.format("CloudComposerAsyncHook.get_environment_client"))
    async def test_delete_environment(self, mock_client) -> None:
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
