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
"""
Tests for Google Cloud Build Hook
"""
import unittest
from typing import Optional
from unittest import mock

from mock import PropertyMock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST, mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_CREATE_BODY = {
    "source": {"storageSource": {"bucket": "cloud-build-examples", "object": "node-docker-example.tar.gz"}},
    "steps": [
        {"name": "gcr.io/cloud-builders/docker", "args": ["build", "-t", "gcr.io/$PROJECT_ID/my-image", "."]}
    ],
    "images": ["gcr.io/$PROJECT_ID/my-image"],
}

TEST_BUILD = {"name": "build-name", "metadata": {"build": {"id": "AAA"}}}
TEST_WAITING_OPERATION = {"done": False, "response": "response"}
TEST_DONE_OPERATION = {"done": True, "response": "response"}
TEST_ERROR_OPERATION = {"done": True, "response": "response", "error": "error"}
TEST_PROJECT_ID = "cloud-build-project-id"


class TestCloudBuildHookWithPassedProjectId(unittest.TestCase):
    hook = None  # type: Optional[CloudBuildHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.build")
    def test_cloud_build_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'cloudbuild', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_build_immediately_complete(self, get_conn_mock):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value\
            .builds.return_value\
            .create.return_value\
            .execute.return_value = TEST_BUILD

        service_mock.projects.return_value.\
            builds.return_value.\
            get.return_value.\
            execute.return_value = TEST_BUILD

        service_mock.operations.return_value.\
            get.return_value.\
            execute.return_value = TEST_DONE_OPERATION

        result = self.hook.create_build(body={}, project_id=TEST_PROJECT_ID)

        service_mock.projects.return_value.builds.return_value.create.assert_called_once_with(
            body={}, projectId=TEST_PROJECT_ID
        )

        self.assertEqual(result, TEST_BUILD)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value.builds.return_value.create.return_value.execute.return_value = (
            TEST_BUILD
        )

        service_mock.projects.return_value.builds.return_value.get.return_value.execute.return_value = (
            TEST_BUILD
        )

        execute_mock = mock.Mock(
            **{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION, TEST_DONE_OPERATION]}
        )
        service_mock.operations.return_value.get.return_value.execute = execute_mock

        result = self.hook.create_build(body={}, project_id=TEST_PROJECT_ID)

        self.assertEqual(result, TEST_BUILD)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.time.sleep")
    def test_error_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value.builds.return_value.create.return_value.execute.return_value = (
            TEST_BUILD
        )

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        service_mock.operations.return_value.get.return_value.execute = execute_mock
        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.create_build(body={})


class TestGcpComputeHookWithDefaultProjectIdFromConnection(unittest.TestCase):
    hook = None  # type: Optional[CloudBuildHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.build")
    def test_cloud_build_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'cloudbuild', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_build_immediately_complete(self, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value.builds.return_value.create.return_value.execute.return_value = (
            TEST_BUILD
        )

        service_mock.projects.return_value.builds.return_value.get.return_value.execute.return_value = (
            TEST_BUILD
        )

        service_mock.operations.return_value.get.return_value.execute.return_value = TEST_DONE_OPERATION

        result = self.hook.create_build(body={})

        service_mock.projects.return_value.builds.return_value.create.assert_called_once_with(
            body={}, projectId='example-project'
        )

        self.assertEqual(result, TEST_BUILD)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value.builds.return_value.create.return_value.execute.return_value = (
            TEST_BUILD
        )

        service_mock.projects.return_value.builds.return_value.get.return_value.execute.return_value = (
            TEST_BUILD
        )

        execute_mock = mock.Mock(
            **{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION, TEST_DONE_OPERATION]}
        )
        service_mock.operations.return_value.get.return_value.execute = execute_mock

        result = self.hook.create_build(body={})

        self.assertEqual(result, TEST_BUILD)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.time.sleep")
    def test_error_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        service_mock.projects.return_value.builds.return_value.create.return_value.execute.return_value = (
            TEST_BUILD
        )

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        service_mock.operations.return_value.get.return_value.execute = execute_mock
        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.create_build(body={})


class TestCloudBuildHookWithoutProjectId(unittest.TestCase):
    hook = None  # type: Optional[CloudBuildHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.build")
    def test_cloud_build_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'cloudbuild', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_create_build(self, mock_get_conn, mock_project_id):
        with self.assertRaises(AirflowException) as e:
            self.hook.create_build(body={})

        self.assertEqual(
            "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "GCP connection definition. Both are not set!",
            str(e.exception),
        )
