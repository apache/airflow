# -*- coding: utf-8 -*-
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
from typing import Optional
import unittest
from unittest import mock

from airflow import AirflowException
from airflow.gcp.hooks.cloud_build import CloudBuildHook
from tests.compat import PropertyMock
from tests.contrib.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
    GCP_PROJECT_ID_HOOK_UNIT_TEST)


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
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
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

    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.gcp.hooks.cloud_build.time.sleep")
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
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.gcp.hooks.cloud_build.time.sleep")
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
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
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
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.gcp.hooks.cloud_build.time.sleep")
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
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
    @mock.patch("airflow.gcp.hooks.cloud_build.time.sleep")
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
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch("airflow.gcp.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_create_build(self, mock_get_conn, mock_project_id):
        with self.assertRaises(AirflowException) as e:
            self.hook.create_build(body={})

        self.assertEqual(
            "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "GCP connection definition. Both are not set!",
            str(e.exception),
        )
