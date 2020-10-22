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
Tests for Google Cloud Life Sciences Hook
"""
import unittest
from unittest import mock

from unittest.mock import PropertyMock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.life_sciences import LifeSciencesHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_OPERATION = {
    "name": 'operation-name',
    "metadata": {"@type": 'anytype'},
    "done": True,
    "response": "response",
}

TEST_WAITING_OPERATION = {"done": False, "response": "response"}
TEST_DONE_OPERATION = {"done": True, "response": "response"}
TEST_ERROR_OPERATION = {"done": True, "response": "response", "error": "error"}
TEST_PROJECT_ID = "life-science-project-id"
TEST_LOCATION = 'test-location'


class TestLifeSciencesHookWithPassedProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = LifeSciencesHook(gcp_conn_id="test")

    def test_location_path(self):
        path = 'projects/life-science-project-id/locations/test-location'
        path2 = self.hook._location_path(project_id=TEST_PROJECT_ID, location=TEST_LOCATION)
        self.assertEqual(path, path2)

    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.build")
    def test_life_science_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'lifesciences', 'v2beta', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    def test_run_pipeline_immediately_complete(self, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value
        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute.return_value = TEST_DONE_OPERATION

        result = self.hook.run_pipeline(body={},  # pylint: disable=no-value-for-parameter
                                        location=TEST_LOCATION)
        parent = self.hook. \
            _location_path(location=TEST_LOCATION)  # pylint: disable=no-value-for-parameter
        service_mock.projects.return_value.locations.return_value \
            .pipelines.return_value.run \
            .assert_called_once_with(body={},
                                     parent=parent)
        # fmt: on
        self.assertEqual(result, TEST_OPERATION)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value
        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        execute_mock = mock.Mock(
            **{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION]}
        )
        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute = execute_mock

        # fmt: on
        result = self.hook.run_pipeline(body={}, location=TEST_LOCATION, project_id=TEST_PROJECT_ID)
        self.assertEqual(result, TEST_OPERATION)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.time.sleep")
    def test_error_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value
        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute = execute_mock
        # fmt: on
        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.run_pipeline(body={}, location=TEST_LOCATION, project_id=TEST_PROJECT_ID)


class TestLifeSciencesHookWithDefaultProjectIdFromConnection(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = LifeSciencesHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.build")
    def test_life_science_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'lifesciences', 'v2beta', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    def test_run_pipeline_immediately_complete(self, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute.return_value = TEST_DONE_OPERATION

        result = self.hook.run_pipeline(body={}, location=TEST_LOCATION, project_id=TEST_PROJECT_ID)
        parent = self.hook._location_path(project_id=TEST_PROJECT_ID, location=TEST_LOCATION)
        service_mock.projects.return_value.locations.return_value \
            .pipelines.return_value.run \
            .assert_called_once_with(body={},
                                     parent=parent)
        # fmt: on
        self.assertEqual(result, TEST_OPERATION)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION]})
        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute = execute_mock
        # fmt: on

        # pylint: disable=no-value-for-parameter
        result = self.hook.run_pipeline(body={}, location=TEST_LOCATION)
        self.assertEqual(result, TEST_OPERATION)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.time.sleep")
    def test_error_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        # fmt: off
        service_mock.projects.return_value \
            .locations.return_value \
            .pipelines.return_value \
            .run.return_value \
            .execute.return_value = TEST_OPERATION

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        service_mock.projects.return_value \
            .locations.return_value \
            .operations.return_value \
            .get.return_value \
            .execute = execute_mock
        # fmt: on

        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.run_pipeline(body={}, location=TEST_LOCATION)  # pylint: disable=no-value-for-parameter


class TestLifeSciencesHookWithoutProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = LifeSciencesHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.build")
    def test_life_science_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'lifesciences', 'v2beta', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.life_sciences.LifeSciencesHook.get_conn")
    def test_run_pipeline(self, get_conn_mock, mock_project_id):  # pylint: disable=unused-argument
        with self.assertRaises(AirflowException) as e:
            self.hook.run_pipeline(body={}, location=TEST_LOCATION)  # pylint: disable=no-value-for-parameter

        self.assertEqual(
            "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "Google Cloud connection definition. Both are not set!",
            str(e.exception),
        )
