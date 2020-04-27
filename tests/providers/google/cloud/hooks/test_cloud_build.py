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
This module contains various unit tests for
functions in CloudBuildHook
"""

import unittest
import unittest.mock as mock
from unittest.mock import PropertyMock, patch

from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

PROJECT_ID = "cloud-build-project"
BUILD_ID = "test-build-id-9832661"
REPO_SOURCE = {"repo_source": {"repo_name": "test_repo", "branch_name": "master"}}
BUILD = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "SUCCESS",
}
BUILD_WORKING = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "WORKING",
}
BUILD_TRIGGER = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {"project_id": PROJECT_ID, "repo_name": "test_repo", "branch_name": "master"},
    "filename": "cloudbuild.yaml",
}
OPERATION = {"metadata": {"build": {"id": BUILD_ID}}}
TRIGGER_ID = "32488e7f-09d6-4fe9-a5fb-4ca1419a6e7a"


class TestCloudBuildHook(unittest.TestCase):
    def setUp(self):
        with patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudBuildHook(gcp_conn_id="test")

    @patch(
        "airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.client_info",
        new_callable=PropertyMock,
    )
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook._get_credentials")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildClient")
    def test_cloud_build_service_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(
            credentials=mock_get_creds.return_value, client_info=mock_client_info.return_value
        )
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.hook._client, result)

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_cancel_build(self, get_conn):
        self.hook.cancel_build(id_=BUILD_ID, project_id=PROJECT_ID)

        get_conn.return_value.cancel_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.TIME_TO_SLEEP_IN_SECONDS")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_create_build_with_wait(self, get_conn, mock_message_to_dict, wait_time):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]
        wait_time.return_value = 0

        self.hook.create_build(build=BUILD, project_id=PROJECT_ID)

        get_conn.return_value.create_build.assert_called_once_with(
            build=BUILD, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

        get_conn.return_value.get_build.assert_has_calls(
            [
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
            ]
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_create_build_without_wait(self, get_conn, mock_message_to_dict):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]

        self.hook.create_build(build=BUILD, project_id=PROJECT_ID, wait=False)

        get_conn.return_value.create_build.assert_called_once_with(
            build=BUILD, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

        get_conn.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_create_build_trigger(self, get_conn):
        self.hook.create_build_trigger(trigger=BUILD_TRIGGER, project_id=PROJECT_ID)

        get_conn.return_value.create_build_trigger.assert_called_once_with(
            trigger=BUILD_TRIGGER, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_delete_build_trigger(self, get_conn):
        self.hook.delete_build_trigger(trigger_id=TRIGGER_ID, project_id=PROJECT_ID)

        get_conn.return_value.delete_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_get_build(self, get_conn):
        self.hook.get_build(id_=BUILD_ID, project_id=PROJECT_ID)

        get_conn.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_get_build_trigger(self, get_conn):
        self.hook.get_build_trigger(trigger_id=TRIGGER_ID, project_id=PROJECT_ID)

        get_conn.return_value.get_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_list_build_triggers(self, get_conn):
        self.hook.list_build_triggers(project_id=PROJECT_ID)

        get_conn.return_value.list_build_triggers.assert_called_once_with(
            project_id=PROJECT_ID, page_size=None, page_token=None, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_list_builds(self, get_conn):
        self.hook.list_builds(project_id=PROJECT_ID)

        get_conn.return_value.list_builds.assert_called_once_with(
            project_id=PROJECT_ID, page_size=None, filter_=None, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.TIME_TO_SLEEP_IN_SECONDS")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_retry_build_with_wait(self, get_conn, mock_message_to_dict, wait_time):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]
        wait_time.return_value = 0

        self.hook.retry_build(id_=BUILD_ID, project_id=PROJECT_ID)

        get_conn.return_value.retry_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

        get_conn.return_value.get_build.assert_has_calls(
            [
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
            ]
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_retry_build_without_wait(self, get_conn, mock_message_to_dict):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]

        self.hook.retry_build(id_=BUILD_ID, project_id=PROJECT_ID, wait=False)

        get_conn.return_value.retry_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

        get_conn.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.TIME_TO_SLEEP_IN_SECONDS")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_run_build_trigger_with_wait(self, get_conn, mock_message_to_dict, wait_time):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]
        wait_time.return_value = 0

        self.hook.run_build_trigger(trigger_id=TRIGGER_ID, source=REPO_SOURCE, project_id=PROJECT_ID)

        get_conn.return_value.run_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            source=REPO_SOURCE,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )

        get_conn.return_value.get_build.assert_has_calls(
            [
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
                mock.call(id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None),
            ]
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_run_build_trigger_without_wait(self, get_conn, mock_message_to_dict):
        mock_message_to_dict.side_effect = [OPERATION, BUILD_WORKING, BUILD]

        self.hook.run_build_trigger(
            trigger_id=TRIGGER_ID, source=REPO_SOURCE, project_id=PROJECT_ID, wait=False
        )

        get_conn.return_value.run_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            source=REPO_SOURCE,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )

        get_conn.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test_update_build_trigger(self, get_conn):
        self.hook.update_build_trigger(trigger_id=TRIGGER_ID, trigger=BUILD_TRIGGER, project_id=PROJECT_ID)

        get_conn.return_value.update_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            trigger=BUILD_TRIGGER,
            project_id=PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @patch("airflow.providers.google.cloud.hooks.cloud_build.MessageToDict")
    @patch("airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook.get_conn")
    def test__wait_for_operation_to_complete(self, get_conn, mock_message_to_dict):
        mock_message_to_dict.return_value = BUILD

        self.hook._wait_for_operation_to_complete(
            func=self.hook.get_build, id_=BUILD_ID, project_id=PROJECT_ID
        )

        get_conn.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=PROJECT_ID, retry=None, timeout=None, metadata=None
        )
