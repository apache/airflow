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

# pylint: disable=R0904, C0111
"""
This module contains various unit tests for GCP DLP Operators
"""

import unittest
from unittest import mock

from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator, CloudBuildCreateBuildOperator, CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator, CloudBuildGetBuildOperator, CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildsOperator, CloudBuildListBuildTriggersOperator, CloudBuildRetryBuildOperator,
    CloudBuildRunBuildTriggerOperator, CloudBuildUpdateBuildTriggerOperator,
)

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "cloud-build-project"
BUILD_ID = "test-build-id-9832661"
REPO_SOURCE = {"repo_source": {"repo_name": "test_repo", "branch_name": "master"}}
BUILD = {
    "source": REPO_SOURCE,
    "steps": [{"name": "gcr.io/cloud-builders/gcloud", "entrypoint": "/bin/sh", "args": ["-c", "ls"]}],
    "status": "SUCCESS",
}
BUILD_TRIGGER = {
    "name": "test-cloud-build-trigger",
    "trigger_template": {"project_id": PROJECT_ID, "repo_name": "test_repo", "branch_name": "master"},
    "filename": "cloudbuild.yaml",
}
OPERATION = {"metadata": {"build": {"id": BUILD_ID}}}
TRIGGER_ID = "32488e7f-09d6-4fe9-a5fb-4ca1419a6e7a"


class TestCloudBuildOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_cancel_build(self, mock_hook):
        mock_hook.return_value.cancel_build.return_value = mock.MagicMock()
        operator = CloudBuildCancelBuildOperator(id_=TRIGGER_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.cancel_build.assert_called_once_with(
            id_=TRIGGER_ID, project_id=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_create_build(self, mock_hook):
        mock_hook.return_value.create_build.return_value = mock.MagicMock()
        operator = CloudBuildCreateBuildOperator(build=BUILD, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_build.assert_called_once_with(
            build=BUILD, project_id=None, wait=True, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_create_build_trigger(self, mock_hook):
        mock_hook.return_value.create_build_trigger.return_value = mock.MagicMock()
        operator = CloudBuildCreateBuildTriggerOperator(trigger=BUILD_TRIGGER, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_build_trigger.assert_called_once_with(
            trigger=BUILD_TRIGGER, project_id=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_delete_build_trigger(self, mock_hook):
        mock_hook.return_value.delete_build_trigger.return_value = mock.MagicMock()
        operator = CloudBuildDeleteBuildTriggerOperator(trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID, project_id=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_get_build(self, mock_hook):
        mock_hook.return_value.get_build.return_value = mock.MagicMock()
        operator = CloudBuildGetBuildOperator(id_=BUILD_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.get_build.assert_called_once_with(
            id_=BUILD_ID, project_id=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_get_build_trigger(self, mock_hook):
        mock_hook.return_value.get_build_trigger.return_value = mock.MagicMock()
        operator = CloudBuildGetBuildTriggerOperator(trigger_id=TRIGGER_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.get_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID, project_id=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_list_build_triggers(self, mock_hook):
        mock_hook.return_value.list_build_triggers.return_value = mock.MagicMock()
        operator = CloudBuildListBuildTriggersOperator(task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.list_build_triggers.assert_called_once_with(
            project_id=None, page_size=None, page_token=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_list_builds(self, mock_hook):
        mock_hook.return_value.list_builds.return_value = mock.MagicMock()
        operator = CloudBuildListBuildsOperator(task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.list_builds.assert_called_once_with(
            project_id=None, page_size=None, filter_=None, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_retry_build(self, mock_hook):
        mock_hook.return_value.retry_build.return_value = mock.MagicMock()
        operator = CloudBuildRetryBuildOperator(id_=BUILD_ID, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.retry_build.assert_called_once_with(
            id_=BUILD_ID, project_id=None, wait=True, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_run_build_trigger(self, mock_hook):
        mock_hook.return_value.run_build_trigger.return_value = mock.MagicMock()
        operator = CloudBuildRunBuildTriggerOperator(trigger_id=TRIGGER_ID, source=REPO_SOURCE, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.run_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            source=REPO_SOURCE,
            project_id=None,
            wait=True,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_update_build_trigger(self, mock_hook):
        mock_hook.return_value.update_build_trigger.return_value = mock.MagicMock()
        operator = CloudBuildUpdateBuildTriggerOperator(
            trigger_id=TRIGGER_ID, trigger=BUILD_TRIGGER, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_build_trigger.assert_called_once_with(
            trigger_id=TRIGGER_ID,
            trigger=BUILD_TRIGGER,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
