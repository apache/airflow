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

from unittest import TestCase, mock

from airflow.providers.google.cloud.operators.dataplex import DataplexHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAPLEX_STRING = "airflow.providers.google.cloud.hooks.dataplex.{}"

PROJECT_ID = "project-id"
REGION = "region"
LAKE_ID = "lake-id"
BODY = {"body": "test"}
DATAPLEX_TASK_ID = "testTask001"

GCP_CONN_ID = "google_cloud_default"


def get_tasks_return_value(conn):
    return (
        conn.return_value.projects.return_value.locations.return_value.lakes.return_value.tasks.return_value
    )


class TestDataplexHook(TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = DataplexHook(gcp_conn_id=GCP_CONN_ID)

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_conn"))
    def test_create_task(self, get_conn_mock):
        self.hook.create_task(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            dataplex_task_id=DATAPLEX_TASK_ID,
            validate_only=None,
        )

        parent = f'projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}'
        get_tasks_return_value(get_conn_mock).create.assert_called_once_with(
            parent=parent, body=BODY, taskId=DATAPLEX_TASK_ID, validateOnly=None
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_conn"))
    def test_delete_task(self, get_conn_mock):
        self.hook.delete_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        name = f'projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}'
        get_tasks_return_value(get_conn_mock).delete.assert_called_once_with(name=name)

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_conn"))
    def test_list_tasks(self, get_conn_mock):
        parent = f'projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}'

        self.hook.list_tasks(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        get_tasks_return_value(get_conn_mock).list.assert_called_once_with(
            parent=parent, pageSize=None, pageToken=None, filter=None, orderBy=None
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_conn"))
    def test_get_tasks(self, get_conn_mock):
        name = f'projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}'

        self.hook.get_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        get_tasks_return_value(get_conn_mock).get.assert_called_once_with(name=name)
