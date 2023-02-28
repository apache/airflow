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

from google.api_core.gapic_v1.method import DEFAULT

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
DELEGATE_TO = "test-delegate-to"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestDataplexHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = DataplexHook(
                gcp_conn_id=GCP_CONN_ID,
                delegate_to=DELEGATE_TO,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_create_task(self, mock_client):
        self.hook.create_task(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            dataplex_task_id=DATAPLEX_TASK_ID,
            validate_only=None,
        )

        parent = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.create_task.assert_called_once_with(
            request=dict(
                parent=parent,
                task_id=DATAPLEX_TASK_ID,
                task=BODY,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_delete_task(self, mock_client):
        self.hook.delete_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}"
        mock_client.return_value.delete_task.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_list_tasks(self, mock_client):
        self.hook.list_tasks(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        parent = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.list_tasks.assert_called_once_with(
            request=dict(
                parent=parent,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_get_task(self, mock_client):
        self.hook.get_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}"
        mock_client.return_value.get_task.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_create_lake(self, mock_client):
        self.hook.create_lake(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            validate_only=None,
        )

        parent = f"projects/{PROJECT_ID}/locations/{REGION}"
        mock_client.return_value.create_lake.assert_called_once_with(
            request=dict(
                parent=parent,
                lake_id=LAKE_ID,
                lake=BODY,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_delete_lake(self, mock_client):
        self.hook.delete_lake(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.delete_lake.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_STRING.format("DataplexHook.get_dataplex_client"))
    def test_get_lake(self, mock_client):
        self.hook.get_lake(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/"
        mock_client.return_value.get_lake.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
