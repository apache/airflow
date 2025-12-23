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

from unittest import mock

from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
)
from airflow.utils import timezone

API_VERSION = "v4"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
REPORT_ID = "report_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
QUERY_ID = FILENAME = "test.csv"
OBJECT_NAME = "object_name"
OPERATION_NAME = "test_operation"
RESOURCE_NAME = "resource/path/to/media"


class TestGoogleDisplayVideo360SDFtoGCSOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.zipfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.os")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.tempfile.TemporaryDirectory"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.open",
        new_callable=mock.mock_open,
    )
    def test_execute(self, mock_open, mock_hook, gcs_hook_mock, temp_dir_mock, os_mock, zipfile_mock):
        operation = {"response": {"resourceName": RESOURCE_NAME}}
        media = mock.Mock()

        mock_hook.return_value.get_sdf_download_operation.return_value = operation
        mock_hook.return_value.download_media.return_value = media

        tmp_dir = "/tmp/mock_dir"
        temp_dir_mock.return_value.__enter__.return_value = tmp_dir

        # Mock os behavior
        os_mock.path.join.side_effect = lambda *args: "/".join(args)
        os_mock.listdir.return_value = [FILENAME]

        # Mock zipfile behavior
        zipfile_mock.ZipFile.return_value.__enter__.return_value.extractall.return_value = None

        op = GoogleDisplayVideo360SDFtoGCSOperator(
            operation_name=OPERATION_NAME,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=False,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=None)

        # Assertions
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_sdf_download_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        mock_hook.return_value.download_media.assert_called_once_with(resource_name=RESOURCE_NAME)
        mock_hook.return_value.download_content_from_request.assert_called_once()

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=f"{tmp_dir}/{FILENAME}",
            gzip=False,
        )

        assert result == f"{BUCKET_NAME}/{OBJECT_NAME}"


class TestGoogleDisplayVideo360CreateSDFDownloadTaskOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, mock_hook):
        body_request = {
            "version": "1",
            "id": "id",
            "filter": {"id": []},
        }
        test_name = "test_task"

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        mock_hook.return_value.create_sdf_download_operation.return_value = {"name": test_name}

        op = GoogleDisplayVideo360CreateSDFDownloadTaskOperator(
            body_request=body_request,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )

        op.execute(context=mock_context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_hook.return_value.create_sdf_download_operation.assert_called_once()
        mock_hook.return_value.create_sdf_download_operation.assert_called_once_with(
            body_request=body_request
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="name", value=test_name)
