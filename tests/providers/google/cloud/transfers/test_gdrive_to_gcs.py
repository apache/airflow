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

import os
from unittest import mock

from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator

FOLDER_ID = os.environ.get("GCP_GDRIVE_FOLDER_ID", "abcd1234")
DRIVE_ID = os.environ.get("GCP_GDRIVE_DRIVE_ID", "abcd1234")
FILE_NAME = os.environ.get("GCP_GDRIVE_TO_GCS_FILE_NAME", "gdrive_to_gcs_file.txt")
BUCKET = os.environ.get("GCP_GDRIVE_TO_GCS_BUCKET", "gdrive-to-gcs-bucket")
OBJECT = "prefix/test.txt"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestGoogleDriveToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.gdrive_to_gcs.BytesIO")
    @mock.patch("airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveHook")
    def test_upload_data(self, mock_gdrive_hook, mock_file_handle):
        mock_gdrive_hook.return_value.get_media_request.return_value = mock.MagicMock()

        file_id = mock_gdrive_hook.get_file_id.return_value["id"]
        mime_type = mock_gdrive_hook.get_file_id.return_value["mime_type"]

        mock_gcs_hook = mock.Mock()

        op = GoogleDriveToGCSOperator(
            task_id="test_task",
            folder_id=FOLDER_ID,
            file_name=FILE_NAME,
            drive_id=DRIVE_ID,
            destination_bucket=BUCKET,
            destination_object=OBJECT,
        )

        op._upload_data(
            gcs_hook=mock_gcs_hook,
            gdrive_hook=mock_gdrive_hook,
        )
        # Test writing to file
        mock_gdrive_hook.get_media_request.assert_called_once_with(file_id=file_id)
        mock_gdrive_hook.download_content_from_request.assert_called_once_with(
            file_handle=mock_file_handle(),
            request=mock_gdrive_hook.get_media_request.return_value,
            chunk_size=104857600,
        )

        # Test upload
        mock_gcs_hook.upload.assert_called_once_with(
            bucket_name=BUCKET, object_name=OBJECT, data=mock_file_handle().getvalue(), mime_type=mime_type
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gdrive_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveHook")
    @mock.patch(
        "airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveToGCSOperator._upload_data"
    )
    def test_execute(self, mock_upload_data, mock_gdrive_hook, mock_gcs_hook):
        context = {}
        op = GoogleDriveToGCSOperator(
            task_id="test_task",
            folder_id=FOLDER_ID,
            file_name=FILE_NAME,
            drive_id=DRIVE_ID,
            destination_bucket=BUCKET,
            destination_object=OBJECT,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context)

        mock_gdrive_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
