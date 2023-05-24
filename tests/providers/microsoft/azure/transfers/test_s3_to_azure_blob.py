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

from airflow.providers.microsoft.azure.transfers.s3_to_azure_blob import S3ToAzureBlobOperator

WASB_CONN_ID = "wasb_default"
AWS_CONN_ID = "aws_default"
BLOB_NAME = "azure_blob"
FILE_PATH = "/file/to/path"
CONTAINER_NAME = "azure_container"
BUCKET_NAME = "airflow"
KEY_NAME = "file.txt"
TASK_ID = "transfer_file"


class TestS3ToAzureBlobOperator:
    def test_init(self):
        operator = S3ToAzureBlobOperator(
            aws_conn_id=AWS_CONN_ID,
            wasb_conn_id=WASB_CONN_ID,
            s3_bucket=BUCKET_NAME,
            s3_key=KEY_NAME,
            blob_name=BLOB_NAME,
            container_name=CONTAINER_NAME,
            task_id=TASK_ID,
        )
        assert operator.aws_conn_id == AWS_CONN_ID
        assert operator.wasb_conn_id == WASB_CONN_ID
        assert operator.s3_bucket == BUCKET_NAME
        assert operator.s3_key == KEY_NAME
        assert operator.blob_name == BLOB_NAME
        assert operator.container_name == CONTAINER_NAME
        assert operator.task_id == TASK_ID

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_azure_blob.WasbHook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_azure_blob.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_azure_blob.tempfile")
    def test_execute(self, mock_temp, mock_hook_s3, mock_hook_wasb):
        op = S3ToAzureBlobOperator(
            aws_conn_id=AWS_CONN_ID,
            wasb_conn_id=WASB_CONN_ID,
            s3_bucket=BUCKET_NAME,
            s3_key=KEY_NAME,
            blob_name=BLOB_NAME,
            container_name=CONTAINER_NAME,
            create_container=True,
            task_id=TASK_ID,
        )

        op.execute(context=None)

        mock_hook_s3.assert_called_once_with(AWS_CONN_ID)
        mock_hook_s3.return_value.get_conn.assert_called_once()
        mock_hook_s3.return_value.get_conn.return_value.download_file.assert_called_once_with(
            BUCKET_NAME, KEY_NAME, mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name
        )

        mock_hook_wasb.assert_called_once_with(wasb_conn_id=WASB_CONN_ID)
        mock_hook_wasb.return_value.load_file.assert_called_once_with(
            file_path=mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name,
            container_name=CONTAINER_NAME,
            blob_name=BLOB_NAME,
            create_container=True,
        )
