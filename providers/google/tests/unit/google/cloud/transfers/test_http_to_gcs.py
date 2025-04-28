#!/usr/bin/env python
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

from airflow.providers.google.cloud.transfers.http_to_gcs import HttpToGCSOperator

TASK_ID = "test-http-to-gcs-operator"
GCP_CONN_ID = "GCP_CONN_ID"
HTTP_CONN_ID = "HTPP_CONN_ID"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_MIME_TYPE = "application/octet-stream"

TEST_BUCKET = "test-bucket"
SOURCE_ENDPOINT = "main_dir/test_object3.json"
DESTINATION_PATH_FILE = "destination_dir/copy.txt"
ENDPOINT = "/"


class TestHttpToGCSOperator:
    def test_init(self):
        operator = HttpToGCSOperator(
            task_id="http_to_gcs_operator",
            http_conn_id=HTTP_CONN_ID,
            endpoint=ENDPOINT,
            object_name=DESTINATION_PATH_FILE,
            bucket_name=TEST_BUCKET,
        )
        assert operator.endpoint == ENDPOINT
        assert operator.object_name == DESTINATION_PATH_FILE
        assert operator.bucket_name == TEST_BUCKET
        assert operator.http_conn_id == HTTP_CONN_ID

    @mock.patch("airflow.providers.google.cloud.transfers.http_to_gcs.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.http_to_gcs.HttpHook")
    def test_execute(self, http_hook, gcs_hook):
        task = HttpToGCSOperator(
            task_id="http_to_gcs_operator",
            http_conn_id=HTTP_CONN_ID,
            endpoint=ENDPOINT,
            object_name=DESTINATION_PATH_FILE,
            bucket_name=TEST_BUCKET,
        )
        task.execute(None)
        gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        gcs_hook.return_value.get_bucket.assert_called_once_with(TEST_BUCKET)
        http_hook.assert_called_once_with(HTTP_CONN_ID)

        gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=TEST_BUCKET,
            object_name=DESTINATION_PATH_FILE,
            filename=mock.ANY,
            mime_type=DEFAULT_MIME_TYPE,
            gzip=False,
        )
        gcs_hook.return_value.upload.assert_not_called()
        gcs_hook.return_value.get_bucket.return_value.blob.assert_called_once_with(DESTINATION_PATH_FILE)
