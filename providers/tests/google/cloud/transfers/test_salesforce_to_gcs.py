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

import pytest

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.salesforce_to_gcs import SalesforceToGcsOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

TASK_ID = "test-task-id"
QUERY = "SELECT id, company FROM Lead WHERE company = 'Hello World Inc'"
SALESFORCE_CONNECTION_ID = "test-salesforce-connection"
GCS_BUCKET = "test-bucket"
GCS_OBJECT_PATH = "path/to/test-file-path"
EXPECTED_GCS_URI = f"gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}"
GCP_CONNECTION_ID = "google_cloud_default"
SALESFORCE_RESPONSE = {
    "records": [
        {
            "attributes": {"type": "Lead", "url": "/services/data/v42.0/sobjects/Lead/00Q3t00001eJ7AnEAK"},
            "Id": "00Q3t00001eJ7AnEAK",
            "Company": "Hello World Inc",
        }
    ],
    "totalSize": 1,
    "done": True,
}
INCLUDE_DELETED = True
QUERY_PARAMS = {"DEFAULT_SETTING": "ENABLED"}


class TestSalesforceToGcsOperator:
    @pytest.mark.db_test
    @mock.patch.object(GCSHook, "upload")
    @mock.patch.object(SalesforceHook, "write_object_to_file")
    @mock.patch.object(SalesforceHook, "make_query")
    def test_execute(self, mock_make_query, mock_write_object_to_file, mock_upload):
        mock_make_query.return_value = SALESFORCE_RESPONSE

        operator = SalesforceToGcsOperator(
            query=QUERY,
            bucket_name=GCS_BUCKET,
            object_name=GCS_OBJECT_PATH,
            salesforce_conn_id=SALESFORCE_CONNECTION_ID,
            gcp_conn_id=GCP_CONNECTION_ID,
            include_deleted=INCLUDE_DELETED,
            query_params=QUERY_PARAMS,
            export_format="json",
            coerce_to_timestamp=True,
            record_time_added=True,
            task_id=TASK_ID,
        )
        result = operator.execute({})

        mock_make_query.assert_called_once_with(
            query=QUERY, include_deleted=INCLUDE_DELETED, query_params=QUERY_PARAMS
        )

        mock_write_object_to_file.assert_called_once_with(
            query_results=SALESFORCE_RESPONSE["records"],
            filename=mock.ANY,
            fmt="json",
            coerce_to_timestamp=True,
            record_time_added=True,
        )

        mock_upload.assert_called_once_with(
            bucket_name=GCS_BUCKET, object_name=GCS_OBJECT_PATH, filename=mock.ANY, gzip=False
        )

        assert EXPECTED_GCS_URI == result
