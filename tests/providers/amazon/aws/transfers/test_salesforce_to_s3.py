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

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

pytestmark = pytest.mark.db_test


TASK_ID = "test-task-id"
QUERY = "SELECT id, company FROM Lead WHERE company = 'Hello World Inc'"
SALESFORCE_CONNECTION_ID = "test-salesforce-connection"
S3_BUCKET = "test-bucket"
S3_KEY = "path/to/test-file-path/test-file.json"
AWS_CONNECTION_ID = "aws_default"
SALESFORCE_RESPONSE = {
    "records": [
        {
            "attributes": {
                "type": "Lead",
                "url": "/services/data/v42.0/sobjects/Lead/00Q3t00001eJ7AnEAK",
            },
            "Id": "00Q3t00001eJ7AnEAK",
            "Company": "Hello World Inc",
        }
    ],
    "totalSize": 1,
    "done": True,
}
QUERY_PARAMS = {"DEFAULT_SETTING": "ENABLED"}
EXPORT_FORMAT = "json"
INCLUDE_DELETED = COERCE_TO_TIMESTAMP = RECORD_TIME_ADDED = True
REPLACE = ENCRYPT = GZIP = False
ACL_POLICY = None


class TestSalesforceToGcsOperator:
    @mock.patch.object(S3Hook, "load_file")
    @mock.patch.object(SalesforceHook, "write_object_to_file")
    @mock.patch.object(SalesforceHook, "make_query")
    def test_execute(self, mock_make_query, mock_write_object_to_file, mock_load_file):
        mock_make_query.return_value = SALESFORCE_RESPONSE

        operator = SalesforceToS3Operator(
            task_id=TASK_ID,
            salesforce_query=QUERY,
            s3_bucket_name=S3_BUCKET,
            s3_key=S3_KEY,
            salesforce_conn_id=SALESFORCE_CONNECTION_ID,
            export_format=EXPORT_FORMAT,
            query_params=QUERY_PARAMS,
            include_deleted=INCLUDE_DELETED,
            coerce_to_timestamp=COERCE_TO_TIMESTAMP,
            record_time_added=RECORD_TIME_ADDED,
            aws_conn_id=AWS_CONNECTION_ID,
            replace=REPLACE,
            encrypt=ENCRYPT,
            gzip=GZIP,
            acl_policy=ACL_POLICY,
        )

        assert operator.task_id == TASK_ID
        assert operator.salesforce_query == QUERY
        assert operator.s3_bucket_name == S3_BUCKET
        assert operator.s3_key == S3_KEY
        assert operator.salesforce_conn_id == SALESFORCE_CONNECTION_ID
        assert operator.export_format == EXPORT_FORMAT
        assert operator.query_params == QUERY_PARAMS
        assert operator.include_deleted == INCLUDE_DELETED
        assert operator.coerce_to_timestamp == COERCE_TO_TIMESTAMP
        assert operator.record_time_added == RECORD_TIME_ADDED
        assert operator.aws_conn_id == AWS_CONNECTION_ID
        assert operator.replace == REPLACE
        assert operator.encrypt == ENCRYPT
        assert operator.gzip == GZIP
        assert operator.acl_policy == ACL_POLICY

        assert f"s3://{S3_BUCKET}/{S3_KEY}" == operator.execute({})

        mock_make_query.assert_called_once_with(
            query=QUERY, include_deleted=INCLUDE_DELETED, query_params=QUERY_PARAMS
        )

        mock_write_object_to_file.assert_called_once_with(
            query_results=SALESFORCE_RESPONSE["records"],
            filename=mock.ANY,
            fmt=EXPORT_FORMAT,
            coerce_to_timestamp=COERCE_TO_TIMESTAMP,
            record_time_added=RECORD_TIME_ADDED,
        )

        mock_load_file.assert_called_once_with(
            bucket_name=S3_BUCKET,
            key=S3_KEY,
            filename=mock.ANY,
            replace=REPLACE,
            encrypt=ENCRYPT,
            gzip=GZIP,
            acl_policy=ACL_POLICY,
        )
