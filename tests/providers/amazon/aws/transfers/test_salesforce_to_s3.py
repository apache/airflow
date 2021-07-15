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

import unittest
from collections import OrderedDict
from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator

TASK_ID = "test-task-id"
QUERY = "SELECT id, company FROM Lead WHERE company = 'Hello World Inc'"
SALESFORCE_CONNECTION_ID = "test-salesforce-connection"
S3_BUCKET = "test-bucket"
S3_OBJECT_PATH = "path/to/test-file-path"
EXPECTED_S3_URI = f"s3://{S3_BUCKET}/{S3_OBJECT_PATH}"
AWS_CONNECTION_ID = "aws_default"
SALESFORCE_RESPONSE = {
    'records': [
        OrderedDict(
            [
                (
                    'attributes',
                    OrderedDict(
                        [('type', 'Lead'), ('url', '/services/data/v42.0/sobjects/Lead/00Q3t00001eJ7AnEAK')]
                    ),
                ),
                ('Id', '00Q3t00001eJ7AnEAK'),
                ('Company', 'Hello World Inc'),
            ]
        )
    ],
    'totalSize': 1,
    'done': True,
}
INCLUDE_DELETED = True
QUERY_PARAMS = {"DEFAULT_SETTING": "ENABLED"}


class TestSalesforceToGcsOperator(unittest.TestCase):
    @mock.patch.object(S3Hook, 'load_file')
    @mock.patch.object(SalesforceHook, 'write_object_to_file')
    @mock.patch.object(SalesforceHook, 'make_query')
    def test_execute(self, mock_make_query, mock_write_object_to_file, mock_load_file):
        mock_make_query.return_value = SALESFORCE_RESPONSE

        operator = SalesforceToS3Operator(
            task_id=TASK_ID,
            salesforce_query=QUERY,
            s3_bucket_name=S3_BUCKET,
            s3_key=S3_OBJECT_PATH,
            salesforce_conn_id=SALESFORCE_CONNECTION_ID,
            export_format="json",
            query_params=QUERY_PARAMS,
            include_deleted=INCLUDE_DELETED,
            coerce_to_timestamp=True,
            record_time_added=True,
            aws_conn_id=AWS_CONNECTION_ID,
            replace=False,
            encrypt=False,
            gzip=False,
            acl_policy=None,
        )
        result = operator.execute({})

        mock_make_query.assert_called_once_with(
            query=QUERY, include_deleted=INCLUDE_DELETED, query_params=QUERY_PARAMS
        )

        mock_write_object_to_file.assert_called_once_with(
            query_results=SALESFORCE_RESPONSE['records'],
            filename=mock.ANY,
            fmt="json",
            coerce_to_timestamp=True,
            record_time_added=True,
        )

        mock_load_file.assert_called_once_with(
            bucket_name=S3_BUCKET,
            key=S3_OBJECT_PATH,
            filename=mock.ANY,
            replace=False,
            encrypt=False,
            gzip=False,
            acl_policy=None,
        )

        assert EXPECTED_S3_URI == result
