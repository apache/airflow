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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.athena import AthenaHook

MOCK_DATA = {
    "query": "SELECT * FROM TEST_TABLE",
    "database": "TEST_DATABASE",
    "outputLocation": "s3://test_s3_bucket/",
    "client_request_token": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "workgroup": "primary",
    "query_execution_id": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "next_token_id": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "max_items": 1000,
}

mock_query_context = {"Database": MOCK_DATA["database"]}
mock_result_configuration = {"OutputLocation": MOCK_DATA["outputLocation"]}

MOCK_RUNNING_QUERY_EXECUTION = {"QueryExecution": {"Status": {"State": "RUNNING"}}}
MOCK_SUCCEEDED_QUERY_EXECUTION = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

MOCK_QUERY_EXECUTION = {"QueryExecutionId": MOCK_DATA["query_execution_id"]}

MOCK_QUERY_EXECUTION_OUTPUT = {
    "QueryExecution": {
        "QueryExecutionId": MOCK_DATA["query_execution_id"],
        "ResultConfiguration": {"OutputLocation": "s3://test_bucket/test.csv"},
    }
}


class TestAthenaHook(unittest.TestCase):
    def setUp(self):
        self.athena = AthenaHook(sleep_time=0)

    def test_init(self):
        assert self.athena.aws_conn_id == "aws_default"
        assert self.athena.sleep_time == 0

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_without_token(self, mock_conn):
        mock_conn.return_value.start_query_execution.return_value = MOCK_QUERY_EXECUTION
        result = self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
        )
        expected_call_params = {
            "QueryString": MOCK_DATA["query"],
            "QueryExecutionContext": mock_query_context,
            "ResultConfiguration": mock_result_configuration,
            "WorkGroup": MOCK_DATA["workgroup"],
        }
        mock_conn.return_value.start_query_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["query_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_with_token(self, mock_conn):
        mock_conn.return_value.start_query_execution.return_value = MOCK_QUERY_EXECUTION
        result = self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
        )
        expected_call_params = {
            "QueryString": MOCK_DATA["query"],
            "QueryExecutionContext": mock_query_context,
            "ResultConfiguration": mock_result_configuration,
            "ClientRequestToken": MOCK_DATA["client_request_token"],
            "WorkGroup": MOCK_DATA["workgroup"],
        }
        mock_conn.return_value.start_query_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["query_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_non_succeeded_query(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.get_query_results(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_default_params(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results(query_execution_id=MOCK_DATA["query_execution_id"])
        expected_call_params = {"QueryExecutionId": MOCK_DATA["query_execution_id"], "MaxResults": 1000}
        mock_conn.return_value.get_query_results.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_next_token(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results(
            query_execution_id=MOCK_DATA["query_execution_id"], next_token_id=MOCK_DATA["next_token_id"]
        )
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "NextToken": MOCK_DATA["next_token_id"],
            "MaxResults": 1000,
        }
        mock_conn.return_value.get_query_results.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_non_succeeded_query(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.get_query_results_paginator(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_default_params(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results_paginator(query_execution_id=MOCK_DATA["query_execution_id"])
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "PaginationConfig": {"MaxItems": None, "PageSize": None, "StartingToken": None},
        }
        mock_conn.return_value.get_paginator.return_value.paginate.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_pagination_config(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results_paginator(
            query_execution_id=MOCK_DATA["query_execution_id"],
            max_items=MOCK_DATA["max_items"],
            page_size=MOCK_DATA["max_items"],
            starting_token=MOCK_DATA["next_token_id"],
        )
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "PaginationConfig": {
                "MaxItems": MOCK_DATA["max_items"],
                "PageSize": MOCK_DATA["max_items"],
                "StartingToken": MOCK_DATA["next_token_id"],
            },
        }
        mock_conn.return_value.get_paginator.return_value.paginate.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_poll_query_when_final(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        result = self.athena.poll_query_status(query_execution_id=MOCK_DATA["query_execution_id"])
        mock_conn.return_value.get_query_execution.assert_called_once()
        assert result == "SUCCEEDED"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_poll_query_with_timeout(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.poll_query_status(
            query_execution_id=MOCK_DATA["query_execution_id"],
            max_polling_attempts=1,
        )
        mock_conn.return_value.get_query_execution.assert_called_once()
        assert result == "RUNNING"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_output_location(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_QUERY_EXECUTION_OUTPUT
        result = self.athena.get_output_location(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result == "s3://test_bucket/test.csv"


if __name__ == "__main__":
    unittest.main()
