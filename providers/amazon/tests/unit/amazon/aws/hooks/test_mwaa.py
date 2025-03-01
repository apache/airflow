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
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook

ENV_NAME = "test_env"
PATH = "/dags/test_dag/dagRuns"
METHOD = "POST"
QUERY_PARAMS = {"limit": 30}


class TestMwaaHook:
    def setup_method(self):
        self.hook = MwaaHook()

        # these example responses are included here instead of as a constant because the hook will mutate
        # responses causing subsequent tests to fail
        self.example_responses = {
            "success": {
                "ResponseMetadata": {
                    "RequestId": "some ID",
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {"header1": "value1"},
                    "RetryAttempts": 0,
                },
                "RestApiStatusCode": 200,
                "RestApiResponse": {
                    "conf": {},
                    "dag_id": "hello_world",
                    "dag_run_id": "manual__2025-02-08T00:33:09.457198+00:00",
                    "data_interval_end": "2025-02-08T00:33:09.457198+00:00",
                    "data_interval_start": "2025-02-08T00:33:09.457198+00:00",
                    "execution_date": "2025-02-08T00:33:09.457198+00:00",
                    "logical_date": "2025-02-08T00:33:09.457198+00:00",
                    "run_type": "manual",
                    "state": "queued",
                },
            },
            "failure": {
                "Error": {"Message": "", "Code": "RestApiClientException"},
                "ResponseMetadata": {
                    "RequestId": "some ID",
                    "HTTPStatusCode": 400,
                    "HTTPHeaders": {"header1": "value1"},
                    "RetryAttempts": 0,
                },
                "RestApiStatusCode": 404,
                "RestApiResponse": {
                    "detail": "DAG with dag_id: 'hello_world1' not found",
                    "status": 404,
                    "title": "DAG not found",
                    "type": "https://airflow.apache.org/docs/apache-airflow/2.10.3/stable-rest-api-ref.html#section/Errors/NotFound",
                },
            },
        }

    def test_init(self):
        assert self.hook.client_type == "mwaa"

    @mock_aws
    def test_get_conn(self):
        assert self.hook.conn is not None

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(None, id="no_body"),
            pytest.param({"conf": {}}, id="non_empty_body"),
        ],
    )
    @mock.patch.object(MwaaHook, "conn")
    def test_invoke_rest_api_success(self, mock_conn, body) -> None:
        boto_invoke_mock = mock.MagicMock(return_value=self.example_responses["success"])
        mock_conn.invoke_rest_api = boto_invoke_mock

        retval = self.hook.invoke_rest_api(ENV_NAME, PATH, METHOD, body, QUERY_PARAMS)
        kwargs_to_assert = {
            "Name": ENV_NAME,
            "Path": PATH,
            "Method": METHOD,
            "Body": body if body else {},
            "QueryParameters": QUERY_PARAMS,
        }
        boto_invoke_mock.assert_called_once_with(**kwargs_to_assert)
        assert retval == {
            k: v for k, v in self.example_responses["success"].items() if k != "ResponseMetadata"
        }

    @mock.patch.object(MwaaHook, "conn")
    def test_invoke_rest_api_failure(self, mock_conn) -> None:
        error = ClientError(
            error_response=self.example_responses["failure"], operation_name="invoke_rest_api"
        )
        boto_invoke_mock = mock.MagicMock(side_effect=error)
        mock_conn.invoke_rest_api = boto_invoke_mock
        mock_log = mock.MagicMock()
        self.hook.log.error = mock_log

        with pytest.raises(ClientError) as caught_error:
            self.hook.invoke_rest_api(ENV_NAME, PATH, METHOD)

        assert caught_error.value == error
        expected_log = {
            k: v
            for k, v in self.example_responses["failure"].items()
            if k != "ResponseMetadata" and k != "Error"
        }
        mock_log.assert_called_once_with(expected_log)
