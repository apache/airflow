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
import requests
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook

ENV_NAME = "test_env"
PATH = "/dags/test_dag/dagRuns"
METHOD = "POST"
BODY: dict = {"conf": {}}
QUERY_PARAMS = {"limit": 30}
HOSTNAME = "example.com"


class TestMwaaHook:
    @pytest.fixture
    def mock_conn(self):
        with mock.patch.object(MwaaHook, "conn") as m:
            yield m

    def setup_method(self):
        self.hook = MwaaHook()

    def test_init(self):
        assert self.hook.client_type == "mwaa"

    @mock_aws
    def test_get_conn(self):
        assert self.hook.conn is not None

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(None, id="no_body"),
            pytest.param(BODY, id="non_empty_body"),
        ],
    )
    def test_invoke_rest_api_success(self, body, mock_conn, example_responses):
        boto_invoke_mock = mock.MagicMock(return_value=example_responses["success"])
        mock_conn.invoke_rest_api = boto_invoke_mock

        retval = self.hook.invoke_rest_api(
            env_name=ENV_NAME, path=PATH, method=METHOD, body=body, query_params=QUERY_PARAMS
        )
        kwargs_to_assert = {
            "Name": ENV_NAME,
            "Path": PATH,
            "Method": METHOD,
            "Body": body if body else {},
            "QueryParameters": QUERY_PARAMS,
        }
        boto_invoke_mock.assert_called_once_with(**kwargs_to_assert)
        mock_conn.create_web_login_token.assert_not_called()
        assert retval == {k: v for k, v in example_responses["success"].items() if k != "ResponseMetadata"}

    def test_invoke_rest_api_failure(self, mock_conn, example_responses):
        error = ClientError(error_response=example_responses["failure"], operation_name="invoke_rest_api")
        mock_conn.invoke_rest_api = mock.MagicMock(side_effect=error)
        mock_error_log = mock.MagicMock()
        self.hook.log.error = mock_error_log

        with pytest.raises(ClientError) as caught_error:
            self.hook.invoke_rest_api(env_name=ENV_NAME, path=PATH, method=METHOD)

        assert caught_error.value == error
        mock_conn.create_web_login_token.assert_not_called()
        expected_log = {k: v for k, v in example_responses["failure"].items() if k != "ResponseMetadata"}
        mock_error_log.assert_called_once_with(expected_log)

    @pytest.mark.parametrize("generate_local_token", [pytest.param(True), pytest.param(False)])
    @mock.patch("airflow.providers.amazon.aws.hooks.mwaa.requests.Session")
    def test_invoke_rest_api_local_token_parameter(
        self, mock_create_session, generate_local_token, mock_conn
    ):
        self.hook.invoke_rest_api(
            env_name=ENV_NAME, path=PATH, method=METHOD, generate_local_token=generate_local_token
        )
        if generate_local_token:
            mock_conn.invoke_rest_api.assert_not_called()
            mock_conn.create_web_login_token.assert_called_once()
            mock_create_session.assert_called_once()
            mock_create_session.return_value.request.assert_called_once()
        else:
            mock_conn.invoke_rest_api.assert_called_once()

    @mock.patch.object(MwaaHook, "_get_session_conn")
    def test_invoke_rest_api_fallback_success_when_iam_fails_with_airflow2(
        self, mock_get_session_conn, mock_conn, example_responses
    ):
        boto_invoke_error = ClientError(
            error_response=example_responses["missingIamRole"], operation_name="invoke_rest_api"
        )
        mock_conn.invoke_rest_api = mock.MagicMock(side_effect=boto_invoke_error)

        kwargs_to_assert = {
            "method": METHOD,
            "url": f"https://{HOSTNAME}/api/v1{PATH}",
            "params": QUERY_PARAMS,
            "headers": {},
            "json": BODY,
            "timeout": 10,
        }

        mock_response = mock.MagicMock()
        mock_response.status_code = example_responses["success"]["RestApiStatusCode"]
        mock_response.json.return_value = example_responses["success"]["RestApiResponse"]
        mock_session = mock.MagicMock()
        mock_session.request.return_value = mock_response

        mock_get_session_conn.return_value = (mock_session, HOSTNAME, None)

        retval = self.hook.invoke_rest_api(
            env_name=ENV_NAME, path=PATH, method=METHOD, body=BODY, query_params=QUERY_PARAMS
        )

        mock_session.request.assert_called_once_with(**kwargs_to_assert)
        mock_response.raise_for_status.assert_called_once()
        assert retval == {k: v for k, v in example_responses["success"].items() if k != "ResponseMetadata"}

    @mock.patch.object(MwaaHook, "_get_session_conn")
    def test_invoke_rest_api_fallback_success_when_iam_fails_with_airflow3(
        self, mock_get_session_conn, mock_conn, example_responses
    ):
        boto_invoke_error = ClientError(
            error_response=example_responses["missingIamRole"], operation_name="invoke_rest_api"
        )
        mock_conn.invoke_rest_api = mock.MagicMock(side_effect=boto_invoke_error)

        kwargs_to_assert = {
            "method": METHOD,
            "url": f"https://{HOSTNAME}/api/v2{PATH}",
            "params": QUERY_PARAMS,
            "headers": {
                "Authorization": "Bearer token",
                "Content-Type": "application/json",
            },
            "json": BODY,
            "timeout": 10,
        }

        mock_response = mock.MagicMock()
        mock_response.status_code = example_responses["success"]["RestApiStatusCode"]
        mock_response.json.return_value = example_responses["success"]["RestApiResponse"]
        mock_session = mock.MagicMock()
        mock_session.request.return_value = mock_response
        mock_login_response = mock.MagicMock()
        mock_login_response.cookies = {"_token": "token"}

        mock_get_session_conn.return_value = (mock_session, HOSTNAME, mock_login_response)

        retval = self.hook.invoke_rest_api(
            env_name=ENV_NAME,
            path=PATH,
            method=METHOD,
            body=BODY,
            query_params=QUERY_PARAMS,
            airflow_version=3,
        )

        mock_session.request.assert_called_once_with(**kwargs_to_assert)
        mock_response.raise_for_status.assert_called_once()
        assert retval == {k: v for k, v in example_responses["success"].items() if k != "ResponseMetadata"}

    @mock.patch.object(MwaaHook, "_get_session_conn")
    def test_invoke_rest_api_using_local_session_token_failure(
        self, mock_get_session_conn, example_responses
    ):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = example_responses["failure"]["RestApiResponse"]
        error = requests.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = error

        mock_session = mock.MagicMock()
        mock_session.request.return_value = mock_response

        mock_get_session_conn.return_value = (mock_session, HOSTNAME, None)

        mock_error_log = mock.MagicMock()
        self.hook.log.error = mock_error_log

        with pytest.raises(requests.HTTPError) as caught_error:
            self.hook.invoke_rest_api(env_name=ENV_NAME, path=PATH, method=METHOD, generate_local_token=True)

        assert caught_error.value == error
        mock_error_log.assert_called_once_with(example_responses["failure"]["RestApiResponse"])

    @mock.patch("airflow.providers.amazon.aws.hooks.mwaa.requests.Session")
    def test_get_session_conn_airflow2(self, mock_create_session, mock_conn):
        token = "token"
        mock_conn.create_web_login_token.return_value = {"WebServerHostname": HOSTNAME, "WebToken": token}
        login_url = f"https://{HOSTNAME}/aws_mwaa/login"
        login_payload = {"token": token}

        mock_session = mock.MagicMock()
        mock_login_response = mock.MagicMock()
        mock_session.post.return_value = mock_login_response
        mock_create_session.return_value = mock_session

        retval = self.hook._get_session_conn(env_name=ENV_NAME, airflow_version=2)

        mock_conn.create_web_login_token.assert_called_once_with(Name=ENV_NAME)
        mock_create_session.assert_called_once_with()
        mock_session.post.assert_called_once_with(login_url, data=login_payload, timeout=10)
        mock_session.post.return_value.raise_for_status.assert_called_once()

        assert retval == (mock_session, HOSTNAME, mock_login_response)

    @mock.patch("airflow.providers.amazon.aws.hooks.mwaa.requests.Session")
    def test_get_session_conn_airflow3(self, mock_create_session, mock_conn):
        token = "token"
        mock_conn.create_web_login_token.return_value = {"WebServerHostname": HOSTNAME, "WebToken": token}
        login_url = f"https://{HOSTNAME}/pluginsv2/aws_mwaa/login"
        login_payload = {"token": token}

        mock_session = mock.MagicMock()
        mock_login_response = mock.MagicMock()
        mock_session.post.return_value = mock_login_response
        mock_create_session.return_value = mock_session

        retval = self.hook._get_session_conn(env_name=ENV_NAME, airflow_version=3)

        mock_conn.create_web_login_token.assert_called_once_with(Name=ENV_NAME)
        mock_create_session.assert_called_once_with()
        mock_session.post.assert_called_once_with(login_url, data=login_payload, timeout=10)
        mock_session.post.return_value.raise_for_status.assert_called_once()

        assert retval == (mock_session, HOSTNAME, mock_login_response)

    @pytest.fixture
    def example_responses(self):
        """Fixture for test responses to avoid mutation between tests."""
        return {
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
            "missingIamRole": {
                "Error": {"Message": "No Airflow role granted in IAM.", "Code": "AccessDeniedException"},
                "ResponseMetadata": {
                    "RequestId": "some ID",
                    "HTTPStatusCode": 403,
                    "HTTPHeaders": {"header1": "value1"},
                    "RetryAttempts": 0,
                },
            },
        }
