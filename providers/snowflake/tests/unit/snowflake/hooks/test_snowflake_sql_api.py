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

import base64
import unittest
import uuid
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import AsyncMock, PropertyMock, call

import aiohttp
import pytest
import requests
import tenacity
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHook,
)

if TYPE_CHECKING:
    from pathlib import Path

SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)
_PASSWORD = "snowflake42"

SINGLE_STMT = "select i from user_test order by i;"
BASE_CONNECTION_KWARGS: dict = {
    "login": "user",
    "conn_type": "snowflake",
    "password": "pw",
    "schema": "public",
    "extra": {
        "database": "db",
        "account": "airflow",
        "warehouse": "af_wh",
        "region": "af_region",
        "role": "af_role",
    },
}

CONN_PARAMS = {
    "account": "airflow",
    "application": "AIRFLOW",
    "authenticator": "snowflake",
    "database": "db",
    "password": "pw",
    "region": "af_region",
    "role": "af_role",
    "schema": "public",
    "session_parameters": None,
    "user": "user",
    "warehouse": "af_wh",
}

CONN_PARAMS_OAUTH = {
    "account": "airflow",
    "application": "AIRFLOW",
    "authenticator": "oauth",
    "database": "db",
    "client_id": "test_client_id",
    "client_secret": "test_client_pw",
    "refresh_token": "secrettoken",
    "region": "af_region",
    "role": "af_role",
    "schema": "public",
    "session_parameters": None,
    "warehouse": "af_wh",
}

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer newT0k3n",
    "Accept": "application/json",
    "User-Agent": "snowflakeSQLAPI/1.0",
    "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
}

HEADERS_OAUTH = {
    "Content-Type": "application/json",
    "Authorization": "Bearer newT0k3n",
    "Accept": "application/json",
    "User-Agent": "snowflakeSQLAPI/1.0",
    "X-Snowflake-Authorization-Token-Type": "OAUTH",
}


GET_RESPONSE = {
    "resultSetMetaData": {
        "numRows": 10000,
        "format": "jsonv2",
        "rowType": [
            {
                "name": "SEQ8()",
                "database": "",
                "schema": "",
                "table": "",
                "scale": 0,
                "precision": 19,
            },
            {
                "name": "RANDSTR(1000, RANDOM())",
                "database": "",
                "schema": "",
                "table": "",
            },
        ],
        "partitionInfo": [
            {
                "rowCount": 12344,
                "uncompressedSize": 14384873,
            },
            {"rowCount": 43746, "uncompressedSize": 43748274, "compressedSize": 746323},
        ],
    },
    "code": "090001",
    "statementStatusUrl": "/api/v2/statements/{handle}?requestId={id5}&partition=10",
    "sqlState": "00000",
    "statementHandle": "{handle}",
    "message": "Statement executed successfully.",
    "createdOn": 1620151693299,
}

HOOK_PARAMS: dict = {
    "database": "airflow_db",
    "schema": "airflow_schema",
    "warehouse": "airflow_warehouse",
    "role": "airflow_role",
}

API_URL = "https://test.snowflakecomputing.com/api/v2/statements/test"


@pytest.fixture
def mock_requests():
    with mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.requests.Session"
    ) as mock_session_cls:
        mock_session = mock.MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session
        yield mock_session


@pytest.fixture
def mock_async_request():
    with mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.aiohttp.ClientSession.request"
    ) as mock_session_cls:
        mock_request = mock.MagicMock()
        mock_session_cls.return_value = mock_request
        yield mock_request


def create_successful_response_mock(content):
    """Create mock response for success state"""
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response


def create_post_side_effect(status_code=429):
    """create mock response for post side effect"""
    response = mock.MagicMock()
    response.status_code = status_code
    response.reason = "test"
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    return response


def create_async_request_client_response_error(request_info=None, history=None, status_code=429):
    """Create mock response for async request side effect"""
    response = mock.MagicMock()
    request_info = mock.MagicMock() if request_info is None else request_info
    history = mock.MagicMock() if history is None else history
    response.status = status_code
    response.reason = f"{status_code} Error"
    response.raise_for_status.side_effect = aiohttp.ClientResponseError(
        request_info=request_info, history=history, status=status_code, message=response.reason
    )
    return response


def create_async_connection_error():
    response = mock.MagicMock()
    response.raise_for_status.side_effect = aiohttp.ClientConnectionError()
    return response


def create_async_request_client_response_success(json=GET_RESPONSE, status_code=200):
    """Create mock response for async request side effect"""
    response = mock.MagicMock()
    response.status = status_code
    response.reason = "test"
    response.json = AsyncMock(return_value=json)
    response.raise_for_status.side_effect = None
    return response


class TestSnowflakeSqlApiHook:
    @pytest.mark.parametrize(
        "sql,statement_count,expected_response, expected_query_ids",
        [
            (SINGLE_STMT, 1, {"statementHandle": "uuid"}, ["uuid"]),
            (SQL_MULTIPLE_STMTS, 4, {"statementHandles": ["uuid", "uuid1"]}, ["uuid", "uuid1"]),
        ],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query(
        self,
        mock_get_header,
        mock_conn_param,
        sql,
        statement_count,
        expected_response,
        expected_query_ids,
        mock_requests,
    ):
        """Test execute_query method, run query by mocking post request method and return the query ids"""
        mock_requests.codes.ok = 200
        mock_requests.request.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.request.return_value).status_code = status_code_mock

        hook = SnowflakeSqlApiHook("mock_conn_id")
        query_ids = hook.execute_query(sql, statement_count)
        assert query_ids == expected_query_ids

    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query_multiple_times_give_fresh_query_ids_each_time(
        self, mock_get_header, mock_conn_param, mock_requests
    ):
        """Test execute_query method, run query by mocking post request method and return the query ids"""
        sql, statement_count, expected_response, expected_query_ids = (
            SQL_MULTIPLE_STMTS,
            4,
            {"statementHandles": ["uuid2", "uuid3"]},
            ["uuid2", "uuid3"],
        )

        mock_requests.codes.ok = 200
        mock_requests.request.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.request.return_value).status_code = status_code_mock

        hook = SnowflakeSqlApiHook("mock_conn_id")
        query_ids = hook.execute_query(sql, statement_count)
        assert query_ids == expected_query_ids

        sql, statement_count, expected_response, expected_query_ids = (
            SINGLE_STMT,
            1,
            {"statementHandle": "uuid"},
            ["uuid"],
        )
        mock_requests.request.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        query_ids = hook.execute_query(sql, statement_count)
        assert query_ids == expected_query_ids

    @pytest.mark.parametrize(
        "sql,statement_count,expected_response, expected_query_ids",
        [(SINGLE_STMT, 1, {"statementHandle": "uuid"}, ["uuid"])],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query_exception_without_statement_handle(
        self,
        mock_get_header,
        mock_conn_param,
        sql,
        statement_count,
        expected_response,
        expected_query_ids,
        mock_requests,
    ):
        """
        Test execute_query method by mocking the exception response and raise airflow exception
        without statementHandle in the response
        """
        side_effect = create_post_side_effect()
        mock_requests.request.side_effect = side_effect
        hook = SnowflakeSqlApiHook("mock_conn_id")

        with pytest.raises(AirflowException) as exception_info:
            hook.execute_query(sql, statement_count)
        assert exception_info

    @pytest.mark.parametrize(
        "sql,statement_count,bindings",
        [
            (SQL_MULTIPLE_STMTS, 4, {"1": {"type": "FIXED", "value": "123"}}),
        ],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query_bindings_warning(
        self,
        mock_get_headers,
        mock_conn_params,
        sql,
        statement_count,
        bindings,
        mock_requests,
    ):
        """Test execute_query method logs warning when bindings are provided for multi-statement queries"""
        mock_conn_params.return_value = CONN_PARAMS
        mock_get_headers.return_value = HEADERS
        mock_requests.request.return_value = create_successful_response_mock(
            {"statementHandles": ["uuid", "uuid1"]}
        )

        hook = SnowflakeSqlApiHook(snowflake_conn_id="mock_conn_id")
        with mock.patch.object(hook.log, "warning") as mock_log_warning:
            hook.execute_query(sql, statement_count, bindings=bindings)
            mock_log_warning.assert_called_once_with(
                "Bindings are not supported for multi-statement queries. Bindings will be ignored."
            )

    @pytest.mark.parametrize(
        "query_ids",
        [
            (["uuid", "uuid1"]),
        ],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    def test_check_query_output(self, mock_geturl_header_params, query_ids, mock_requests):
        """Test check_query_output by passing query ids as params and mock get_request_url_header_params"""
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "/test/airflow/"
        mock_requests.request.return_value.json.return_value = GET_RESPONSE
        hook = SnowflakeSqlApiHook("mock_conn_id")
        with mock.patch.object(hook.log, "info") as mock_log_info:
            hook.check_query_output(query_ids)
        mock_log_info.assert_called_with(GET_RESPONSE)

    @pytest.mark.parametrize("query_ids", [["uuid", "uuid1"]])
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    def test_check_query_output_exception(
        self,
        mock_geturl_header_params,
        query_ids,
        mock_requests,
    ):
        """
        Test check_query_output by passing query ids as params and mock get_request_url_header_params
        to raise airflow exception and mock with http error
        """
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "https://test/airflow/"
        custom_retry_args = {
            "stop": tenacity.stop_after_attempt(2),  # Only 2 attempts instead of default 5
        }
        hook = SnowflakeSqlApiHook("mock_conn_id", api_retry_args=custom_retry_args)
        mock_requests.request.side_effect = [create_post_side_effect(status_code=500)] * 3
        with pytest.raises(requests.exceptions.HTTPError):
            hook.check_query_output(query_ids)

    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_get_request_url_header_params(self, mock_get_header, mock_conn_param):
        """Test get_request_url_header_params by mocking _get_conn_params and get_headers"""
        mock_conn_param.return_value = CONN_PARAMS
        mock_get_header.return_value = HEADERS
        hook = SnowflakeSqlApiHook("mock_conn_id")
        header, params, url = hook.get_request_url_header_params("uuid")
        assert header == HEADERS
        assert url == "https://airflow.af_region.snowflakecomputing.com/api/v2/statements/uuid"

    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_private_key")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.utils.sql_api_generate_jwt.JWTGenerator.get_token")
    def test_get_headers_should_support_private_key(self, mock_get_token, mock_conn_param, mock_private_key):
        """Test get_headers method by mocking get_private_key and _get_conn_params method"""
        mock_get_token.return_value = "newT0k3n"
        mock_conn_param.return_value = CONN_PARAMS
        hook = SnowflakeSqlApiHook(snowflake_conn_id="mock_conn_id")
        result = hook.get_headers()
        assert result == HEADERS

    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_oauth_token")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    def test_get_headers_should_support_oauth(self, mock_conn_param, mock_oauth_token):
        """Test get_headers method by mocking get_oauth_token and _get_conn_params method"""
        mock_conn_param.return_value = CONN_PARAMS_OAUTH
        mock_oauth_token.return_value = "newT0k3n"
        hook = SnowflakeSqlApiHook(snowflake_conn_id="mock_conn_id")
        result = hook.get_headers()
        assert result == HEADERS_OAUTH

    @mock.patch("airflow.providers.snowflake.hooks.snowflake.HTTPBasicAuth")
    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    def test_get_oauth_token(self, mock_conn_param, requests_post, mock_auth):
        """Test get_oauth_token method makes the right http request"""
        basic_auth = {"Authorization": "Basic usernamepassword"}
        mock_conn_param.return_value = CONN_PARAMS_OAUTH
        requests_post.return_value.status_code = 200
        mock_auth.return_value = basic_auth
        hook = SnowflakeSqlApiHook(snowflake_conn_id="mock_conn_id")
        with pytest.warns(expected_warning=AirflowProviderDeprecationWarning):
            hook.get_oauth_token(CONN_PARAMS_OAUTH)
        requests_post.assert_called_once_with(
            f"https://{CONN_PARAMS_OAUTH['account']}.snowflakecomputing.com/oauth/token-request",
            data={
                "grant_type": "refresh_token",
                "refresh_token": CONN_PARAMS_OAUTH["refresh_token"],
                "redirect_uri": "https://localhost.com",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=basic_auth,
        )

    @pytest.fixture
    def unencrypted_temporary_private_key(self, tmp_path: Path) -> Path:
        """Encrypt the pem file from the path"""
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        test_key_file = tmp_path / "test_key.pem"
        test_key_file.write_bytes(private_key)
        return test_key_file

    @pytest.fixture
    def base64_encoded_unencrypted_private_key(self, unencrypted_temporary_private_key: Path) -> str:
        return base64.b64encode(unencrypted_temporary_private_key.read_bytes()).decode("utf-8")

    @pytest.fixture
    def encrypted_temporary_private_key(self, tmp_path: Path) -> Path:
        """Encrypt private key from the temp path"""
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(_PASSWORD.encode()),
        )
        test_key_file: Path = tmp_path / "test_key.p8"
        test_key_file.write_bytes(private_key)
        return test_key_file

    @pytest.fixture
    def base64_encoded_encrypted_private_key(self, encrypted_temporary_private_key: Path) -> str:
        return base64.b64encode(encrypted_temporary_private_key.read_bytes()).decode("utf-8")

    def test_get_private_key_should_support_private_auth_in_connection(
        self, base64_encoded_encrypted_private_key: str
    ):
        """Test get_private_key function with private_key_content in connection"""
        connection_kwargs: Any = {
            **BASE_CONNECTION_KWARGS,
            "password": _PASSWORD,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_content": base64_encoded_encrypted_private_key,
            },
        }
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
        ):
            hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
            hook.get_private_key()
            assert hook.private_key is not None

    def test_get_private_key_raise_exception(
        self, encrypted_temporary_private_key: Path, base64_encoded_encrypted_private_key: str
    ):
        """
        Test get_private_key function with private_key_content and private_key_file in connection
        and raise airflow exception
        """
        connection_kwargs: Any = {
            **BASE_CONNECTION_KWARGS,
            "password": _PASSWORD,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_content": base64_encoded_encrypted_private_key,
                "private_key_file": str(encrypted_temporary_private_key),
            },
        }
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
        with (
            unittest.mock.patch.dict(
                "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
            ),
            pytest.raises(
                AirflowException,
                match="The private_key_file and private_key_content extra fields are mutually "
                "exclusive. Please remove one.",
            ),
        ):
            hook.get_private_key()

    def test_get_private_key_should_support_private_auth_with_encrypted_key(
        self, encrypted_temporary_private_key
    ):
        """Test get_private_key method by supporting for private auth encrypted_key"""
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "password": _PASSWORD,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_file": str(encrypted_temporary_private_key),
            },
        }
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
        ):
            hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
            hook.get_private_key()
            assert hook.private_key is not None

    def test_get_private_key_should_support_private_auth_with_unencrypted_key(
        self,
        unencrypted_temporary_private_key,
    ):
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "password": None,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_file": str(unencrypted_temporary_private_key),
            },
        }
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
        ):
            hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
            hook.get_private_key()
            assert hook.private_key is not None
        connection_kwargs["password"] = ""
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
        ):
            hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
            hook.get_private_key()
            assert hook.private_key is not None
        connection_kwargs["password"] = _PASSWORD
        with (
            unittest.mock.patch.dict(
                "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
            ),
            pytest.raises(TypeError, match="Password was given but private key is not encrypted."),
        ):
            SnowflakeSqlApiHook(snowflake_conn_id="test_conn").get_private_key()

    @pytest.mark.parametrize(
        "status_code,response,expected_response",
        [
            (
                200,
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statementHandle": "uuid",
                },
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statement_handles": ["uuid"],
                },
            ),
            (
                200,
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statementHandles": ["uuid", "uuid1"],
                },
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statement_handles": ["uuid", "uuid1"],
                },
            ),
            (202, {}, {"status": "running", "message": "Query statements are still running"}),
            (422, {"status": "error", "message": "test"}, {"status": "error", "message": "test"}),
            (404, {"status": "error", "message": "test"}, {"status": "error", "message": "test"}),
        ],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    def test_get_sql_api_query_status(
        self, mock_geturl_header_params, status_code, response, expected_response, mock_requests
    ):
        """Test get_sql_api_query_status function by mocking the status, response and expected
        response"""
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "/test/airflow/"

        class MockResponse:
            def __init__(self, status_code, data):
                self.status_code = status_code
                self.data = data

            def json(self):
                return self.data

            def raise_for_status(self):
                return

        mock_requests.request.return_value = MockResponse(status_code, response)
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
        assert hook.get_sql_api_query_status("uuid") == expected_response

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code,response,expected_response",
        [
            (
                200,
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statementHandle": "uuid",
                },
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statement_handles": ["uuid"],
                },
            ),
            (
                200,
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statementHandles": ["uuid", "uuid1"],
                },
                {
                    "status": "success",
                    "message": "Statement executed successfully.",
                    "statement_handles": ["uuid", "uuid1"],
                },
            ),
            (202, {}, {"status": "running", "message": "Query statements are still running"}),
            (422, {"status": "error", "message": "test"}, {"status": "error", "message": "test"}),
            (404, {"status": "error", "message": "test"}, {"status": "error", "message": "test"}),
        ],
    )
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    async def test_get_sql_api_query_status_async(
        self, mock_geturl_header_params, status_code, response, expected_response, mock_async_request
    ):
        """Test Async get_sql_api_query_status_async function by mocking the status,
        response and expected response"""
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "/test/airflow/"
        mock_async_request.__aenter__.return_value.status = status_code
        mock_async_request.__aenter__.return_value.json = AsyncMock(return_value=response)
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
        response = await hook.get_sql_api_query_status_async("uuid")
        assert response == expected_response

    @pytest.mark.parametrize("hook_params", [(HOOK_PARAMS), ({})])
    def test_hook_parameter_propagation(self, hook_params):
        """
        This tests the proper propagation of unpacked hook params into the SnowflakeSqlApiHook object.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn", **hook_params)
        assert hook.database == hook_params.get("database", None)
        assert hook.schema == hook_params.get("schema", None)
        assert hook.warehouse == hook_params.get("warehouse", None)
        assert hook.role == hook_params.get("role", None)

    @pytest.mark.parametrize(
        "test_hook_params, sql, statement_count, expected_payload, expected_response",
        [
            (
                {},
                SINGLE_STMT,
                1,
                {
                    "statement": SINGLE_STMT,
                    "resultSetMetaData": {"format": "json"},
                    "database": CONN_PARAMS["database"],
                    "schema": CONN_PARAMS["schema"],
                    "warehouse": CONN_PARAMS["warehouse"],
                    "role": CONN_PARAMS["role"],
                    "bindings": {},
                    "parameters": {
                        "MULTI_STATEMENT_COUNT": 1,
                        "query_tag": "",
                    },
                },
                {"statementHandle": "uuid"},
            ),
            (
                {},
                SQL_MULTIPLE_STMTS,
                4,
                {
                    "statement": SQL_MULTIPLE_STMTS,
                    "resultSetMetaData": {"format": "json"},
                    "database": CONN_PARAMS["database"],
                    "schema": CONN_PARAMS["schema"],
                    "warehouse": CONN_PARAMS["warehouse"],
                    "role": CONN_PARAMS["role"],
                    "bindings": {},
                    "parameters": {
                        "MULTI_STATEMENT_COUNT": 4,
                        "query_tag": "",
                    },
                },
                {"statementHandles": ["uuid", "uuid1"]},
            ),
            (
                HOOK_PARAMS,
                SINGLE_STMT,
                1,
                {
                    "statement": SINGLE_STMT,
                    "resultSetMetaData": {"format": "json"},
                    "database": HOOK_PARAMS["database"],
                    "schema": HOOK_PARAMS["schema"],
                    "warehouse": HOOK_PARAMS["warehouse"],
                    "role": HOOK_PARAMS["role"],
                    "bindings": {},
                    "parameters": {
                        "MULTI_STATEMENT_COUNT": 1,
                        "query_tag": "",
                    },
                },
                {"statementHandle": "uuid"},
            ),
            (
                HOOK_PARAMS,
                SQL_MULTIPLE_STMTS,
                4,
                {
                    "statement": SQL_MULTIPLE_STMTS,
                    "resultSetMetaData": {"format": "json"},
                    "database": HOOK_PARAMS["database"],
                    "schema": HOOK_PARAMS["schema"],
                    "warehouse": HOOK_PARAMS["warehouse"],
                    "role": HOOK_PARAMS["role"],
                    "bindings": {},
                    "parameters": {
                        "MULTI_STATEMENT_COUNT": 4,
                        "query_tag": "",
                    },
                },
                {"statementHandles": ["uuid", "uuid1"]},
            ),
        ],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_proper_parametrization_of_execute_query_api_request(
        self,
        mock_get_headers,
        mock_conn_param,
        mock_uuid,
        test_hook_params,
        sql,
        statement_count,
        expected_payload,
        expected_response,
        mock_requests,
    ):
        """
        This tests if the query execution ordered by POST request to Snowflake API
        is sent with proper context parameters (database, schema, warehouse, role)
        """
        mock_uuid.return_value = "uuid"
        params = {"requestId": "uuid", "async": True, "pageSize": 10}
        mock_conn_param.return_value = CONN_PARAMS
        mock_get_headers.return_value = HEADERS
        mock_requests.codes.ok = 200
        mock_requests.request.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        hook = SnowflakeSqlApiHook("mock_conn_id", **test_hook_params)
        url = f"{hook.account_identifier}.snowflakecomputing.com/api/v2/statements"

        hook.execute_query(sql, statement_count)

        mock_requests.request.assert_called_once_with(
            method="post", url=url, headers=HEADERS, json=expected_payload, params=params
        )

    @pytest.mark.parametrize(
        "status_code,should_retry",
        [
            (429, True),  # Too Many Requests - should retry
            (503, True),  # Service Unavailable - should retry
            (504, True),  # Gateway Timeout - should retry
            (500, False),  # Internal Server Error - should not retry
            (400, False),  # Bad Request - should not retry
            (401, False),  # Unauthorized - should not retry
            (404, False),  # Not Found - should not retry
        ],
    )
    def test_make_api_call_with_retries_http_errors(self, status_code, should_retry, mock_requests):
        """
        Test that _make_api_call_with_retries method only retries on specific HTTP status codes.
        Should retry on 429, 503, 504 but not on other error codes.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # Mock failed response
        failed_response = mock.MagicMock()
        failed_response.status_code = status_code
        failed_response.json.return_value = {"error": "test error"}
        failed_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=failed_response)

        # Mock successful response for retries
        success_response = mock.MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"statementHandle": "uuid"}
        success_response.raise_for_status.return_value = None

        if should_retry:
            # For retryable errors, first call fails, second succeeds
            mock_requests.request.side_effect = [failed_response, success_response]
            status_code, resp_json = hook._make_api_call_with_retries(
                method="GET",
                url=API_URL,
                headers=HEADERS,
            )
            assert status_code == 200
            assert resp_json == {"statementHandle": "uuid"}
            assert mock_requests.request.call_count == 2
            mock_requests.request.assert_has_calls(
                [
                    call(
                        method="get",
                        json=None,
                        url=API_URL,
                        params=None,
                        headers=HEADERS,
                    ),
                    call(
                        method="get",
                        json=None,
                        url=API_URL,
                        params=None,
                        headers=HEADERS,
                    ),
                ]
            )
        else:
            # For non-retryable errors, should fail immediately
            mock_requests.request.side_effect = [failed_response]
            with pytest.raises(requests.exceptions.HTTPError):
                hook._make_api_call_with_retries(
                    method="GET",
                    url=API_URL,
                    headers=HEADERS,
                )
            assert mock_requests.request.call_count == 1
            mock_requests.request.assert_called_with(
                method="get",
                json=None,
                url=API_URL,
                params=None,
                headers=HEADERS,
            )

    def test_make_api_call_with_retries_connection_errors(self, mock_requests):
        """
        Test that _make_api_call_with_retries method retries on connection errors.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # Mock connection error then success
        success_response = mock.MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"statementHandle": "uuid"}
        success_response.raise_for_status.return_value = None

        mock_requests.request.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            success_response,
        ]

        status_code, resp_json = hook._make_api_call_with_retries(
            "POST", API_URL, HEADERS, json={"test": "data"}
        )

        assert status_code == 200
        mock_requests.request.assert_called_with(
            method="post",
            url=API_URL,
            params=None,
            headers=HEADERS,
            json={"test": "data"},
        )
        assert resp_json == {"statementHandle": "uuid"}
        assert mock_requests.request.call_count == 2

    def test_make_api_call_with_retries_timeout_errors(self, mock_requests):
        """
        Test that _make_api_call_with_retries method retries on timeout errors.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # Mock timeout error then success
        success_response = mock.MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"statementHandle": "uuid"}
        success_response.raise_for_status.return_value = None

        mock_requests.request.side_effect = [
            requests.exceptions.Timeout("Request timed out"),
            success_response,
        ]

        status_code, resp_json = hook._make_api_call_with_retries("GET", API_URL, HEADERS)

        assert status_code == 200
        assert resp_json == {"statementHandle": "uuid"}
        assert mock_requests.request.call_count == 2

    def test_make_api_call_with_retries_max_attempts(self, mock_requests):
        """
        Test that _make_api_call_with_retries method respects max retry attempts.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # Mock response that always fails with retryable error
        failed_response = mock.MagicMock()
        failed_response.status_code = 429
        failed_response.json.return_value = {"error": "rate limited"}
        failed_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=failed_response)

        mock_requests.request.side_effect = [failed_response] * 10  # More failures than max retries

        with pytest.raises(requests.exceptions.HTTPError):
            hook._make_api_call_with_retries("GET", API_URL, HEADERS)

        # Should attempt 5 times (initial + 4 retries) based on default retry config
        assert mock_requests.request.call_count == 5

    def test_make_api_call_with_retries_success_no_retry(self, mock_requests):
        """
        Test that _make_api_call_with_retries method doesn't retry on successful requests.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # Mock successful response
        success_response = mock.MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"statementHandle": "uuid"}
        success_response.raise_for_status.return_value = None

        mock_requests.request.return_value = success_response

        status_code, resp_json = hook._make_api_call_with_retries(
            "POST", API_URL, HEADERS, json={"test": "data"}
        )

        assert status_code == 200
        assert resp_json == {"statementHandle": "uuid"}
        assert mock_requests.request.call_count == 1

    def test_make_api_call_with_retries_unsupported_method(self):
        """
        Test that _make_api_call_with_retries method raises ValueError for unsupported HTTP methods.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        with pytest.raises(ValueError, match="Unsupported HTTP method: PUT"):
            hook._make_api_call_with_retries("PUT", API_URL, HEADERS)

    def test_make_api_call_with_retries_custom_retry_config(self, mock_requests):
        """
        Test that _make_api_call_with_retries method respects custom retry configuration.
        """

        # Create hook with custom retry config
        custom_retry_args = {
            "stop": tenacity.stop_after_attempt(2),  # Only 2 attempts instead of default 5
        }
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn", api_retry_args=custom_retry_args)

        # Mock response that always fails with retryable error
        failed_response = mock.MagicMock()
        failed_response.status_code = 503
        failed_response.json.return_value = {"error": "service unavailable"}
        failed_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=failed_response)

        mock_requests.request.side_effect = [failed_response] * 3

        with pytest.raises(requests.exceptions.HTTPError):
            hook._make_api_call_with_retries("GET", API_URL, HEADERS)

        # Should attempt only 2 times due to custom config
        assert mock_requests.request.call_count == 2

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_success(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async returns response on success.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        mock_response = create_async_request_client_response_success()
        mock_async_request.__aenter__.return_value = mock_response
        status_code, resp_json = await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)
        assert status_code == 200
        assert resp_json == GET_RESPONSE
        assert mock_async_request.__aenter__.call_count == 1

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_retryable_http_error(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async retries on retryable HTTP errors (429, 503, 504).
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # First response: 429, then 200
        mock_response_429 = create_async_request_client_response_error()
        mock_response_200 = create_async_request_client_response_success()
        # Side effect for request context manager
        mock_async_request.__aenter__.side_effect = [
            mock_response_429,
            mock_response_200,
        ]

        status_code, resp_json = await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)
        assert status_code == 200
        assert resp_json == GET_RESPONSE
        assert mock_async_request.__aenter__.call_count == 2

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_non_retryable_http_error(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async does not retry on non-retryable HTTP errors.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        mock_response_400 = create_async_request_client_response_error(status_code=400)

        mock_async_request.__aenter__.return_value = mock_response_400

        with pytest.raises(aiohttp.ClientResponseError):
            await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)
        assert mock_async_request.__aenter__.call_count == 1

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_connection_error(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async retries on connection errors.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        # First: connection error, then: success
        failed_conn = create_async_connection_error()

        mock_request_200 = create_async_request_client_response_success()

        # Side effect for request context manager
        mock_async_request.__aenter__.side_effect = [
            failed_conn,
            mock_request_200,
        ]

        status_code, resp_json = await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)
        assert status_code == 200
        assert resp_json == GET_RESPONSE
        assert mock_async_request.__aenter__.call_count == 2

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_max_attempts(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async respects max retry attempts.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
        mock_request_429 = create_async_request_client_response_error(status_code=429)

        # Always returns 429
        mock_async_request.__aenter__.side_effect = [mock_request_429] * 5

        with pytest.raises(aiohttp.ClientResponseError):
            await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)
        # Should attempt 5 times (default max retries)
        assert mock_async_request.__aenter__.call_count == 5

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_unsupported_method(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async raises ValueError for unsupported HTTP methods.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        with pytest.raises(ValueError, match="Unsupported HTTP method: PATCH"):
            await hook._make_api_call_with_retries_async("PATCH", API_URL, HEADERS)
        # No HTTP call should be made
        assert mock_async_request.__aenter__.call_count == 0

    @pytest.mark.asyncio
    async def test_make_api_call_with_retries_async_json_decode_error_prevention(self, mock_async_request):
        """
        Test that _make_api_call_with_retries_async calls raise_for_status() before response.json()
        to prevent JSONDecodeError when response is not valid JSON.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        failed_response = mock.MagicMock()
        failed_response.status = 500
        failed_response.json = AsyncMock(side_effect=ValueError("Invalid JSON"))
        failed_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=mock.MagicMock(),
            history=mock.MagicMock(),
            status=500,
            message="Internal Server Error",
        )

        mock_async_request.__aenter__.return_value = failed_response

        with pytest.raises(aiohttp.ClientResponseError):
            await hook._make_api_call_with_retries_async("GET", API_URL, HEADERS)

        failed_response.raise_for_status.assert_called_once()
        failed_response.json.assert_not_called()

    def test_make_api_call_with_retries_json_decode_error_prevention(self, mock_requests):
        """
        Test that _make_api_call_with_retries calls raise_for_status() before response.json()
        to prevent JSONDecodeError when response is not valid JSON.
        """
        hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")

        failed_response = mock.MagicMock()
        failed_response.status_code = 500
        failed_response.json.side_effect = requests.exceptions.JSONDecodeError("Invalid JSON", "", 0)
        failed_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=failed_response)

        mock_requests.request.return_value = failed_response

        with pytest.raises(requests.exceptions.HTTPError):
            hook._make_api_call_with_retries("GET", API_URL, HEADERS)

        failed_response.raise_for_status.assert_called_once()
        failed_response.json.assert_not_called()
