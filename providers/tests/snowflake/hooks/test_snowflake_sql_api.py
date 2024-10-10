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
import uuid
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import AsyncMock, PropertyMock

import pytest
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from responses import RequestsMock

from airflow.exceptions import AirflowException
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


class TestSnowflakeSqlApiHook:
    @pytest.mark.parametrize(
        "sql,statement_count,expected_response, expected_query_ids",
        [
            (SINGLE_STMT, 1, {"statementHandle": "uuid"}, ["uuid"]),
            (SQL_MULTIPLE_STMTS, 4, {"statementHandles": ["uuid", "uuid1"]}, ["uuid", "uuid1"]),
        ],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query(
        self,
        mock_get_header,
        mock_conn_param,
        mock_requests,
        sql,
        statement_count,
        expected_response,
        expected_query_ids,
    ):
        """Test execute_query method, run query by mocking post request method and return the query ids"""
        mock_requests.codes.ok = 200
        mock_requests.post.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        hook = SnowflakeSqlApiHook("mock_conn_id")
        query_ids = hook.execute_query(sql, statement_count)
        assert query_ids == expected_query_ids

    @pytest.mark.parametrize(
        "sql,statement_count,expected_response, expected_query_ids",
        [(SINGLE_STMT, 1, {"statementHandle": "uuid"}, ["uuid"])],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query_exception_without_statement_handle(
        self,
        mock_get_header,
        mock_conn_param,
        mock_requests,
        sql,
        statement_count,
        expected_response,
        expected_query_ids,
    ):
        """
        Test execute_query method by mocking the exception response and raise airflow exception
        without statementHandle in the response
        """
        side_effect = create_post_side_effect()
        mock_requests.post.side_effect = side_effect
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
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_execute_query_bindings_warning(
        self,
        mock_get_headers,
        mock_conn_params,
        mock_requests,
        sql,
        statement_count,
        bindings,
    ):
        """Test execute_query method logs warning when bindings are provided for multi-statement queries"""
        mock_conn_params.return_value = CONN_PARAMS
        mock_get_headers.return_value = HEADERS
        mock_requests.post.return_value = create_successful_response_mock(
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
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    def test_check_query_output(self, mock_geturl_header_params, mock_requests, query_ids):
        """Test check_query_output by passing query ids as params and mock get_request_url_header_params"""
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "/test/airflow/"
        mock_requests.get.return_value.json.return_value = GET_RESPONSE
        hook = SnowflakeSqlApiHook("mock_conn_id")
        with mock.patch.object(hook.log, "info") as mock_log_info:
            hook.check_query_output(query_ids)
        mock_log_info.assert_called_with(GET_RESPONSE)

    @pytest.mark.parametrize("query_ids", [(["uuid", "uuid1"])])
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook."
        "get_request_url_header_params"
    )
    def test_check_query_output_exception(self, mock_geturl_header_params, query_ids):
        """
        Test check_query_output by passing query ids as params and mock get_request_url_header_params
        to raise airflow exception and mock with http error
        """
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "https://test/airflow/"
        hook = SnowflakeSqlApiHook("mock_conn_id")
        with mock.patch.object(hook.log, "error"), RequestsMock() as requests_mock:
            requests_mock.get(url="https://test/airflow/", json={"foo": "bar"}, status=500)
            with pytest.raises(AirflowException, match='Response: {"foo": "bar"}, Status Code: 500'):
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

    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.HTTPBasicAuth")
    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    def test_get_oauth_token(self, mock_conn_param, requests_post, mock_auth):
        """Test get_oauth_token method makes the right http request"""
        BASIC_AUTH = {"Authorization": "Basic usernamepassword"}
        mock_conn_param.return_value = CONN_PARAMS_OAUTH
        requests_post.return_value.status_code = 200
        mock_auth.return_value = BASIC_AUTH
        hook = SnowflakeSqlApiHook(snowflake_conn_id="mock_conn_id")
        hook.get_oauth_token()
        requests_post.assert_called_once_with(
            f"https://{CONN_PARAMS_OAUTH['account']}.{CONN_PARAMS_OAUTH['region']}.snowflakecomputing.com/oauth/token-request",
            data={
                "grant_type": "refresh_token",
                "refresh_token": CONN_PARAMS_OAUTH["refresh_token"],
                "redirect_uri": "https://localhost.com",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=BASIC_AUTH,
        )

    @pytest.fixture
    def non_encrypted_temporary_private_key(self, tmp_path: Path) -> Path:
        """Encrypt the pem file from the path"""
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        test_key_file = tmp_path / "test_key.pem"
        test_key_file.write_bytes(private_key)
        return test_key_file

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

    def test_get_private_key_should_support_private_auth_in_connection(
        self, encrypted_temporary_private_key: Path
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
                "private_key_content": str(encrypted_temporary_private_key.read_text()),
            },
        }
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()
        ):
            hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
            hook.get_private_key()
            assert hook.private_key is not None

    def test_get_private_key_raise_exception(self, encrypted_temporary_private_key: Path):
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
                "private_key_content": str(encrypted_temporary_private_key.read_text()),
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
        non_encrypted_temporary_private_key,
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
                "private_key_file": str(non_encrypted_temporary_private_key),
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
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    def test_get_sql_api_query_status(
        self, mock_requests, mock_geturl_header_params, status_code, response, expected_response
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

        mock_requests.get.return_value = MockResponse(status_code, response)
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
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.aiohttp.ClientSession.get")
    async def test_get_sql_api_query_status_async(
        self, mock_get, mock_geturl_header_params, status_code, response, expected_response
    ):
        """Test Async get_sql_api_query_status_async function by mocking the status,
        response and expected response"""
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        mock_geturl_header_params.return_value = HEADERS, params, "/test/airflow/"
        mock_get.return_value.__aenter__.return_value.status = status_code
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=response)
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
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.requests")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook._get_conn_params",
        new_callable=PropertyMock,
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_headers")
    def test_proper_parametrization_of_execute_query_api_request(
        self,
        mock_get_headers,
        mock_conn_param,
        mock_requests,
        mock_uuid,
        test_hook_params,
        sql,
        statement_count,
        expected_payload,
        expected_response,
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
        mock_requests.post.side_effect = [
            create_successful_response_mock(expected_response),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        hook = SnowflakeSqlApiHook("mock_conn_id", **test_hook_params)
        url = f"{hook.account_identifier}.snowflakecomputing.com/api/v2/statements"

        hook.execute_query(sql, statement_count)

        mock_requests.post.assert_called_once_with(url, headers=HEADERS, json=expected_payload, params=params)
