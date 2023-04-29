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

import json
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import multidict
import pytest
from aiohttp import ClientResponseError, RequestInfo
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyAsyncHook, LivyHook
from airflow.utils import db
from tests.test_utils.db import clear_db_connections

LIVY_CONN_ID = LivyHook.default_conn_name
DEFAULT_CONN_ID = LivyHook.default_conn_name
DEFAULT_HOST = "livy"
DEFAULT_SCHEMA = "http"
DEFAULT_PORT = 8998
MATCH_URL = f"//{DEFAULT_HOST}:{DEFAULT_PORT}"

BATCH_ID = 100
SAMPLE_GET_RESPONSE = {"id": BATCH_ID, "state": BatchState.SUCCESS.value}
VALID_SESSION_ID_TEST_CASES = [
    pytest.param(BATCH_ID, id="integer"),
    pytest.param(str(BATCH_ID), id="integer as string"),
]
INVALID_SESSION_ID_TEST_CASES = [
    pytest.param(None, id="none"),
    pytest.param("forty two", id="invalid string"),
    pytest.param({"a": "b"}, id="dictionary"),
]


class TestLivyHook:
    @classmethod
    def setup_class(cls):
        clear_db_connections(add_default_connections_back=False)
        db.merge_conn(
            Connection(
                conn_id=DEFAULT_CONN_ID,
                conn_type="http",
                host=DEFAULT_HOST,
                schema=DEFAULT_SCHEMA,
                port=DEFAULT_PORT,
            )
        )
        db.merge_conn(Connection(conn_id="default_port", conn_type="http", host="http://host"))
        db.merge_conn(Connection(conn_id="default_protocol", conn_type="http", host="host"))
        db.merge_conn(Connection(conn_id="port_set", host="host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="schema_set", host="host", conn_type="http", schema="https"))
        db.merge_conn(
            Connection(conn_id="dont_override_schema", conn_type="http", host="http://host", schema="https")
        )
        db.merge_conn(Connection(conn_id="missing_host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="invalid_uri", uri="http://invalid_uri:4321"))
        db.merge_conn(
            Connection(
                conn_id="with_credentials", login="login", password="secret", conn_type="http", host="host"
            )
        )

    @classmethod
    def teardown_class(cls):
        clear_db_connections(add_default_connections_back=True)

    @pytest.mark.parametrize(
        "conn_id, expected",
        [
            pytest.param("default_port", "http://host", id="default-port"),
            pytest.param("default_protocol", "http://host", id="default-protocol"),
            pytest.param("port_set", "http://host:1234", id="with-defined-port"),
            pytest.param("schema_set", "https://host", id="with-defined-schema"),
            pytest.param("dont_override_schema", "http://host", id="ignore-defined-schema"),
        ],
    )
    def test_build_get_hook(self, conn_id, expected):
        hook = LivyHook(livy_conn_id=conn_id)
        hook.get_conn()
        assert hook.base_url == expected

    @pytest.mark.skip("Inherited HttpHook does not handle missing hostname")
    def test_missing_host(self):
        with pytest.raises(AirflowException):
            LivyHook(livy_conn_id="missing_host").get_conn()

    def test_build_body_minimal_request(self):
        assert LivyHook.build_post_batch_body(file="appname") == {"file": "appname"}

    def test_build_body_complex_request(self):
        body = LivyHook.build_post_batch_body(
            file="appname",
            class_name="org.example.livy",
            proxy_user="proxyUser",
            args=["a", "1"],
            jars=["jar1", "jar2"],
            files=["file1", "file2"],
            py_files=["py1", "py2"],
            archives=["arch1", "arch2"],
            queue="queue",
            name="name",
            conf={"a": "b"},
            driver_cores=2,
            driver_memory="1M",
            executor_memory="1m",
            executor_cores="1",
            num_executors="10",
        )

        assert body == {
            "file": "appname",
            "className": "org.example.livy",
            "proxyUser": "proxyUser",
            "args": ["a", "1"],
            "jars": ["jar1", "jar2"],
            "files": ["file1", "file2"],
            "pyFiles": ["py1", "py2"],
            "archives": ["arch1", "arch2"],
            "queue": "queue",
            "name": "name",
            "conf": {"a": "b"},
            "driverCores": 2,
            "driverMemory": "1M",
            "executorMemory": "1m",
            "executorCores": "1",
            "numExecutors": "10",
        }

    def test_parameters_validation(self):
        with pytest.raises(ValueError):
            LivyHook.build_post_batch_body(file="appname", executor_memory="xxx")

        assert LivyHook.build_post_batch_body(file="appname", args=["a", 1, 0.1])["args"] == ["a", "1", "0.1"]

    @pytest.mark.parametrize(
        "size",
        [
            pytest.param("1m", id="lowercase-short"),
            pytest.param("1mb", id="lowercase-long"),
            pytest.param("1mb", id="uppercase-short"),
            pytest.param("1GB", id="uppercase-long"),
            pytest.param("1Gb", id="mix-case"),
            pytest.param(None, id="none"),
        ],
    )
    def test_validate_size_format(self, size):
        assert LivyHook._validate_size_format(size)

    @pytest.mark.parametrize(
        "size",
        [
            pytest.param("1Gb foo", id="fullmatch"),
            pytest.param("10", id="missing size"),
            pytest.param(1, id="integer"),
        ],
    )
    def test_validate_size_format_failed(self, size):
        with pytest.raises(ValueError, match=rf"Invalid java size format for string'{size}'"):
            assert LivyHook._validate_size_format(size)

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param([1, "string"], id="list"),
            pytest.param((1, "string"), id="tuple"),
            pytest.param([], id="empty list"),
        ],
    )
    def test_validate_list_of_stringables(self, value):
        assert LivyHook._validate_list_of_stringables(value)

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param({"a": "a"}, id="dict"),
            pytest.param([1, {}], id="invalid element"),
            pytest.param(None, id="none"),
            pytest.param(42, id="integer"),
            pytest.param("foo-bar", id="string"),
        ],
    )
    def test_validate_list_of_stringables_failed(self, value):
        with pytest.raises(ValueError, match="List of strings expected"):
            assert LivyHook._validate_list_of_stringables(value)

    @pytest.mark.parametrize(
        "config",
        [
            pytest.param({"k1": "v1", "k2": 0}, id="valid dictionary config"),
            pytest.param({}, id="empty dictionary"),
            pytest.param(None, id="none"),
        ],
    )
    def test_validate_extra_conf(self, config):
        LivyHook._validate_extra_conf(config)

    @pytest.mark.parametrize(
        "config",
        [
            pytest.param("k1=v1", id="string"),
            pytest.param([("k1", "v1"), ("k2", 0)], id="list of tuples"),
            pytest.param({"outer": {"inner": "val"}}, id="nested dictionary"),
            pytest.param({"has_val": "val", "no_val": None}, id="none values in dictionary"),
            pytest.param({"has_val": "val", "no_val": ""}, id="empty values in dictionary"),
        ],
    )
    def test_validate_extra_conf_failed(self, config):
        with pytest.raises(ValueError):
            LivyHook._validate_extra_conf(config)

    @patch("airflow.providers.apache.livy.hooks.livy.LivyHook.run_method")
    def test_post_batch_arguments(self, mock_request):

        mock_request.return_value.status_code = 201
        mock_request.return_value.json.return_value = {
            "id": BATCH_ID,
            "state": BatchState.STARTING.value,
            "log": [],
        }

        resp = LivyHook().post_batch(file="sparkapp")

        mock_request.assert_called_once_with(
            method="POST", endpoint="/batches", data=json.dumps({"file": "sparkapp"}), headers={}
        )

        request_args = mock_request.call_args[1]
        assert "data" in request_args
        assert isinstance(request_args["data"], str)

        assert isinstance(resp, int)
        assert resp == BATCH_ID

    def test_post_batch_success(self, requests_mock):
        requests_mock.register_uri(
            "POST",
            "//livy:8998/batches",
            json={"id": BATCH_ID, "state": BatchState.STARTING.value, "log": []},
            status_code=201,
        )
        resp = LivyHook().post_batch(file="sparkapp")
        assert isinstance(resp, int)
        assert resp == BATCH_ID

    def test_post_batch_fail(self, requests_mock):
        requests_mock.register_uri("POST", f"{MATCH_URL}/batches", json={}, status_code=400, reason="ERROR")
        with pytest.raises(AirflowException):
            LivyHook().post_batch(file="sparkapp")

    def test_get_batch_success(self, requests_mock):
        requests_mock.register_uri(
            "GET", f"{MATCH_URL}/batches/{BATCH_ID}", json={"id": BATCH_ID}, status_code=200
        )
        resp = LivyHook().get_batch(BATCH_ID)
        assert isinstance(resp, dict)
        assert "id" in resp

    def test_get_batch_fail(self, requests_mock):
        requests_mock.register_uri(
            "GET",
            f"{MATCH_URL}/batches/{BATCH_ID}",
            json={"msg": "Unable to find batch"},
            status_code=404,
            reason="ERROR",
        )
        with pytest.raises(AirflowException):
            LivyHook().get_batch(BATCH_ID)

    def test_invalid_uri(self):
        with pytest.raises(RequestException):
            LivyHook(livy_conn_id="invalid_uri").post_batch(file="sparkapp")

    def test_get_batch_state_success(self, requests_mock):
        running = BatchState.RUNNING

        requests_mock.register_uri(
            "GET",
            f"{MATCH_URL}/batches/{BATCH_ID}/state",
            json={"id": BATCH_ID, "state": running.value},
            status_code=200,
        )

        state = LivyHook().get_batch_state(BATCH_ID)

        assert isinstance(state, BatchState)
        assert state == running

    def test_get_batch_state_fail(self, requests_mock):
        requests_mock.register_uri(
            "GET", f"{MATCH_URL}/batches/{BATCH_ID}/state", json={}, status_code=400, reason="ERROR"
        )
        with pytest.raises(AirflowException):
            LivyHook().get_batch_state(BATCH_ID)

    def test_get_batch_state_missing(self, requests_mock):
        requests_mock.register_uri("GET", f"{MATCH_URL}/batches/{BATCH_ID}/state", json={}, status_code=200)
        with pytest.raises(AirflowException):
            LivyHook().get_batch_state(BATCH_ID)

    def test_parse_post_response(self):
        res_id = LivyHook._parse_post_response({"id": BATCH_ID, "log": []})
        assert BATCH_ID == res_id

    def test_delete_batch_success(self, requests_mock):
        requests_mock.register_uri(
            "DELETE", f"{MATCH_URL}/batches/{BATCH_ID}", json={"msg": "deleted"}, status_code=200
        )
        assert LivyHook().delete_batch(BATCH_ID) == {"msg": "deleted"}

    def test_delete_batch_fail(self, requests_mock):
        requests_mock.register_uri(
            "DELETE", f"{MATCH_URL}/batches/{BATCH_ID}", json={}, status_code=400, reason="ERROR"
        )
        with pytest.raises(AirflowException):
            LivyHook().delete_batch(BATCH_ID)

    def test_missing_batch_id(self, requests_mock):
        requests_mock.register_uri("POST", f"{MATCH_URL}/batches", json={}, status_code=201)
        with pytest.raises(AirflowException):
            LivyHook().post_batch(file="sparkapp")

    @pytest.mark.parametrize("session_id", VALID_SESSION_ID_TEST_CASES)
    def test_get_batch_validation(self, session_id, requests_mock):
        requests_mock.register_uri(
            "GET", f"{MATCH_URL}/batches/{session_id}", json=SAMPLE_GET_RESPONSE, status_code=200
        )
        assert LivyHook().get_batch(session_id) == SAMPLE_GET_RESPONSE

    @pytest.mark.parametrize("session_id", INVALID_SESSION_ID_TEST_CASES)
    def test_get_batch_validation_failed(self, session_id):
        with pytest.raises(TypeError, match=r"\'session_id\' must be an integer"):
            LivyHook().get_batch(session_id)

    @pytest.mark.parametrize("session_id", VALID_SESSION_ID_TEST_CASES)
    def test_get_batch_state_validation(self, session_id, requests_mock):
        requests_mock.register_uri(
            "GET", f"{MATCH_URL}/batches/{session_id}/state", json=SAMPLE_GET_RESPONSE, status_code=200
        )
        assert LivyHook().get_batch_state(session_id) == BatchState.SUCCESS

    @pytest.mark.parametrize("session_id", INVALID_SESSION_ID_TEST_CASES)
    def test_get_batch_state_validation_failed(self, session_id):
        with pytest.raises(TypeError, match=r"\'session_id\' must be an integer"):
            LivyHook().get_batch_state(session_id)

    def test_delete_batch_validation(self, requests_mock):
        requests_mock.register_uri(
            "DELETE", f"{MATCH_URL}/batches/{BATCH_ID}", json={"id": BATCH_ID}, status_code=200
        )
        assert LivyHook().delete_batch(BATCH_ID) == {"id": BATCH_ID}

    @pytest.mark.parametrize("session_id", INVALID_SESSION_ID_TEST_CASES)
    def test_delete_batch_validation_failed(self, session_id):
        with pytest.raises(TypeError, match=r"\'session_id\' must be an integer"):
            LivyHook().delete_batch(session_id)

    @pytest.mark.parametrize("session_id", VALID_SESSION_ID_TEST_CASES)
    def test_check_session_id(self, session_id):
        LivyHook._validate_session_id(session_id)  # Should not raise any error

    @pytest.mark.parametrize("session_id", INVALID_SESSION_ID_TEST_CASES)
    def test_check_session_id_failed(self, session_id):
        with pytest.raises(TypeError, match=r"\'session_id\' must be an integer"):
            LivyHook._validate_session_id("asd")

    def test_extra_headers(self, requests_mock):
        requests_mock.register_uri(
            "POST",
            "//livy:8998/batches",
            json={"id": BATCH_ID, "state": BatchState.STARTING.value, "log": []},
            status_code=201,
            request_headers={"X-Requested-By": "user"},
        )

        hook = LivyHook(extra_headers={"X-Requested-By": "user"})
        hook.post_batch(file="sparkapp")

    def test_alternate_auth_type(self):
        auth_type = MagicMock()

        hook = LivyHook(livy_conn_id="with_credentials", auth_type=auth_type)

        auth_type.assert_not_called()

        hook.get_conn()

        auth_type.assert_called_once_with("login", "secret")


class TestLivyAsyncHook:
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.run_method")
    async def test_get_batch_state_running(self, mock_run_method):
        """Asserts the batch state as running with success response."""
        mock_run_method.return_value = {"status": "success", "response": {"state": BatchState.RUNNING}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state == {
            "batch_state": BatchState.RUNNING,
            "response": "successfully fetched the batch state.",
            "status": "success",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.run_method")
    async def test_get_batch_state_error(self, mock_run_method):
        """Asserts the batch state as error with error response."""
        mock_run_method.return_value = {"status": "error", "response": {"state": "error"}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.run_method")
    async def test_get_batch_state_error_without_state(self, mock_run_method):
        """Asserts the batch state as error without state returned as part of mock."""
        mock_run_method.return_value = {"status": "success", "response": {}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.run_method")
    async def test_get_batch_logs_success(self, mock_run_method):
        """Asserts the batch log as success."""
        mock_run_method.return_value = {"status": "success", "response": {}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_logs(BATCH_ID, 0, 100)
        assert state["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.run_method")
    async def test_get_batch_logs_error(self, mock_run_method):
        """Asserts the batch log for error."""
        mock_run_method.return_value = {"status": "error", "response": {}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_logs(BATCH_ID, 0, 100)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_logs")
    async def test_dump_batch_logs_success(self, mock_get_batch_logs):
        """Asserts the log dump log for success response."""
        mock_get_batch_logs.return_value = {
            "status": "success",
            "response": {"id": 1, "log": ["mock_log_1", "mock_log_2", "mock_log_3"]},
        }
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        log_dump = await hook.dump_batch_logs(BATCH_ID)
        assert log_dump == ["mock_log_1", "mock_log_2", "mock_log_3"]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_logs")
    async def test_dump_batch_logs_error(self, mock_get_batch_logs):
        """Asserts the log dump log for error response."""
        mock_get_batch_logs.return_value = {
            "status": "error",
            "response": {"id": 1, "log": ["mock_log_1", "mock_log_2"]},
        }
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        log_dump = await hook.dump_batch_logs(BATCH_ID)
        assert log_dump == {"id": 1, "log": ["mock_log_1", "mock_log_2"]}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook._do_api_call_async")
    async def test_run_method_success(self, mock_do_api_call_async):
        """Asserts the run_method for success response."""
        mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        response = await hook.run_method("localhost", "GET")
        assert response["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook._do_api_call_async")
    async def test_run_method_error(self, mock_do_api_call_async):
        """Asserts the run_method for error response."""
        mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        response = await hook.run_method("localhost", "abc")
        assert response == {"status": "error", "response": "Invalid http method abc"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_post_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for success response for POST method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.post = AsyncMock()
        mock_session.return_value.__aenter__.return_value.post.return_value.json = AsyncMock(
            return_value={"status": "success"}
        )
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "https://localhost"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_get_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for GET method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.get = AsyncMock()
        mock_session.return_value.__aenter__.return_value.get.return_value.json = AsyncMock(
            return_value={"status": "success"}
        )
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        hook.method = "GET"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_patch_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for PATCH method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch = AsyncMock()
        mock_session.return_value.__aenter__.return_value.patch.return_value.json = AsyncMock(
            return_value={"status": "success"}
        )
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        hook.method = "PATCH"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_unexpected_method_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for unexpected method error"""
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        hook.method = "abc"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(endpoint=GET_RUN_ENDPOINT, headers={})
        assert response == {"Response": "Unexpected HTTP Method: abc", "status": "error"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_with_type_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for TypeError."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"random value"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {}
        hook = LivyAsyncHook(livy_conn_id=LIVY_CONN_ID)
        hook.method = "PATCH"
        hook.retry_limit = 1
        hook.retry_delay = 1
        hook.http_conn_id = mock_get_connection
        with pytest.raises(TypeError):
            await hook._do_api_call_async(endpoint="", data="test", headers=mock_fun, extra_options=mock_fun)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_connection")
    async def test_do_api_call_async_with_client_response_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for Client Response Error."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"random value"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch = AsyncMock()
        mock_session.return_value.__aenter__.return_value.patch.return_value.json.side_effect = (
            ClientResponseError(
                request_info=RequestInfo(url="example.com", method="PATCH", headers=multidict.CIMultiDict()),
                status=500,
                history=[],
            )
        )
        GET_RUN_ENDPOINT = ""
        hook = LivyAsyncHook(livy_conn_id="livy_default")
        hook.method = "PATCH"
        hook.base_url = ""
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response["status"] == "error"

    def set_conn(self):
        db.merge_conn(
            Connection(conn_id=LIVY_CONN_ID, conn_type="http", host="host", schema="http", port=8998)
        )
        db.merge_conn(Connection(conn_id="default_port", conn_type="http", host="http://host"))
        db.merge_conn(Connection(conn_id="default_protocol", conn_type="http", host="host"))
        db.merge_conn(Connection(conn_id="port_set", host="host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="schema_set", host="host", conn_type="http", schema="zzz"))
        db.merge_conn(
            Connection(conn_id="dont_override_schema", conn_type="http", host="http://host", schema="zzz")
        )
        db.merge_conn(Connection(conn_id="missing_host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="invalid_uri", uri="http://invalid_uri:4321"))

    def test_build_get_hook(self):
        self.set_conn()
        connection_url_mapping = {
            # id, expected
            "default_port": "http://host",
            "default_protocol": "http://host",
            "port_set": "http://host:1234",
            "schema_set": "zzz://host",
            "dont_override_schema": "http://host",
        }

        for conn_id, expected in connection_url_mapping.items():
            hook = LivyAsyncHook(livy_conn_id=conn_id)
            response_conn: Connection = hook.get_connection(conn_id=conn_id)
            assert isinstance(response_conn, Connection)
            assert hook._generate_base_url(response_conn) == expected

    def test_build_body(self):
        # minimal request
        body = LivyAsyncHook.build_post_batch_body(file="appname")

        assert body == {"file": "appname"}

        # complex request
        body = LivyAsyncHook.build_post_batch_body(
            file="appname",
            class_name="org.example.livy",
            proxy_user="proxyUser",
            args=["a", "1"],
            jars=["jar1", "jar2"],
            files=["file1", "file2"],
            py_files=["py1", "py2"],
            archives=["arch1", "arch2"],
            queue="queue",
            name="name",
            conf={"a": "b"},
            driver_cores=2,
            driver_memory="1M",
            executor_memory="1m",
            executor_cores="1",
            num_executors="10",
        )

        assert body == {
            "file": "appname",
            "className": "org.example.livy",
            "proxyUser": "proxyUser",
            "args": ["a", "1"],
            "jars": ["jar1", "jar2"],
            "files": ["file1", "file2"],
            "pyFiles": ["py1", "py2"],
            "archives": ["arch1", "arch2"],
            "queue": "queue",
            "name": "name",
            "conf": {"a": "b"},
            "driverCores": 2,
            "driverMemory": "1M",
            "executorMemory": "1m",
            "executorCores": "1",
            "numExecutors": "10",
        }

    def test_parameters_validation(self):
        with pytest.raises(ValueError):
            LivyAsyncHook.build_post_batch_body(file="appname", executor_memory="xxx")

        assert LivyAsyncHook.build_post_batch_body(file="appname", args=["a", 1, 0.1])["args"] == [
            "a",
            "1",
            "0.1",
        ]

    def test_parse_post_response(self):
        res_id = LivyAsyncHook._parse_post_response({"id": BATCH_ID, "log": []})

        assert BATCH_ID == res_id

    @pytest.mark.parametrize("valid_size", ["1m", "1mb", "1G", "1GB", "1Gb", None])
    def test_validate_size_format_success(self, valid_size):
        assert LivyAsyncHook._validate_size_format(valid_size)

    @pytest.mark.parametrize("invalid_size", ["1Gb foo", "10", 1])
    def test_validate_size_format_failure(self, invalid_size):
        with pytest.raises(ValueError):
            assert LivyAsyncHook._validate_size_format(invalid_size)

    @pytest.mark.parametrize(
        "valid_string",
        [
            [1, "string"],
            (1, "string"),
            [],
        ],
    )
    def test_validate_list_of_stringables_success(self, valid_string):
        assert LivyAsyncHook._validate_list_of_stringables(valid_string)

    @pytest.mark.parametrize("invalid_string", [{"a": "a"}, [1, {}], [1, None], None, 1, "string"])
    def test_validate_list_of_stringables_failure(self, invalid_string):
        with pytest.raises(ValueError):
            LivyAsyncHook._validate_list_of_stringables(invalid_string)

    @pytest.mark.parametrize(
        "conf",
        [
            {"k1": "v1", "k2": 0},
            {},
            None,
        ],
    )
    def test_validate_extra_conf_success(self, conf):
        assert LivyAsyncHook._validate_extra_conf(conf)

    @pytest.mark.parametrize(
        "conf",
        [
            "k1=v1",
            [("k1", "v1"), ("k2", 0)],
            {"outer": {"inner": "val"}},
            {"has_val": "val", "no_val": None},
            {"has_val": "val", "no_val": ""},
        ],
    )
    def test_validate_extra_conf_failure(self, conf):
        with pytest.raises(ValueError):
            LivyAsyncHook._validate_extra_conf(conf)

    def test_parse_request_response(self):
        assert BATCH_ID == LivyAsyncHook._parse_request_response(
            response={"id": BATCH_ID, "log": []}, parameter="id"
        )

    @pytest.mark.parametrize("conn_id", [100, 0])
    def test_check_session_id_success(self, conn_id):
        assert LivyAsyncHook._validate_session_id(conn_id) is None

    @pytest.mark.parametrize("conn_id", [None, "asd"])
    def test_check_session_id_failure(self, conn_id):
        with pytest.raises(TypeError):
            LivyAsyncHook._validate_session_id(None)
