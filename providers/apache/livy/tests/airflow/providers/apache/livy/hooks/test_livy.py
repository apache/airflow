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
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.utils import db
from tests.test_utils.db import clear_db_connections

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
