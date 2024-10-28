#
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

import contextlib
import functools
import json
import logging
import os
from http import HTTPStatus
from unittest import mock

import pytest
import requests
import tenacity
from aioresponses import aioresponses
from requests.adapters import Response
from requests.auth import AuthBase, HTTPBasicAuth
from requests.models import DEFAULT_REDIRECT_LIMIT

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook


@pytest.fixture
def aioresponse():
    """
    Creates mock async API response.
    """
    with aioresponses() as async_response:
        yield async_response


def get_airflow_connection(conn_id: str = "http_default"):
    return Connection(
        conn_id=conn_id, conn_type="http", host="test:8080/", extra='{"bearer": "test"}'
    )


def get_airflow_connection_with_extra(extra: dict):
    def inner(conn_id: str = "http_default"):
        return Connection(
            conn_id=conn_id, conn_type="http", host="test:8080/", extra=json.dumps(extra)
        )

    return inner


def get_airflow_connection_with_port(conn_id: str = "http_default"):
    return Connection(conn_id=conn_id, conn_type="http", host="test.com", port=1234)


def get_airflow_connection_with_login_and_password(conn_id: str = "http_default"):
    return Connection(
        conn_id=conn_id,
        conn_type="http",
        host="test.com",
        login="username",
        password="pass",
    )


class TestHttpHook:
    """Test get, post and raise_for_status"""

    def setup_method(self):
        import requests_mock

        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount("mock", adapter)
        self.get_hook = HttpHook(method="GET")
        self.get_lowercase_hook = HttpHook(method="get")
        self.post_hook = HttpHook(method="POST")

    def test_raise_for_status_with_200(self, requests_mock):
        requests_mock.get(
            "http://test:8080/v1/test",
            status_code=200,
            text='{"status":{"status": 200}}',
            reason="OK",
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            resp = self.get_hook.run("v1/test")
            assert resp.text == '{"status":{"status": 200}}'

    @mock.patch("requests.Request")
    @mock.patch("requests.Session")
    def test_get_request_with_port(self, mock_session, mock_request):
        from requests.exceptions import MissingSchema

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            expected_url = "http://test.com:1234/some/endpoint"
            for endpoint in ["some/endpoint", "/some/endpoint"]:
                with contextlib.suppress(MissingSchema):
                    self.get_hook.run(endpoint)

                mock_request.assert_called_once_with(
                    mock.ANY, expected_url, headers=mock.ANY, params=mock.ANY
                )

                mock_request.reset_mock()

    def test_get_request_do_not_raise_for_status_if_check_response_is_false(
        self, requests_mock
    ):
        requests_mock.get(
            "http://test:8080/v1/test",
            status_code=404,
            text='{"status":{"status": 404}}',
            reason="Bad request",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            resp = self.get_hook.run("v1/test", extra_options={"check_response": False})
            assert resp.text == '{"status":{"status": 404}}'

    def test_hook_contains_header_from_extra_field(self):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            expected_conn = get_airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) == conn.headers
            assert conn.headers.get("bearer") == "test"

    def test_hook_ignore_max_redirects_from_extra_field_as_header(self):
        airflow_connection = get_airflow_connection_with_extra(
            extra={"bearer": "test", "max_redirects": 3}
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            expected_conn = airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) != conn.headers
            assert conn.headers.get("bearer") == "test"
            assert conn.headers.get("allow_redirects") is None
            assert conn.proxies == {}
            assert conn.stream is False
            assert conn.verify is True
            assert conn.cert is None
            assert conn.max_redirects == 3
            assert conn.trust_env is True

    def test_hook_ignore_proxies_from_extra_field_as_header(self):
        airflow_connection = get_airflow_connection_with_extra(
            extra={
                "bearer": "test",
                "proxies": {"http": "http://proxy:80", "https": "https://proxy:80"},
            }
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            expected_conn = airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) != conn.headers
            assert conn.headers.get("bearer") == "test"
            assert conn.headers.get("proxies") is None
            assert conn.proxies == {
                "http": "http://proxy:80",
                "https": "https://proxy:80",
            }
            assert conn.stream is False
            assert conn.verify is True
            assert conn.cert is None
            assert conn.max_redirects == DEFAULT_REDIRECT_LIMIT
            assert conn.trust_env is True

    def test_hook_ignore_verify_from_extra_field_as_header(self):
        airflow_connection = get_airflow_connection_with_extra(
            extra={"bearer": "test", "verify": False}
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            expected_conn = airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) != conn.headers
            assert conn.headers.get("bearer") == "test"
            assert conn.headers.get("verify") is None
            assert conn.proxies == {}
            assert conn.stream is False
            assert conn.verify is False
            assert conn.cert is None
            assert conn.max_redirects == DEFAULT_REDIRECT_LIMIT
            assert conn.trust_env is True

    def test_hook_ignore_cert_from_extra_field_as_header(self):
        airflow_connection = get_airflow_connection_with_extra(
            extra={"bearer": "test", "cert": "cert.crt"}
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            expected_conn = airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) != conn.headers
            assert conn.headers.get("bearer") == "test"
            assert conn.headers.get("cert") is None
            assert conn.proxies == {}
            assert conn.stream is False
            assert conn.verify is True
            assert conn.cert == "cert.crt"
            assert conn.max_redirects == DEFAULT_REDIRECT_LIMIT
            assert conn.trust_env is True

    def test_hook_ignore_trust_env_from_extra_field_as_header(self):
        airflow_connection = get_airflow_connection_with_extra(
            extra={"bearer": "test", "trust_env": False}
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            expected_conn = airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) != conn.headers
            assert conn.headers.get("bearer") == "test"
            assert conn.headers.get("cert") is None
            assert conn.proxies == {}
            assert conn.stream is False
            assert conn.verify is True
            assert conn.cert is None
            assert conn.max_redirects == DEFAULT_REDIRECT_LIMIT
            assert conn.trust_env is False

    @mock.patch("requests.Request")
    def test_hook_with_method_in_lowercase(self, mock_requests):
        from requests.exceptions import InvalidURL, MissingSchema

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            data = "test params"
            with contextlib.suppress(MissingSchema, InvalidURL):
                self.get_lowercase_hook.run("v1/test", data=data)
            mock_requests.assert_called_once_with(
                mock.ANY, mock.ANY, headers=mock.ANY, params=data
            )

    @pytest.mark.db_test
    def test_hook_uses_provided_header(self):
        conn = self.get_hook.get_conn(headers={"bearer": "newT0k3n"})
        assert conn.headers.get("bearer") == "newT0k3n"

    @pytest.mark.db_test
    def test_hook_has_no_header_from_extra(self):
        conn = self.get_hook.get_conn()
        assert conn.headers.get("bearer") is None

    def test_hooks_header_from_extra_is_overridden(self):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            conn = self.get_hook.get_conn(headers={"bearer": "newT0k3n"})
            assert conn.headers.get("bearer") == "newT0k3n"

    def test_post_request(self, requests_mock):
        requests_mock.post(
            "http://test:8080/v1/test",
            status_code=200,
            text='{"status":{"status": 200}}',
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            resp = self.post_hook.run("v1/test")
            assert resp.status_code == 200

    def test_post_request_with_error_code(self, requests_mock):
        requests_mock.post(
            "http://test:8080/v1/test",
            status_code=418,
            text='{"status":{"status": 418}}',
            reason="I'm a teapot",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            with pytest.raises(AirflowException):
                self.post_hook.run("v1/test")

    def test_post_request_do_not_raise_for_status_if_check_response_is_false(
        self, requests_mock
    ):
        requests_mock.post(
            "http://test:8080/v1/test",
            status_code=418,
            text='{"status":{"status": 418}}',
            reason="I'm a teapot",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            resp = self.post_hook.run("v1/test", extra_options={"check_response": False})
            assert resp.status_code == 418

    @pytest.mark.db_test
    @mock.patch("airflow.providers.http.hooks.http.requests.Session")
    def test_retry_on_conn_error(self, mocked_session):
        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(7),
            retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
        )

        def send_and_raise(unused_request, **kwargs):
            raise requests.exceptions.ConnectionError

        mocked_session().send.side_effect = send_and_raise
        # The job failed for some reason
        with pytest.raises(tenacity.RetryError):
            self.get_hook.run_with_advanced_retry(
                endpoint="v1/test", _retry_args=retry_args
            )
        assert (
            self.get_hook._retry_obj.stop.max_attempt_number + 1
            == mocked_session.call_count
        )

    def test_run_with_advanced_retry(self, requests_mock):
        requests_mock.get("http://test:8080/v1/test", status_code=200, reason="OK")

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception_type(Exception),
            reraise=True,
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            response = self.get_hook.run_with_advanced_retry(
                endpoint="v1/test", _retry_args=retry_args
            )
            assert isinstance(response, requests.Response)

    def test_header_from_extra_and_run_method_are_merged(self):
        def run_and_return(
            unused_session, prepped_request, unused_extra_options, **kwargs
        ):
            return prepped_request

        # The job failed for some reason
        with mock.patch(
            "airflow.providers.http.hooks.http.HttpHook.run_and_check",
            side_effect=run_and_return,
        ):
            with mock.patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
            ):
                prepared_request = self.get_hook.run(
                    "v1/test", headers={"some_other_header": "test"}
                )
                actual = dict(prepared_request.headers)
                assert actual.get("bearer") == "test"
                assert actual.get("some_other_header") == "test"

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
    def test_http_connection(self, mock_get_connection):
        conn = Connection(
            conn_id="http_default", conn_type="http", host="localhost", schema="http"
        )
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == "http://localhost"

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
    def test_https_connection(self, mock_get_connection):
        conn = Connection(
            conn_id="http_default", conn_type="http", host="localhost", schema="https"
        )
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == "https://localhost"

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
    def test_host_encoded_http_connection(self, mock_get_connection):
        conn = Connection(
            conn_id="http_default", conn_type="http", host="http://localhost"
        )
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == "http://localhost"

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
    def test_host_encoded_https_connection(self, mock_get_connection):
        conn = Connection(
            conn_id="http_default", conn_type="http", host="https://localhost"
        )
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == "https://localhost"

    def test_method_converted_to_uppercase_when_created_in_lowercase(self):
        assert self.get_lowercase_hook.method == "GET"

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.get_connection")
    def test_connection_without_host(self, mock_get_connection):
        conn = Connection(conn_id="http_default", conn_type="http")
        mock_get_connection.return_value = conn

        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == "http://"

    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_json_request(self, method, requests_mock):
        obj1 = {"a": 1, "b": "abc", "c": [1, 2, {"d": 10}]}

        def match_obj1(request):
            return request.json() == obj1

        requests_mock.request(
            method=method, url="//test:8080/v1/test", additional_matcher=match_obj1
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            # will raise NoMockAddress exception if obj1 != request.json()
            HttpHook(method=method).run("v1/test", json=obj1)

    @mock.patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_verify_set_to_true_by_default(self, mock_session_send):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            self.get_hook.run("/some/endpoint")
            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies={},
                stream=False,
                timeout=None,
                verify=True,
            )

    @mock.patch("airflow.providers.http.hooks.http.requests.Session.send")
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_requests_ca_bundle_env_var(self, mock_session_send):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            self.get_hook.run("/some/endpoint")

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies={},
                stream=False,
                timeout=None,
                verify="/tmp/test.crt",
            )

    @mock.patch("airflow.providers.http.hooks.http.requests.Session.send")
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_respects_requests_ca_bundle_env_var(self, mock_session_send):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            self.get_hook.run("/some/endpoint", extra_options={"verify": True})

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies={},
                stream=False,
                timeout=None,
                verify="/tmp/test.crt",
            )

    @mock.patch("airflow.providers.http.hooks.http.requests.Session.send")
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_false_parameter_overwrites_set_requests_ca_bundle_env_var(
        self, mock_session_send
    ):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_port,
        ):
            self.get_hook.run("/some/endpoint", extra_options={"verify": False})

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies={},
                stream=False,
                timeout=None,
                verify=False,
            )

    def test_connection_success(self, requests_mock):
        requests_mock.get(
            "http://test:8080",
            status_code=200,
            json={"status": {"status": 200}},
            reason="OK",
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            status, msg = self.get_hook.test_connection()
            assert status is True
            assert msg == "Connection successfully tested"

    def test_connection_failure(self, requests_mock):
        requests_mock.get(
            "http://test:8080",
            status_code=500,
            json={"message": "internal server error"},
            reason="NOT_OK",
        )
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            status, msg = self.get_hook.test_connection()
            assert status is False
            assert msg == "500:NOT_OK"

    @mock.patch("requests.auth.AuthBase.__init__")
    def test_loginless_custom_auth_initialized_with_no_args(self, auth):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            auth.return_value = None
            hook = HttpHook("GET", "http_default", AuthBase)
            hook.get_conn()
            auth.assert_called_once_with()

    @mock.patch("requests.auth.AuthBase.__init__")
    def test_loginless_custom_auth_initialized_with_args(self, auth):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            auth.return_value = None
            auth_with_args = functools.partial(AuthBase, "test_arg")
            hook = HttpHook("GET", "http_default", auth_with_args)
            hook.get_conn()
            auth.assert_called_once_with("test_arg")

    @mock.patch("requests.auth.HTTPBasicAuth.__init__")
    def test_login_password_basic_auth_initialized(self, auth):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection_with_login_and_password,
        ):
            auth.return_value = None
            hook = HttpHook("GET", "http_default", HTTPBasicAuth)
            hook.get_conn()
            auth.assert_called_once_with("username", "pass")

    @mock.patch("requests.auth.HTTPBasicAuth.__init__")
    def test_default_auth_not_initialized(self, auth):
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            auth.return_value = None
            hook = HttpHook("GET", "http_default")
            hook.get_conn()
            auth.assert_not_called()

    def test_keep_alive_enabled(self):
        with (
            mock.patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection_with_port,
            ),
            mock.patch(
                "requests_toolbelt.adapters.socket_options.TCPKeepAliveAdapter.send"
            ) as tcp_keep_alive_send,
            mock.patch("requests.adapters.HTTPAdapter.send") as http_send,
        ):
            hook = HttpHook(method="GET")
            response = Response()
            response.status_code = HTTPStatus.OK
            tcp_keep_alive_send.return_value = response
            http_send.return_value = response
            hook.run("v1/test")
            tcp_keep_alive_send.assert_called()
            http_send.assert_not_called()

    def test_keep_alive_disabled(self):
        with (
            mock.patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection_with_port,
            ),
            mock.patch(
                "requests_toolbelt.adapters.socket_options.TCPKeepAliveAdapter.send"
            ) as tcp_keep_alive_send,
            mock.patch("requests.adapters.HTTPAdapter.send") as http_send,
        ):
            hook = HttpHook(method="GET", tcp_keep_alive=False)
            response = Response()
            response.status_code = HTTPStatus.OK
            tcp_keep_alive_send.return_value = response
            http_send.return_value = response
            hook.run("v1/test")
            tcp_keep_alive_send.assert_not_called()
            http_send.assert_called()

    @pytest.mark.parametrize(
        "base_url, endpoint, expected_url",
        [
            pytest.param(
                "https://example.org",
                "/v1/test",
                "https://example.org/v1/test",
                id="both-set",
            ),
            pytest.param(
                "", "http://foo/bar/v1/test", "http://foo/bar/v1/test", id="only-endpoint"
            ),
        ],
    )
    def test_url_from_endpoint(self, base_url: str, endpoint: str, expected_url: str):
        hook = HttpHook()
        hook.base_url = base_url
        assert hook.url_from_endpoint(endpoint) == expected_url


class TestHttpAsyncHook:
    @pytest.mark.asyncio
    async def test_do_api_call_async_non_retryable_error(self, aioresponse):
        """Test api call asynchronously with non retryable error."""
        hook = HttpAsyncHook(method="GET")
        aioresponse.get("http://httpbin.org/non_existent_endpoint", status=400)

        with (
            pytest.raises(AirflowException, match="400:Bad Request"),
            mock.patch.dict(
                "os.environ",
                AIRFLOW_CONN_HTTP_DEFAULT="http://httpbin.org/",
            ),
        ):
            await hook.run(endpoint="non_existent_endpoint")

    @pytest.mark.asyncio
    async def test_do_api_call_async_retryable_error(self, caplog, aioresponse):
        """Test api call asynchronously with retryable error."""
        caplog.set_level(logging.WARNING, logger="airflow.providers.http.hooks.http")
        hook = HttpAsyncHook(method="GET")
        aioresponse.get(
            "http://httpbin.org/non_existent_endpoint", status=500, repeat=True
        )

        with (
            pytest.raises(AirflowException, match="500:Internal Server Error"),
            mock.patch.dict(
                "os.environ",
                AIRFLOW_CONN_HTTP_DEFAULT="http://httpbin.org/",
            ),
        ):
            await hook.run(endpoint="non_existent_endpoint")

        assert (
            "[Try 3 of 3] Request to http://httpbin.org/non_existent_endpoint failed"
            in caplog.text
        )

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_do_api_call_async_unknown_method(self):
        """Test api call asynchronously for unknown http method."""
        hook = HttpAsyncHook(method="NOPE")
        json = {"existing_cluster_id": "xxxx-xxxxxx-xxxxxx"}

        with pytest.raises(AirflowException, match="Unexpected HTTP Method: NOPE"):
            await hook.run(endpoint="non_existent_endpoint", data=json)

    @pytest.mark.asyncio
    async def test_async_post_request(self, aioresponse):
        """Test api call asynchronously for POST request."""
        hook = HttpAsyncHook()

        aioresponse.post(
            "http://test:8080/v1/test",
            status=200,
            payload='{"status":{"status": 200}}',
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            resp = await hook.run("v1/test")
            assert resp.status == 200

    @pytest.mark.asyncio
    async def test_async_post_request_with_error_code(self, aioresponse):
        """Test api call asynchronously for POST request with error."""
        hook = HttpAsyncHook()

        aioresponse.post(
            "http://test:8080/v1/test",
            status=418,
            payload='{"status":{"status": 418}}',
            reason="I am teapot",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            with pytest.raises(AirflowException):
                await hook.run("v1/test")

    @pytest.mark.asyncio
    async def test_async_request_uses_connection_extra(self, aioresponse):
        """Test api call asynchronously with a connection that has extra field."""

        connection_extra = {"bearer": "test"}

        aioresponse.post(
            "http://test:8080/v1/test",
            status=200,
            payload='{"status":{"status": 200}}',
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = HttpAsyncHook()
            with mock.patch(
                "aiohttp.ClientSession.post", new_callable=mock.AsyncMock
            ) as mocked_function:
                await hook.run("v1/test")
                headers = mocked_function.call_args.kwargs.get("headers")
                assert all(
                    key in headers and headers[key] == value
                    for key, value in connection_extra.items()
                )

    @pytest.mark.asyncio
    async def test_async_request_uses_connection_extra_with_requests_parameters(self):
        """Test api call asynchronously with a connection that has extra field."""
        connection_extra = {"bearer": "test"}
        proxy = {"http": "http://proxy:80", "https": "https://proxy:80"}
        airflow_connection = get_airflow_connection_with_extra(
            extra={
                **connection_extra,
                **{
                    "proxies": proxy,
                    "timeout": 60,
                    "verify": False,
                    "allow_redirects": False,
                    "max_redirects": 3,
                    "trust_env": False,
                },
            }
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", side_effect=airflow_connection
        ):
            hook = HttpAsyncHook()
            with mock.patch(
                "aiohttp.ClientSession.post", new_callable=mock.AsyncMock
            ) as mocked_function:
                await hook.run("v1/test")
                headers = mocked_function.call_args.kwargs.get("headers")
                assert all(
                    key in headers and headers[key] == value
                    for key, value in connection_extra.items()
                )
                assert mocked_function.call_args.kwargs.get("proxy") == proxy
                assert mocked_function.call_args.kwargs.get("timeout") == 60
                assert mocked_function.call_args.kwargs.get("verify_ssl") is False
                assert mocked_function.call_args.kwargs.get("allow_redirects") is False
                assert mocked_function.call_args.kwargs.get("max_redirects") == 3
                assert mocked_function.call_args.kwargs.get("trust_env") is False

    def test_process_extra_options_from_connection(self):
        extra_options = {}
        proxy = {"http": "http://proxy:80", "https": "https://proxy:80"}
        conn = get_airflow_connection_with_extra(
            extra={
                "bearer": "test",
                "stream": True,
                "cert": "cert.crt",
                "proxies": proxy,
                "timeout": 60,
                "verify": False,
                "allow_redirects": False,
                "max_redirects": 3,
                "trust_env": False,
            }
        )()

        actual = HttpAsyncHook._process_extra_options_from_connection(
            conn=conn, extra_options=extra_options
        )

        assert extra_options == {
            "proxy": proxy,
            "timeout": 60,
            "verify_ssl": False,
            "allow_redirects": False,
            "max_redirects": 3,
            "trust_env": False,
        }
        assert actual == {"bearer": "test"}

    @pytest.mark.asyncio
    async def test_build_request_url_from_connection(self):
        conn = get_airflow_connection()
        schema = conn.schema or "http"  # default to http
        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            hook = HttpAsyncHook()
            with mock.patch(
                "aiohttp.ClientSession.post", new_callable=mock.AsyncMock
            ) as mocked_function:
                await hook.run("v1/test")
                assert (
                    mocked_function.call_args.args[0] == f"{schema}://{conn.host}v1/test"
                )

    @pytest.mark.asyncio
    async def test_build_request_url_from_endpoint_param(self):
        def get_empty_conn(conn_id: str = "http_default"):
            return Connection(conn_id=conn_id, conn_type="http")

        hook = HttpAsyncHook()
        with (
            mock.patch(
                "airflow.hooks.base.BaseHook.get_connection", side_effect=get_empty_conn
            ),
            mock.patch(
                "aiohttp.ClientSession.post", new_callable=mock.AsyncMock
            ) as mocked_function,
        ):
            await hook.run("test.com:8080/v1/test")
            assert mocked_function.call_args.args[0] == "http://test.com:8080/v1/test"
