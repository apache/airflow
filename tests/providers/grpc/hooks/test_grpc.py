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

import os
from io import StringIO
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.models import Connection
from airflow.providers.grpc.hooks.grpc import GrpcHook


def get_airflow_connection(auth_type="NO_AUTH", credential_pem_file=None, scopes=None):
    extra = {
        "extra__grpc__auth_type": f"{auth_type}",
        "extra__grpc__credential_pem_file": f"{credential_pem_file}",
        "extra__grpc__scopes": f"{scopes}",
    }

    return Connection(conn_id="grpc_default", conn_type="grpc", host="test:8080", extra=extra)


def get_airflow_connection_with_port():
    return Connection(
        conn_id="grpc_default",
        conn_type="grpc",
        host="test.com",
        port=1234,
        extra='{"extra__grpc__auth_type": "NO_AUTH"}',
    )


class StubClass:
    def __init__(self, _):
        pass

    def single_call(self, data):
        return data

    def stream_call(self, data):
        return ["streaming", "call"]


class TestGrpcHook:
    def setup_method(self):
        self.channel_mock = mock.patch("grpc.Channel").start()

    def custom_conn_func(self, _):
        mocked_channel = self.channel_mock.return_value
        return mocked_channel

    @mock.patch("grpc.insecure_channel")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_no_auth_connection(self, mock_get_connection, mock_insecure_channel):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_insecure_channel.return_value = mocked_channel

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_insecure_channel.assert_called_once_with(expected_url)
        assert channel == mocked_channel

    @mock.patch("grpc.insecure_channel")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_connection_with_port(self, mock_get_connection, mock_insecure_channel):
        conn = get_airflow_connection_with_port()
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_insecure_channel.return_value = mocked_channel

        channel = hook.get_conn()
        expected_url = "test.com:1234"

        mock_insecure_channel.assert_called_once_with(expected_url)
        assert channel == mocked_channel

    @mock.patch("airflow.providers.grpc.hooks.grpc.open")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("grpc.ssl_channel_credentials")
    @mock.patch("grpc.secure_channel")
    def test_connection_with_ssl(
        self, mock_secure_channel, mock_channel_credentials, mock_get_connection, mock_open
    ):
        conn = get_airflow_connection(auth_type="SSL", credential_pem_file="pem")
        mock_get_connection.return_value = conn
        mock_open.return_value = StringIO("credential")
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_channel_credentials.return_value = mock_credential_object

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_open.assert_called_once_with("pem", "rb")
        mock_channel_credentials.assert_called_once_with("credential")
        mock_secure_channel.assert_called_once_with(expected_url, mock_credential_object)
        assert channel == mocked_channel

    @mock.patch("airflow.providers.grpc.hooks.grpc.open")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("grpc.ssl_channel_credentials")
    @mock.patch("grpc.secure_channel")
    def test_connection_with_tls(
        self, mock_secure_channel, mock_channel_credentials, mock_get_connection, mock_open
    ):
        conn = get_airflow_connection(auth_type="TLS", credential_pem_file="pem")
        mock_get_connection.return_value = conn
        mock_open.return_value = StringIO("credential")
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_channel_credentials.return_value = mock_credential_object

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_open.assert_called_once_with("pem", "rb")
        mock_channel_credentials.assert_called_once_with("credential")
        mock_secure_channel.assert_called_once_with(expected_url, mock_credential_object)
        assert channel == mocked_channel

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("google.auth.jwt.OnDemandCredentials.from_signing_credentials")
    @mock.patch("google.auth.default")
    @mock.patch("google.auth.transport.grpc.secure_authorized_channel")
    def test_connection_with_jwt(
        self, mock_secure_channel, mock_google_default_auth, mock_google_cred, mock_get_connection
    ):
        conn = get_airflow_connection(auth_type="JWT_GOOGLE")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_google_default_auth.return_value = (mock_credential_object, "")
        mock_google_cred.return_value = mock_credential_object

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_google_cred.assert_called_once_with(mock_credential_object)
        mock_secure_channel.assert_called_once_with(mock_credential_object, None, expected_url)
        assert channel == mocked_channel

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("google.auth.transport.requests.Request")
    @mock.patch("google.auth.default")
    @mock.patch("google.auth.transport.grpc.secure_authorized_channel")
    def test_connection_with_google_oauth(
        self, mock_secure_channel, mock_google_default_auth, mock_google_auth_request, mock_get_connection
    ):
        conn = get_airflow_connection(auth_type="OATH_GOOGLE", scopes="grpc,gcs")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_google_default_auth.return_value = (mock_credential_object, "")
        mock_google_auth_request.return_value = "request"

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_google_default_auth.assert_called_once_with(scopes=["grpc", "gcs"])
        mock_secure_channel.assert_called_once_with(mock_credential_object, "request", expected_url)
        assert channel == mocked_channel

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_custom_connection(self, mock_get_connection):
        conn = get_airflow_connection("CUSTOM")
        mock_get_connection.return_value = conn
        mocked_channel = self.channel_mock.return_value
        hook = GrpcHook("grpc_default", custom_connection_func=self.custom_conn_func)

        channel = hook.get_conn()

        assert channel == mocked_channel

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_custom_connection_with_no_connection_func(self, mock_get_connection):
        conn = get_airflow_connection("CUSTOM")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")

        with pytest.raises(AirflowConfigException):
            hook.get_conn()

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_connection_type_not_supported(self, mock_get_connection):
        conn = get_airflow_connection("NOT_SUPPORT")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")

        with pytest.raises(AirflowConfigException):
            hook.get_conn()

    @mock.patch("grpc.intercept_channel")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("grpc.insecure_channel")
    def test_connection_with_interceptors(
        self, mock_insecure_channel, mock_get_connection, mock_intercept_channel
    ):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = self.channel_mock.return_value
        hook = GrpcHook("grpc_default", interceptors=["test1"])
        mock_insecure_channel.return_value = mocked_channel
        mock_intercept_channel.return_value = mocked_channel

        channel = hook.get_conn()

        assert channel == mocked_channel
        mock_intercept_channel.assert_called_once_with(mocked_channel, "test1")

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("airflow.providers.grpc.hooks.grpc.GrpcHook.get_conn")
    def test_simple_run(self, mock_get_conn, mock_get_connection):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = mock.Mock()
        mocked_channel.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mocked_channel.__exit__ = mock.Mock(return_value=None)
        hook = GrpcHook("grpc_default")
        mock_get_conn.return_value = mocked_channel

        response = hook.run(StubClass, "single_call", data={"data": "hello"})

        assert next(response) == "hello"

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("airflow.providers.grpc.hooks.grpc.GrpcHook.get_conn")
    def test_stream_run(self, mock_get_conn, mock_get_connection):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = mock.Mock()
        mocked_channel.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mocked_channel.__exit__ = mock.Mock(return_value=None)
        hook = GrpcHook("grpc_default")
        mock_get_conn.return_value = mocked_channel

        response = hook.run(StubClass, "stream_call", data={"data": ["hello!", "hi"]})

        assert next(response) == ["streaming", "call"]

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param(
                "a://abc:50?extra__grpc__auth_type=NO_AUTH",
                id="prefix",
            ),
            pytest.param("a://abc:50?auth_type=NO_AUTH", id="no-prefix"),
        ],
    )
    @patch("airflow.providers.grpc.hooks.grpc.grpc.insecure_channel")
    def test_backcompat_prefix_works(self, channel_mock, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = GrpcHook("my_conn")
            hook.get_conn()
            channel_mock.assert_called_with("abc:50")

    def test_backcompat_prefix_both_prefers_short(self):
        with patch.dict(
            os.environ,
            {"AIRFLOW_CONN_MY_CONN": "a://abc:50?extra__grpc__auth_type=non-pref&auth_type=pref"},
        ):
            hook = GrpcHook("my_conn")
            assert hook._get_field("auth_type") == "pref"
