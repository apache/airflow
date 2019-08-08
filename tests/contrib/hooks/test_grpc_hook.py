# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
from io import StringIO

from airflow.exceptions import AirflowConfigException
from airflow.contrib.hooks.grpc_hook import GrpcHook
from airflow.models import Connection
from tests.compat import mock


def get_airflow_connection(auth_type="NO_AUTH", credential_pem_file=None, scopes=None):
    extra = \
        '{{"extra__grpc__auth_type": "{auth_type}",' \
        '"extra__grpc__credential_pem_file": "{credential_pem_file}",' \
        '"extra__grpc__scopes": "{scopes}"}}' \
        .format(auth_type=auth_type,
                credential_pem_file=credential_pem_file,
                scopes=scopes)

    return Connection(
        conn_id='grpc_default',
        conn_type='grpc',
        host='test:8080',
        extra=extra
    )


def get_airflow_connection_with_port():
    return Connection(
        conn_id='grpc_default',
        conn_type='grpc',
        host='test.com',
        port=1234,
        extra='{"extra__grpc__auth_type": "NO_AUTH"}'
    )


class StubClass:
    def __init__(self, _):
        pass

    def single_call(self, data):
        return data

    def stream_call(self, data):  # pylint: disable=unused-argument
        return ["streaming", "call"]


class TestGrpcHook(unittest.TestCase):
    def setUp(self):
        self.channel_mock = mock.patch('grpc.Channel').start()

    def custom_conn_func(self, _):
        mocked_channel = self.channel_mock.return_value
        return mocked_channel

    @mock.patch('grpc.insecure_channel')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_no_auth_connection(self, mock_get_connection, mock_insecure_channel):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_insecure_channel.return_value = mocked_channel

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_insecure_channel.assert_called_once_with(expected_url)
        self.assertEqual(channel, mocked_channel)

    @mock.patch('grpc.insecure_channel')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_connection_with_port(self, mock_get_connection, mock_insecure_channel):
        conn = get_airflow_connection_with_port()
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_insecure_channel.return_value = mocked_channel

        channel = hook.get_conn()
        expected_url = "test.com:1234"

        mock_insecure_channel.assert_called_once_with(expected_url)
        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.contrib.hooks.grpc_hook.open')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('grpc.ssl_channel_credentials')
    @mock.patch('grpc.secure_channel')
    def test_connection_with_ssl(self,
                                 mock_secure_channel,
                                 mock_channel_credentials,
                                 mock_get_connection,
                                 mock_open):
        conn = get_airflow_connection(
            auth_type="SSL",
            credential_pem_file="pem"
        )
        mock_get_connection.return_value = conn
        mock_open.return_value = StringIO('credential')
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_channel_credentials.return_value = mock_credential_object

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_open.assert_called_once_with("pem")
        mock_channel_credentials.assert_called_once_with('credential')
        mock_secure_channel.assert_called_once_with(
            expected_url,
            mock_credential_object
        )
        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.contrib.hooks.grpc_hook.open')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('grpc.ssl_channel_credentials')
    @mock.patch('grpc.secure_channel')
    def test_connection_with_tls(self,
                                 mock_secure_channel,
                                 mock_channel_credentials,
                                 mock_get_connection,
                                 mock_open):
        conn = get_airflow_connection(
            auth_type="TLS",
            credential_pem_file="pem"
        )
        mock_get_connection.return_value = conn
        mock_open.return_value = StringIO('credential')
        hook = GrpcHook("grpc_default")
        mocked_channel = self.channel_mock.return_value
        mock_secure_channel.return_value = mocked_channel
        mock_credential_object = "test_credential_object"
        mock_channel_credentials.return_value = mock_credential_object

        channel = hook.get_conn()
        expected_url = "test:8080"

        mock_open.assert_called_once_with("pem")
        mock_channel_credentials.assert_called_once_with('credential')
        mock_secure_channel.assert_called_once_with(
            expected_url,
            mock_credential_object
        )
        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('google.auth.jwt.OnDemandCredentials.from_signing_credentials')
    @mock.patch('google.auth.default')
    @mock.patch('google.auth.transport.grpc.secure_authorized_channel')
    def test_connection_with_jwt(self,
                                 mock_secure_channel,
                                 mock_google_default_auth,
                                 mock_google_cred,
                                 mock_get_connection):
        conn = get_airflow_connection(
            auth_type="JWT_GOOGLE"
        )
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
        mock_secure_channel.assert_called_once_with(
            mock_credential_object,
            None,
            expected_url
        )
        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('google.auth.transport.requests.Request')
    @mock.patch('google.auth.default')
    @mock.patch('google.auth.transport.grpc.secure_authorized_channel')
    def test_connection_with_google_oauth(self,
                                          mock_secure_channel,
                                          mock_google_default_auth,
                                          mock_google_auth_request,
                                          mock_get_connection):
        conn = get_airflow_connection(
            auth_type="OATH_GOOGLE",
            scopes="grpc,gcs"
        )
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
        mock_secure_channel.assert_called_once_with(
            mock_credential_object,
            "request",
            expected_url
        )
        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_custom_connection(self, mock_get_connection):
        conn = get_airflow_connection("CUSTOM")
        mock_get_connection.return_value = conn
        mocked_channel = self.channel_mock.return_value
        hook = GrpcHook("grpc_default", custom_connection_func=self.custom_conn_func)

        channel = hook.get_conn()

        self.assertEqual(channel, mocked_channel)

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_custom_connection_with_no_connection_func(self, mock_get_connection):
        conn = get_airflow_connection("CUSTOM")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")

        with self.assertRaises(AirflowConfigException):
            hook.get_conn()

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_connection_type_not_supported(self, mock_get_connection):
        conn = get_airflow_connection("NOT_SUPPORT")
        mock_get_connection.return_value = conn
        hook = GrpcHook("grpc_default")

        with self.assertRaises(AirflowConfigException):
            hook.get_conn()

    @mock.patch('grpc.intercept_channel')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('grpc.insecure_channel')
    def test_connection_with_interceptors(self,
                                          mock_insecure_channel,
                                          mock_get_connection,
                                          mock_intercept_channel):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = self.channel_mock.return_value
        hook = GrpcHook("grpc_default", interceptors=["test1"])
        mock_insecure_channel.return_value = mocked_channel
        mock_intercept_channel.return_value = mocked_channel

        channel = hook.get_conn()

        self.assertEqual(channel, mocked_channel)
        mock_intercept_channel.assert_called_once_with(mocked_channel, "test1")

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('airflow.contrib.hooks.grpc_hook.GrpcHook.get_conn')
    def test_simple_run(self, mock_get_conn, mock_get_connection):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = mock.Mock()
        mocked_channel.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mocked_channel.__exit__ = mock.Mock(return_value=None)
        hook = GrpcHook("grpc_default")
        mock_get_conn.return_value = mocked_channel

        response = hook.run(StubClass, "single_call", data={'data': 'hello'})

        self.assertEqual(next(response), "hello")

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @mock.patch('airflow.contrib.hooks.grpc_hook.GrpcHook.get_conn')
    def test_stream_run(self, mock_get_conn, mock_get_connection):
        conn = get_airflow_connection()
        mock_get_connection.return_value = conn
        mocked_channel = mock.Mock()
        mocked_channel.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mocked_channel.__exit__ = mock.Mock(return_value=None)
        hook = GrpcHook("grpc_default")
        mock_get_conn.return_value = mocked_channel

        response = hook.run(StubClass, "stream_call", data={'data': ['hello!', "hi"]})

        self.assertEqual(next(response), ["streaming", "call"])
