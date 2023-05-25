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

from unittest.mock import call, patch

import pytest
from hdfs import HdfsError

from airflow.models.connection import Connection
from airflow.providers.apache.hdfs.hooks.webhdfs import AirflowWebHDFSHookException, WebHDFSHook


class TestWebHDFSHook:
    def setup_method(self):
        self.webhdfs_hook = WebHDFSHook()

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.requests.Session", return_value="session")
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_1.com,host_2.com", port=321, login="user"),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_get_conn_without_schema(
        self, socket_mock, mock_get_connection, mock_insecure_client, mock_session
    ):
        mock_insecure_client.side_effect = [HdfsError("Error"), mock_insecure_client.return_value]
        socket_mock.socket.return_value.connect_ex.return_value = 0
        conn = self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value
        hosts = connection.host.split(",")
        mock_insecure_client.assert_has_calls(
            [
                call(
                    f"http://{host}:{connection.port}",
                    user=connection.login,
                    session=mock_session.return_value,
                )
                for host in hosts
            ]
        )
        mock_insecure_client.return_value.status.assert_called_once_with("/")
        assert conn == mock_insecure_client.return_value

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.requests.Session", return_value="session")
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_1.com,host_2.com", port=321, schema="schema", login="user"),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_get_conn_with_schema(self, socket_mock, mock_get_connection, mock_insecure_client, mock_session):
        mock_insecure_client.side_effect = [HdfsError("Error"), mock_insecure_client.return_value]
        socket_mock.socket.return_value.connect_ex.return_value = 0
        conn = self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value
        hosts = connection.host.split(",")
        mock_insecure_client.assert_has_calls(
            [
                call(
                    f"http://{host}:{connection.port}/{connection.schema}",
                    user=connection.login,
                    session=mock_session.return_value,
                )
                for host in hosts
            ]
        )
        mock_insecure_client.return_value.status.assert_called_once_with("/")
        assert conn == mock_insecure_client.return_value

    @pytest.mark.parametrize(
        "test_connection",
        [
            pytest.param(Connection(host="host_1.com,host_2.com", login="user"), id="without-password"),
            pytest.param(
                Connection(host="host_1.com,host_2.com", login="user", password="password"),
                id="with-password",
            ),
        ],
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.requests.Session")
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_get_conn_without_port_schema(
        self, socket_mock, mock_insecure_client, mock_session, test_connection
    ):
        with patch.object(WebHDFSHook, "get_connection", return_value=test_connection):
            mock_insecure_client.side_effect = [HdfsError("Error"), mock_insecure_client.return_value]
            socket_mock.socket.return_value.connect_ex.return_value = 0
            conn = self.webhdfs_hook.get_conn()
            hosts = test_connection.host.split(",")
            mock_insecure_client.assert_has_calls(
                [
                    call(
                        f"http://{host}",
                        user=test_connection.login,
                        session=mock_session.return_value,
                    )
                    for host in hosts
                ]
            )
            mock_insecure_client.return_value.status.assert_called_once_with("/")
            assert conn == mock_insecure_client.return_value

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient", side_effect=HdfsError("Error"))
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_2", port=321, login="user"),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_get_conn_hdfs_error(self, socket_mock, mock_get_connection, mock_insecure_client):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        with pytest.raises(AirflowWebHDFSHookException):
            self.webhdfs_hook.get_conn()

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.requests.Session", return_value="session")
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.KerberosClient", create=True)
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_1", port=123),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs._kerberos_security_mode", return_value=True)
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_get_conn_kerberos_security_mode(
        self,
        socket_mock,
        mock_kerberos_security_mode,
        mock_get_connection,
        mock_kerberos_client,
        mock_session,
    ):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        conn = self.webhdfs_hook.get_conn()

        connection = mock_get_connection.return_value
        mock_kerberos_client.assert_called_once_with(
            f"http://{connection.host}:{connection.port}", session=mock_session.return_value
        )
        assert conn == mock_kerberos_client.return_value

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook._find_valid_server", return_value=None)
    def test_get_conn_no_connection_found(self, mock_get_connection):
        with pytest.raises(AirflowWebHDFSHookException):
            self.webhdfs_hook.get_conn()

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_conn")
    def test_check_for_path(self, mock_get_conn):
        hdfs_path = "path"

        exists_path = self.webhdfs_hook.check_for_path(hdfs_path)

        mock_get_conn.assert_called_once_with()
        mock_status = mock_get_conn.return_value.status
        mock_status.assert_called_once_with(hdfs_path, strict=False)
        assert exists_path == bool(mock_status.return_value)

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_conn")
    def test_load_file(self, mock_get_conn):
        source = "source"
        destination = "destination"

        self.webhdfs_hook.load_file(source, destination)

        mock_get_conn.assert_called_once_with()
        mock_upload = mock_get_conn.return_value.upload
        mock_upload.assert_called_once_with(
            hdfs_path=destination, local_path=source, overwrite=True, n_threads=1
        )

    def test_simple_init(self):
        hook = WebHDFSHook()
        assert hook.proxy_user is None

    def test_init_proxy_user(self):
        hook = WebHDFSHook(proxy_user="someone")
        assert "someone" == hook.proxy_user

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.KerberosClient", create=True)
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(
            host="host_1", port=123, extra={"use_ssl": "True", "verify": "/ssl/cert/path"}
        ),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs._kerberos_security_mode", return_value=True)
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_conn_kerberos_ssl(
        self, socket_mock, mock_kerberos_security_mode, mock_get_connection, mock_kerberos_client
    ):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value

        assert f"https://{connection.host}:{connection.port}" == mock_kerberos_client.call_args[0][0]
        assert "/ssl/cert/path" == mock_kerberos_client.call_args[1]["session"].verify

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(
            host="host_1", port=123, schema="schema", extra={"use_ssl": "True", "verify": False}
        ),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_conn_insecure_ssl_with_port_schema(self, socket_mock, mock_get_connection, mock_insecure_client):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value

        assert (
            f"https://{connection.host}:{connection.port}/{connection.schema}"
            == mock_insecure_client.call_args[0][0]
        )
        assert not mock_insecure_client.call_args[1]["session"].verify

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_1", schema="schema", extra={"use_ssl": "True", "verify": False}),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_conn_insecure_ssl_without_port(self, socket_mock, mock_get_connection, mock_insecure_client):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value

        assert f"https://{connection.host}/{connection.schema}" == mock_insecure_client.call_args[0][0]
        assert not mock_insecure_client.call_args[1]["session"].verify

    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.InsecureClient")
    @patch(
        "airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook.get_connection",
        return_value=Connection(host="host_1", port=123, extra={"use_ssl": "True", "verify": False}),
    )
    @patch("airflow.providers.apache.hdfs.hooks.webhdfs.socket")
    def test_conn_insecure_ssl_without_schema(self, socket_mock, mock_get_connection, mock_insecure_client):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.webhdfs_hook.get_conn()
        connection = mock_get_connection.return_value

        assert f"https://{connection.host}:{connection.port}" == mock_insecure_client.call_args[0][0]
        assert not mock_insecure_client.call_args[1]["session"].verify
