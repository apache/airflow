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

from pathlib import Path
from typing import Any
from unittest.mock import call, patch

import pytest
from hdfs import HdfsError
from hdfs.ext.kerberos import KerberosClient
from requests_mock import Mocker

from airflow import AirflowException
from airflow.models.connection import Connection
from airflow.providers.apache.hdfs.hooks.webhdfs import AirflowWebHDFSHookException, WebHDFSHook

GETFILESTATUS_FILE_NOT_FOUND_JSON = {
    "RemoteException": {
        "exception": "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message": "File does not exist: /root/path",
    }
}
GETFILESTATUS_FILE_FOUND = {
    "FileStatus": {
        "accessTime": 1671547795947,
        "blockSize": 134217728,
        "childrenNum": 0,
        "fileId": 72162413,
        "group": "hdfs",
        "length": 9,
        "modificationTime": 1671547795955,
        "owner": "user",
        "pathSuffix": "",
        "permission": "644",
        "replication": 2,
        "storagePolicy": 0,
        "type": "FILE",
    }
}

GETFILESTATUS_DIRECTORY_FOUND = {
    "FileStatus": {
        "accessTime": 0,
        "blockSize": 0,
        "childrenNum": 7,
        "fileId": 63204162,
        "group": "hdfs",
        "length": 0,
        "modificationTime": 1671716416412,
        "owner": "user",
        "pathSuffix": "",
        "permission": "700",
        "replication": 0,
        "storagePolicy": 0,
        "type": "DIRECTORY",
    }
}

MKDIRS_PERMISSION_DENIED = {
    "RemoteException": {
        "exception": "AccessControlException",
        "javaClassName": "org.apache.hadoop.security.AccessControlException",
        "message": 'Permission denied: user=user, access=WRITE, inode="/user/user2":user2:hdfs:drwxr-xr-x',
    }
}


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

        assert f"https://{connection.host}:{connection.port}" == mock_kerberos_client.call_args.args[0]
        assert "/ssl/cert/path" == mock_kerberos_client.call_args.kwargs["session"].verify

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
            == mock_insecure_client.call_args.args[0]
        )
        assert not mock_insecure_client.call_args.kwargs["session"].verify

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

        assert f"https://{connection.host}/{connection.schema}" == mock_insecure_client.call_args.args[0]
        assert not mock_insecure_client.call_args.kwargs["session"].verify

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

        assert f"https://{connection.host}:{connection.port}" == mock_insecure_client.call_args.args[0]
        assert not mock_insecure_client.call_args.kwargs["session"].verify


class MockedWebHDFSHook(WebHDFSHook):
    """Mock used to test all WebHDFS interaction using requests_mock."""

    def get_conn(self) -> Any:
        return KerberosClient("https://example.com:50471/")


class TestWebHDFSHookInteraction:
    def setup_method(self):
        self.webhdfs_hook = MockedWebHDFSHook()

    def test_check_for_path_existing(self, requests_mock: Mocker) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            json=GETFILESTATUS_FILE_FOUND,
        )
        assert self.webhdfs_hook.check_for_path("/root/path") is True

    def test_check_for_path_non_existing(self, requests_mock: Mocker):
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        assert self.webhdfs_hook.check_for_path("/root/path") is False

    def test_load_file(self, requests_mock: Mocker, tmp_path: Path) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=LISTSTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        requests_mock.put(
            "https://example.com:50471/webhdfs/v1/root/path?op=CREATE",
            status_code=307,
            headers={
                # A data-node.
                "location": "https://example1.com:1022/webhdfs/v1/root/path?op=CREATE"
            },
        )
        requests_mock.put(
            "https://example1.com:1022/webhdfs/v1/root/path?op=CREATE",  # the data node.
            status_code=201,
        )

        filename = tmp_path / "test_webhdfs_load_file"
        filename.write_bytes(b"Log line\n")
        self.webhdfs_hook.load_file(str(filename), "/root/path")
        assert requests_mock.call_count == 3

    def test_webhdfs_parse_webhdfs_url(self):
        assert WebHDFSHook.parse_webhdfs_url("webhdfs:///remote/log/location") == "/remote/log/location"

    @pytest.mark.parametrize(
        "path,error",
        [
            (
                "webhdfs://remote/log/location",
                'Please provide an absolute path instead of "remote/log/location"',
            ),
            ("webhdfs://", "Please provide a non-empty path"),
        ],
    )
    def test_webhdfs_parse_webhdfs_url_not_root(self, path, error):
        with pytest.raises(AirflowException) as e:
            WebHDFSHook.parse_webhdfs_url(path)
        assert str(e.value) == error

    def test_webhdfshook_read_file(self, requests_mock: Mocker) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?offset=0&op=OPEN", content=b"Content"
        )
        assert self.webhdfs_hook.read_file("/root/path") == "Content"

    def test_webhdfshook_read_file_file_not_found(self, requests_mock: Mocker) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?offset=0&op=OPEN",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        with pytest.raises(HdfsError) as e:
            self.webhdfs_hook.read_file("/root/path")
        assert e.value.message == "File does not exist: /root/path"

    def test_webhdfshook_write_file(
        self,
        requests_mock: Mocker,
    ) -> None:
        # Check file exists.
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            json=GETFILESTATUS_FILE_FOUND,
        )

        requests_mock.put(
            "https://example.com:50471/webhdfs/v1/root/path?op=CREATE&overwrite=True",
            status_code=307,
            headers={
                # A data-node.
                "location": "https://example1.com:1022/webhdfs/v1/root/path?op=CREATE"
            },
        )
        requests_mock.put(
            "https://example1.com:1022/webhdfs/v1/root/path?op=CREATE",  # the data node.
            status_code=201,
        )
        self.webhdfs_hook.write_file("log", "/root/path", overwrite=True)
        assert requests_mock.call_count == 3
        assert requests_mock.last_request.text == "log"

    def test_webhdfshook_write_file_do_not_overwrite_existing_file(self, requests_mock: Mocker) -> None:
        # Check file exists.
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS", json=GETFILESTATUS_FILE_FOUND
        )
        with pytest.raises(FileExistsError) as e:
            self.webhdfs_hook.write_file("log", "/root/path", overwrite=False)
        assert str(e.value) == "File /root/path already exists."
        assert requests_mock.call_count == 1

    def test_webhdfshook_append_file(
        self,
        requests_mock: Mocker,
    ) -> None:
        # Check file exists.
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            json=GETFILESTATUS_FILE_FOUND,
        )

        requests_mock.post(
            "https://example.com:50471/webhdfs/v1/root/path?op=APPEND",
            status_code=307,
            headers={
                # A data-node.
                "location": "https://example1.com:1022/webhdfs/v1/root/path?op=APPEND"
            },
        )
        requests_mock.post(
            "https://example1.com:1022/webhdfs/v1/root/path?op=APPEND",  # the data node.
            status_code=201,
        )
        self.webhdfs_hook.append_file("log", "/root/path")
        assert requests_mock.call_count == 3
        assert requests_mock.last_request.text == "log"

    def test_webhdfshook_append_file_append_not_existing_file(self, requests_mock: Mocker) -> None:
        # Check file exists.
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        with pytest.raises(FileNotFoundError) as e:
            self.webhdfs_hook.append_file("log", "/root/path")
        assert str(e.value) == "File /root/path not found."
        assert requests_mock.call_count == 1

    def test_webhdfshook_is_file(self, requests_mock: Mocker) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS", json=GETFILESTATUS_FILE_FOUND
        )
        assert self.webhdfs_hook.is_file("/root/path") is True
        assert self.webhdfs_hook.is_directory("/root/path") is False

    def test_webhdfshook_is_directory(self, requests_mock: Mocker) -> None:
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/root/path?op=GETFILESTATUS",
            json=GETFILESTATUS_DIRECTORY_FOUND,
        )
        assert self.webhdfs_hook.is_file("/root/path") is False
        assert self.webhdfs_hook.is_directory("/root/path") is True

    def test_webhdfshook_make_directory(self, requests_mock: Mocker) -> None:
        requests_mock.put("https://example.com:50471/webhdfs/v1/root/path?op=MKDIRS")
        self.webhdfs_hook.make_directory("/root/path")
        assert requests_mock.call_count == 1

    def test_webhdfshook_make_directory_permission_denied(self, requests_mock: Mocker) -> None:
        requests_mock.put(
            "https://example.com:50471/webhdfs/v1/root/path?op=MKDIRS",
            status_code=403,
            json=MKDIRS_PERMISSION_DENIED,
        )
        with pytest.raises(HdfsError) as e:
            self.webhdfs_hook.make_directory("/root/path")
        assert requests_mock.call_count == 1
        assert (
            str(e.value)
            == 'Permission denied: user=user, access=WRITE, inode="/user/user2":user2:hdfs:drwxr-xr-x'
        )
