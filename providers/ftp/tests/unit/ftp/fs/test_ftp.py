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

import ftplib
from unittest import mock

import pytest
from fsspec.implementations.ftp import FTPFileSystem

from airflow.providers.ftp.fs.ftp import _AirflowFTPFileSystem, get_fs, schemes

TEST_CONN_ID = "ftp_fs_test_conn"
FS_CLASS = "airflow.providers.ftp.fs.ftp._AirflowFTPFileSystem"


@pytest.fixture
def ftp_connection(monkeypatch):
    def _set(uri: str) -> None:
        monkeypatch.setenv(f"AIRFLOW_CONN_{TEST_CONN_ID}".upper(), uri)

    return _set


@pytest.fixture
def mock_connect():
    """Replace the network connection of ``FTPFileSystem`` with a mocked ``ftplib.FTP``."""

    def connect(fs):
        fs.ftp = mock.create_autospec(ftplib.FTP, instance=True)

    with mock.patch.object(FTPFileSystem, "_connect", autospec=True, side_effect=connect) as patched:
        yield patched


class TestFtpFilesystem:
    def test_schemes(self):
        assert schemes == ["ftp"]

    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_with_connection(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost:2121?encoding=cp1251")

        get_fs(conn_id=TEST_CONN_ID)

        mock_ftp_fs.assert_called_once_with(
            passive=True,
            host="testhost",
            port=2121,
            username="testuser",
            password="testpass",
            encoding="cp1251",
        )

    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_defaults_to_port_21(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost")

        get_fs(conn_id=TEST_CONN_ID)

        mock_ftp_fs.assert_called_once_with(
            passive=True, host="testhost", port=21, username="testuser", password="testpass"
        )

    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_without_connection(self, mock_ftp_fs):
        get_fs(conn_id=None, storage_options={"host": "manual-host", "username": "manual-user"})

        mock_ftp_fs.assert_called_once_with(host="manual-host", username="manual-user")

    @pytest.mark.parametrize(
        ("conn_uri", "conn_id", "storage_options", "match"),
        [
            pytest.param(None, None, None, "needs a connection id", id="no-connection"),
            pytest.param(
                "ftp://testuser:testpass@", TEST_CONN_ID, None, "has no host", id="connection-without-host"
            ),
        ],
    )
    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_without_host_raises(
        self, mock_ftp_fs, ftp_connection, conn_uri, conn_id, storage_options, match
    ):
        if conn_uri:
            ftp_connection(conn_uri)

        with pytest.raises(ValueError, match=match):
            get_fs(conn_id=conn_id, storage_options=storage_options)

        mock_ftp_fs.assert_not_called()

    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_host_from_storage_options_for_connection_without_host(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@")

        get_fs(conn_id=TEST_CONN_ID, storage_options={"host": "manual-host"})

        mock_ftp_fs.assert_called_once_with(
            passive=True, host="manual-host", port=21, username="testuser", password="testpass"
        )

    @mock.patch(FS_CLASS, autospec=True)
    def test_get_fs_storage_options_override_connection(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost")

        get_fs(conn_id=TEST_CONN_ID, storage_options={"port": 2121, "timeout": 5})

        mock_ftp_fs.assert_called_once_with(
            passive=True, host="testhost", port=2121, username="testuser", password="testpass", timeout=5
        )

    @pytest.mark.parametrize(
        ("query", "passive"),
        [
            pytest.param("", True, id="passive-by-default"),
            pytest.param("?passive=true", True, id="passive"),
            pytest.param("?passive=false", False, id="active"),
        ],
    )
    def test_passive_extra_is_applied_on_every_connect(self, mock_connect, ftp_connection, query, passive):
        ftp_connection(f"ftp://testuser:testpass@testhost{query}")

        fs = get_fs(conn_id=TEST_CONN_ID)
        fs.ftp.set_pasv.assert_called_once_with(passive)

        # A read that is cut short reconnects through ``_connect``.
        fs._connect()

        fs.ftp.set_pasv.assert_called_once_with(passive)
        assert mock_connect.call_count == 2

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            pytest.param("ls", ("/data",), id="ls"),
            pytest.param("_open", ("/data/file.csv",), id="open"),
        ],
    )
    def test_reconnects_when_the_server_closed_the_connection(self, mock_connect, method, args):
        fs = _AirflowFTPFileSystem(host="testhost")
        fs.ftp.voidcmd.side_effect = ftplib.error_temp("421 Timeout.")

        with mock.patch.object(FTPFileSystem, method, autospec=True) as parent_method:
            getattr(fs, method)(*args)

        assert mock_connect.call_count == 2
        parent_method.assert_called_once()

    @pytest.mark.parametrize("method", ["ls", "_open"])
    def test_keeps_a_live_connection(self, mock_connect, method):
        fs = _AirflowFTPFileSystem(host="testhost")

        with mock.patch.object(FTPFileSystem, method, autospec=True):
            getattr(fs, method)("/data")

        fs.ftp.voidcmd.assert_called_once_with("NOOP")
        assert mock_connect.call_count == 1
