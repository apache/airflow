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

from airflow.providers.ftp.fs.ftp import _ActiveModeFTPFileSystem, get_fs, schemes

TEST_CONN_ID = "ftp_fs_test_conn"


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

    @mock.patch("airflow.providers.ftp.fs.ftp.FTPFileSystem", autospec=True)
    def test_get_fs_with_connection(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost:2121?encoding=cp1251")

        get_fs(conn_id=TEST_CONN_ID)

        mock_ftp_fs.assert_called_once_with(
            host="testhost", port=2121, username="testuser", password="testpass", encoding="cp1251"
        )

    @mock.patch("airflow.providers.ftp.fs.ftp.FTPFileSystem", autospec=True)
    def test_get_fs_defaults_to_port_21(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost")

        get_fs(conn_id=TEST_CONN_ID)

        mock_ftp_fs.assert_called_once_with(
            host="testhost", port=21, username="testuser", password="testpass"
        )

    @mock.patch("airflow.providers.ftp.fs.ftp.FTPFileSystem", autospec=True)
    def test_get_fs_without_connection(self, mock_ftp_fs):
        get_fs(conn_id=None, storage_options={"host": "manual-host", "username": "manual-user"})

        mock_ftp_fs.assert_called_once_with(host="manual-host", username="manual-user")

    @mock.patch("airflow.providers.ftp.fs.ftp.FTPFileSystem", autospec=True)
    def test_get_fs_storage_options_override_connection(self, mock_ftp_fs, ftp_connection):
        ftp_connection("ftp://testuser:testpass@testhost")

        get_fs(conn_id=TEST_CONN_ID, storage_options={"port": 2121, "timeout": 5})

        mock_ftp_fs.assert_called_once_with(
            host="testhost", port=2121, username="testuser", password="testpass", timeout=5
        )

    @pytest.mark.parametrize(
        ("query", "expected_cls"),
        [
            pytest.param("", FTPFileSystem, id="passive-by-default"),
            pytest.param("?passive=true", FTPFileSystem, id="passive"),
            pytest.param("?passive=false", _ActiveModeFTPFileSystem, id="active"),
        ],
    )
    @pytest.mark.usefixtures("mock_connect")
    def test_get_fs_follows_passive_extra(self, query, expected_cls, ftp_connection):
        ftp_connection(f"ftp://testuser:testpass@testhost{query}")

        fs = get_fs(conn_id=TEST_CONN_ID, storage_options={"skip_instance_cache": True})

        assert type(fs) is expected_cls

    def test_active_mode_turns_passive_off_on_every_connect(self, mock_connect):
        fs = _ActiveModeFTPFileSystem(host="testhost", skip_instance_cache=True)
        fs.ftp.set_pasv.assert_called_once_with(False)

        # A read that is cut short reconnects through ``_connect``.
        fs._connect()

        fs.ftp.set_pasv.assert_called_once_with(False)
        assert mock_connect.call_count == 2
