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

from unittest.mock import patch

import pytest

pytest.importorskip("sshfs")

TEST_CONN_ID = "sftp_test_conn"


@pytest.fixture(scope="module", autouse=True)
def _setup_connections():
    with pytest.MonkeyPatch.context() as mp_ctx:
        mp_ctx.setenv(
            f"AIRFLOW_CONN_{TEST_CONN_ID}".upper(),
            "sftp://testuser:testpass@testhost:2222",
        )
        yield


class TestSftpFilesystem:
    def test_schemes(self):
        from airflow.providers.sftp.fs.sftp import schemes

        assert "sftp" in schemes
        assert "ssh" in schemes

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_with_connection(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        get_fs(conn_id=TEST_CONN_ID)

        mock_sshfs.assert_called_once()
        call_kwargs = mock_sshfs.call_args.kwargs
        assert call_kwargs["host"] == "testhost"
        assert call_kwargs["port"] == 2222
        assert call_kwargs["username"] == "testuser"
        assert call_kwargs["password"] == "testpass"

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_without_connection(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        # When conn_id is None, storage_options are passed directly to SSHFileSystem
        storage_options = {"host": "manual-host", "username": "manual-user"}
        get_fs(conn_id=None, storage_options=storage_options)

        mock_sshfs.assert_called_once()
        call_kwargs = mock_sshfs.call_args.kwargs
        assert call_kwargs["host"] == "manual-host"
        assert call_kwargs["username"] == "manual-user"

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_storage_options_merge(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        storage_options = {"custom_option": "custom_value"}
        get_fs(conn_id=TEST_CONN_ID, storage_options=storage_options)

        mock_sshfs.assert_called_once()
        call_kwargs = mock_sshfs.call_args.kwargs
        assert call_kwargs["custom_option"] == "custom_value"
        assert call_kwargs["host"] == "testhost"

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_storage_options_override(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        storage_options = {"port": 3333}
        get_fs(conn_id=TEST_CONN_ID, storage_options=storage_options)

        mock_sshfs.assert_called_once()
        call_kwargs = mock_sshfs.call_args.kwargs
        assert call_kwargs["port"] == 3333

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_with_key_file(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_KEY_FILE",
                "sftp://testuser@testhost?key_file=%2Fpath%2Fto%2Fkey",
            )

            get_fs(conn_id="sftp_key_file")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert call_kwargs["client_keys"] == ["/path/to/key"]

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_with_private_key(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_PRIVATE_KEY",
                "sftp://testuser@testhost?private_key=PRIVATE_KEY_CONTENT&private_key_passphrase=secret",
            )

            get_fs(conn_id="sftp_private_key")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert call_kwargs["client_keys"] == ["PRIVATE_KEY_CONTENT"]
            assert call_kwargs["passphrase"] == "secret"

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_with_private_key_no_passphrase(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_PRIVATE_KEY_NO_PASS",
                "sftp://testuser@testhost?private_key=PRIVATE_KEY_CONTENT",
            )

            get_fs(conn_id="sftp_private_key_no_pass")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert call_kwargs["client_keys"] == ["PRIVATE_KEY_CONTENT"]
            assert "passphrase" not in call_kwargs

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_with_no_host_key_check(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_NO_HOST_CHECK",
                "sftp://testuser@testhost?no_host_key_check=true",
            )

            get_fs(conn_id="sftp_no_host_check")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert call_kwargs["known_hosts"] is None

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_default_port(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_DEFAULT_PORT",
                "sftp://testuser@testhost",
            )

            get_fs(conn_id="sftp_default_port")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert call_kwargs["port"] == 22

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_without_password(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_NO_PASSWORD",
                "sftp://testuser@testhost",
            )

            get_fs(conn_id="sftp_no_password")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert "password" not in call_kwargs

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_host_key_check_enabled_by_default(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_HOST_CHECK_DEFAULT",
                "sftp://testuser@testhost",
            )

            get_fs(conn_id="sftp_host_check_default")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert "known_hosts" not in call_kwargs

    @patch("sshfs.SSHFileSystem", autospec=True)
    def test_get_fs_host_key_check_explicit_false(self, mock_sshfs):
        from airflow.providers.sftp.fs.sftp import get_fs

        with pytest.MonkeyPatch.context() as mp_ctx:
            mp_ctx.setenv(
                "AIRFLOW_CONN_SFTP_HOST_CHECK_FALSE",
                "sftp://testuser@testhost?no_host_key_check=false",
            )

            get_fs(conn_id="sftp_host_check_false")

            mock_sshfs.assert_called_once()
            call_kwargs = mock_sshfs.call_args.kwargs
            assert "known_hosts" not in call_kwargs
