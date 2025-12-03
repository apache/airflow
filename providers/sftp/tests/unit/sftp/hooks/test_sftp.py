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

import datetime
import json
import os
import shutil
from io import BytesIO, StringIO
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import paramiko
import pytest
from asyncssh import SFTPAttrs, SFTPNoSuchFile
from asyncssh.sftp import SFTPName
from paramiko.client import SSHClient
from paramiko.sftp_client import SFTPClient

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.sftp.hooks.sftp import SFTPHook, SFTPHookAsync


def generate_host_key(pkey: paramiko.PKey):
    with StringIO() as key_fh:
        pkey.write_private_key(key_fh)
        key_fh.seek(0)
        key_obj = paramiko.RSAKey(file_obj=key_fh)
    return key_obj.get_base64()


TMP_DIR_FOR_TESTS = "tests_sftp_hook_dir"
SUB_DIR = "sub_dir"
TMP_FILE_FOR_TESTS = "test_file.txt"
ANOTHER_FILE_FOR_TESTS = "test_file_1.txt"
LOG_FILE_FOR_TESTS = "test_log.log"
FIFO_FOR_TESTS = "test_fifo"

SFTP_CONNECTION_USER = "root"

TEST_PKEY = paramiko.RSAKey.generate(4096)
TEST_HOST_KEY = generate_host_key(pkey=TEST_PKEY)
TEST_KEY_FILE = "~/.ssh/id_rsa"


class TestSFTPHook:
    def update_connection(self, login):
        import os

        # Get the current connection from environment variable to find the old login
        old_connection = os.environ.get("AIRFLOW_CONN_SFTP_DEFAULT")
        old_login = "airflow"  # default fallback

        if old_connection:
            try:
                old_conn = Connection.from_json(old_connection)
                old_login = old_conn.login
            except Exception:
                pass

        # Set the connection as an environment variable
        new_connection = Connection(
            conn_id="sftp_default",
            conn_type="sftp",
            host="localhost",
            login=login,
            password="airflow",
            extra="",  # clear out extra so it doesn't look for a key file
        )
        os.environ[f"AIRFLOW_CONN_{new_connection.conn_id.upper()}"] = new_connection.as_json()

        return old_login

    def _create_additional_test_file(self, file_name):
        with open(os.path.join(self.temp_dir, file_name), "a") as file:
            file.write("Test file")

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, tmp_path_factory):
        """Define default connection during tests and create directory structure."""
        temp_dir = tmp_path_factory.mktemp("sftp-temp")
        self.old_login = self.update_connection(SFTP_CONNECTION_USER)
        self.hook = SFTPHook()
        os.makedirs(os.path.join(temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR))

        for file_name in [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS, LOG_FILE_FOR_TESTS]:
            with open(os.path.join(temp_dir, file_name), "a") as file:
                file.write("Test file")
        with open(os.path.join(temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR, TMP_FILE_FOR_TESTS), "a") as file:
            file.write("Test file")
        try:
            os.mkfifo(os.path.join(temp_dir, TMP_DIR_FOR_TESTS, FIFO_FOR_TESTS))
        except AttributeError:
            os.makedirs(os.path.join(temp_dir, TMP_DIR_FOR_TESTS, FIFO_FOR_TESTS))

        self.temp_dir = str(temp_dir)

        yield

        shutil.rmtree(os.path.join(temp_dir, TMP_DIR_FOR_TESTS))
        for file_name in [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS, LOG_FILE_FOR_TESTS]:
            os.remove(os.path.join(temp_dir, file_name))
        self.update_connection(self.old_login)

    def test_get_conn(self):
        output = self.hook.get_conn()
        assert isinstance(output, paramiko.SFTPClient)
        assert self.hook.conn is not None

    def test_close_conn(self):
        self.hook.conn = self.hook.get_conn()
        assert self.hook.conn is not None
        self.hook.close_conn()
        assert self.hook.conn is None

    def test_get_managed_conn(self):
        with self.hook.get_managed_conn() as conn1:
            assert isinstance(conn1, paramiko.SFTPClient)
            with self.hook.get_managed_conn() as conn2:
                assert conn1 == conn2
                assert self.hook.get_conn_count() == 2
            assert self.hook.get_conn_count() == 1
        assert self.hook.get_conn_count() == 0
        assert self.hook.conn is None

    @patch("paramiko.SSHClient")
    @patch("paramiko.ProxyCommand")
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_proxy_command_cache_invalidated_after_connection_closed(
        self, mock_get_connection, mock_proxy_command, mock_ssh_client
    ):
        """
        Assert that the ProxyCommand gets invalidated after the connection is closed
        """

        mock_connection = MagicMock()
        mock_connection.login = "user"
        mock_connection.password = None
        mock_connection.host = "example.com"
        mock_connection.port = 22
        mock_connection.extra = None
        mock_get_connection.return_value = mock_connection

        mock_sftp_client = MagicMock(spec=SFTPClient)
        mock_ssh_client.open_sftp.return_value = mock_sftp_client

        mock_transport = MagicMock()
        mock_ssh_client.return_value.get_transport.return_value = mock_transport
        mock_proxy_command.return_value = MagicMock()

        host_proxy_cmd = "ncat --proxy-auth proxy_user:**** --proxy proxy_host:port %h %p"
        prev_proxy_command = None

        hook = SFTPHook(
            remote_host="example.com",
            username="user",
            host_proxy_cmd=host_proxy_cmd,
        )

        with hook.get_managed_conn() as _:
            assert hasattr(self.hook, "host_proxy")
            prev_proxy_command = hook.host_proxy

        mock_proxy_command.return_value = MagicMock()

        assert prev_proxy_command != hook.host_proxy

    @patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn")
    def test_get_close_conn(self, mock_get_conn):
        mock_sftp_client = MagicMock(spec=SFTPClient)
        mock_ssh_client = MagicMock(spec=SSHClient)
        mock_ssh_client.open_sftp.return_value = mock_sftp_client
        mock_get_conn.return_value = mock_ssh_client

        with SFTPHook().get_managed_conn() as conn:
            assert conn == mock_sftp_client

        mock_sftp_client.close.assert_called_once()
        mock_ssh_client.close.assert_called_once()

    def test_describe_directory(self):
        output = self.hook.describe_directory(self.temp_dir)
        assert TMP_DIR_FOR_TESTS in output

    def test_list_directory(self):
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS]

    def test_list_directory_with_attr(self):
        output = self.hook.list_directory_with_attr(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        file_names = [f.filename for f in output]
        assert all(isinstance(f, paramiko.SFTPAttributes) for f in output)
        assert sorted(file_names) == [SUB_DIR, FIFO_FOR_TESTS]

    def test_mkdir(self):
        new_dir_name = "mk_dir"
        self.hook.mkdir(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_name in output
        # test the directory has default permissions to 777 - umask
        umask = 0o022
        with self.hook.get_managed_conn() as conn:
            output = conn.lstat(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
            assert output.st_mode & 0o777 == 0o777 - umask

    def test_create_and_delete_directory(self):
        new_dir_name = "new_dir"
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_name in output
        # test the directory has default permissions to 777
        umask = 0o022
        with self.hook.get_managed_conn() as conn:
            output = conn.lstat(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
            assert output.st_mode & 0o777 == 0o777 - umask
        # test directory already exists for code coverage, should not raise an exception
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        # test path already exists and is a file, should raise an exception
        with pytest.raises(AirflowException, match="already exists and is a file"):
            self.hook.create_directory(
                os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR, TMP_FILE_FOR_TESTS)
            )
        self.hook.delete_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_name not in output

    def test_create_and_delete_directories(self):
        base_dir = "base_dir"
        sub_dir = "sub_dir"
        new_dir_path = os.path.join(base_dir, sub_dir)
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_path))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert base_dir in output
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, base_dir))
        assert sub_dir in output
        self.hook.delete_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_path))
        self.hook.delete_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, base_dir))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_path not in output
        assert base_dir not in output

    def test_create_and_delete_directory_with_files(self):
        new_dir = "new_dir"
        sub_dir = "sub_dir"
        additional_file = "additional_file.txt"
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir in output
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir, sub_dir))
        self._create_additional_test_file(file_name=additional_file)
        self.hook.store_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir, additional_file),
            local_full_path=os.path.join(self.temp_dir, additional_file),
        )
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir))
        assert sub_dir in output
        assert additional_file in output
        self.hook.delete_directory(
            os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir), include_files=True
        )
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir not in output

    def test_store_retrieve_and_delete_file(self):
        self.hook.store_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(self.temp_dir, TMP_FILE_FOR_TESTS),
        )
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS, TMP_FILE_FOR_TESTS]
        retrieved_file_name = "retrieved.txt"
        self.hook.retrieve_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(self.temp_dir, retrieved_file_name),
        )
        assert retrieved_file_name in os.listdir(self.temp_dir)
        os.remove(os.path.join(self.temp_dir, retrieved_file_name))
        self.hook.delete_file(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS]

    def test_store_retrieve_and_delete_file_using_buffer(self):
        file_contents = BytesIO(b"Test file")
        self.hook.store_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=file_contents,
        )
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS, TMP_FILE_FOR_TESTS]
        retrieved_file_contents = BytesIO()
        self.hook.retrieve_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=retrieved_file_contents,
        )
        assert retrieved_file_contents.getvalue() == file_contents.getvalue()
        self.hook.delete_file(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS]

    def test_get_mod_time(self):
        self.hook.store_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(self.temp_dir, TMP_FILE_FOR_TESTS),
        )
        output = self.hook.get_mod_time(
            path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS)
        )
        assert len(output) == 14

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_default(self, get_connection):
        connection = Connection(login="login", host="host")
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_enabled(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_disabled(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_ciphers(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"ciphers": ["A", "B", "C"]}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.ciphers == ["A", "B", "C"]

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_disabled_for_all_but_true(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": "foo"}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_ignore(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"ignore_hostkey_verification": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_default(self, get_connection):
        connection = Connection(login="login", host="host")
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is None

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key(self, get_connection):
        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps({"host_key": TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_with_type(self, get_connection):
        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps({"host_key": "ssh-rsa " + TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_with_no_host_key_check(self, get_connection):
        connection = Connection(login="login", host="host", extra=json.dumps({"host_key": TEST_HOST_KEY}))
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is not None

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_key_content_as_str(self, get_connection):
        file_obj = StringIO()
        TEST_PKEY.write_private_key(file_obj)
        file_obj.seek(0)
        key_content_str = file_obj.read()

        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps(
                {
                    "private_key": key_content_str,
                }
            ),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.pkey == TEST_PKEY
        assert hook.key_file is None

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_key_file(self, get_connection):
        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps(
                {
                    "key_file": TEST_KEY_FILE,
                }
            ),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.key_file == TEST_KEY_FILE

    @pytest.mark.parametrize(
        ("path", "exists"),
        [
            (TMP_DIR_FOR_TESTS, True),
            (TMP_FILE_FOR_TESTS, True),
            (TMP_DIR_FOR_TESTS + "abc", False),
            (TMP_DIR_FOR_TESTS + "/abc", False),
        ],
    )
    def test_path_exists(self, path, exists):
        path = os.path.join(self.temp_dir, path)
        result = self.hook.path_exists(path)
        assert result == exists

    @pytest.mark.parametrize(
        ("path", "prefix", "delimiter", "match"),
        [
            ("test/path/file.bin", None, None, True),
            ("test/path/file.bin", "test", None, True),
            ("test/path/file.bin", "test/", None, True),
            ("test/path/file.bin", None, "bin", True),
            ("test/path/file.bin", "test", "bin", True),
            ("test/path/file.bin", "test/", "file.bin", True),
            ("test/path/file.bin", None, "file.bin", True),
            ("test/path/file.bin", "diff", None, False),
            ("test/path/file.bin", "test//", None, False),
            ("test/path/file.bin", None, ".txt", False),
            ("test/path/file.bin", "diff", ".txt", False),
        ],
    )
    def test_path_match(self, path, prefix, delimiter, match):
        result = self.hook._is_path_match(path=path, prefix=prefix, delimiter=delimiter)
        assert result == match

    def test_get_tree_map(self):
        tree_map = self.hook.get_tree_map(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        files, dirs, unknowns = tree_map

        assert files == [os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR, TMP_FILE_FOR_TESTS)]
        assert dirs == [os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR)]
        assert unknowns == [os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, FIFO_FOR_TESTS)]

    @patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn")
    def test_connection_failure(self, mock_get_conn):
        mock_ssh_client = MagicMock(spec=SSHClient)
        type(mock_ssh_client.open_sftp.return_value).normalize = PropertyMock(
            side_effect=Exception("Connection Error")
        )
        mock_get_conn.return_value = mock_ssh_client

        hook = SFTPHook()
        status, msg = hook.test_connection()

        assert status is False
        assert msg == "Connection Error"

    @pytest.mark.parametrize(
        "test_connection_side_effect",
        [
            (lambda arg: (True, "Connection successfully tested")),
            (lambda arg: RuntimeError("Test connection failed")),
        ],
    )
    @patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_conn")
    def test_context_manager(self, mock_get_conn, test_connection_side_effect):
        mock_sftp_client = MagicMock(spec=SFTPClient)
        mock_ssh_client = MagicMock(spec=SSHClient)
        mock_ssh_client.open_sftp.return_value = mock_sftp_client
        mock_get_conn.return_value = mock_ssh_client

        type(mock_sftp_client.normalize.return_value).normalize = PropertyMock(
            side_effect=test_connection_side_effect
        )

        hook = SFTPHook()
        if isinstance(test_connection_side_effect, RuntimeError):
            with pytest.raises(RuntimeError, match="Test connection failed"):
                hook.test_connection()
        else:
            status, msg = hook.test_connection()

            assert status is True
            assert msg == "Connection successfully tested"

        mock_ssh_client.open_sftp.assert_called_once()
        mock_sftp_client.close.assert_called()

    def test_get_suffix_pattern_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "*.txt")
        # In CI files might have different name, so we check that file found rather than actual name
        assert output, TMP_FILE_FOR_TESTS

    def test_get_prefix_pattern_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "test*")
        # In CI files might have different name, so we check that file found rather than actual name
        assert output, TMP_FILE_FOR_TESTS

    def test_get_pattern_not_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "*.text")
        assert output == ""

    def test_get_several_pattern_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "*.log")
        assert output == LOG_FILE_FOR_TESTS

    def test_get_first_pattern_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "test_*.txt")
        assert output == TMP_FILE_FOR_TESTS

    def test_get_middle_pattern_match(self):
        output = self.hook.get_file_by_pattern(self.temp_dir, "*_file_*.txt")
        assert output == ANOTHER_FILE_FOR_TESTS

    def test_get_none_matched_files(self):
        output = self.hook.get_files_by_pattern(self.temp_dir, "*.text")
        assert output == []

    def test_get_matched_files_several_pattern(self):
        output = self.hook.get_files_by_pattern(self.temp_dir, "*.log")
        assert output == [LOG_FILE_FOR_TESTS]

    def test_get_all_matched_files(self):
        output = self.hook.get_files_by_pattern(self.temp_dir, "test_*.txt")
        assert sorted(output) == [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS]

    def test_get_matched_files_with_different_pattern(self):
        output = self.hook.get_files_by_pattern(self.temp_dir, "*_file_*.txt")
        assert output == [ANOTHER_FILE_FOR_TESTS]

    def test_store_and_retrieve_directory(self):
        stored_dir_name = "stored_dir"
        self.hook.store_directory(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name),
            local_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR),
        )
        output = self.hook.list_directory(
            path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name)
        )
        assert output == [TMP_FILE_FOR_TESTS]
        retrieved_dir_name = "retrieved_dir"
        self.hook.retrieve_directory(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name),
            local_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, retrieved_dir_name),
        )
        assert retrieved_dir_name in os.listdir(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))

    def test_store_and_retrieve_directory_concurrently(self):
        stored_dir_name = "stored_dir"
        self.hook.store_directory_concurrently(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name),
            local_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, SUB_DIR),
        )
        output = self.hook.list_directory(
            path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name)
        )
        assert output == [TMP_FILE_FOR_TESTS]
        retrieved_dir_name = "retrieved_dir"
        self.hook.retrieve_directory_concurrently(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, stored_dir_name),
            local_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, retrieved_dir_name),
        )
        assert retrieved_dir_name in os.listdir(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))

    @patch("paramiko.SSHClient")
    @patch("paramiko.ProxyCommand")
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_sftp_hook_with_proxy_command(self, mock_get_connection, mock_proxy_command, mock_ssh_client):
        # Mock the connection to not have a password
        mock_connection = MagicMock()
        mock_connection.login = "user"
        mock_connection.password = None
        mock_connection.host = "example.com"
        mock_connection.port = 22
        mock_connection.extra = None
        mock_get_connection.return_value = mock_connection

        mock_sftp_client = MagicMock(spec=SFTPClient)
        mock_ssh_client.open_sftp.return_value = mock_sftp_client

        mock_transport = MagicMock()
        mock_ssh_client.return_value.get_transport.return_value = mock_transport
        mock_proxy_command.return_value = MagicMock()

        host_proxy_cmd = "ncat --proxy-auth proxy_user:**** --proxy proxy_host:port %h %p"

        hook = SFTPHook(
            remote_host="example.com",
            username="user",
            host_proxy_cmd=host_proxy_cmd,
        )

        with hook.get_managed_conn():
            mock_proxy_command.assert_called_once_with(host_proxy_cmd)
            mock_ssh_client.return_value.connect.assert_called_once_with(
                hostname="example.com",
                username="user",
                timeout=None,
                compress=True,
                port=22,
                sock=mock_proxy_command.return_value,
                look_for_keys=True,
                banner_timeout=30.0,
                auth_timeout=None,
            )


class MockSFTPClient:
    def __init__(self):
        pass

    async def listdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        return ["..", ".", "file"]

    async def readdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        return [SFTPName(".."), SFTPName("."), SFTPName("file")]

    async def stat(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("No files matching")
        sftp_obj = SFTPAttrs()
        sftp_obj.mtime = 1667302566
        return sftp_obj


class MockSSHClient:
    def __init__(self):
        pass

    async def start_sftp_client(self):
        return MockSFTPClient()


class MockAirflowConnection:
    def __init__(self, known_hosts="~/.ssh/known_hosts"):
        self.host = "localhost"
        self.port = 22
        self.login = "username"
        self.password = "password"
        self.extra = """
        {
            "key_file": "~/keys/my_key",
            "known_hosts": "unused",
            "passphrase": "mypassphrase"
        }
        """
        self.extra_dejson = {
            "key_file": "~/keys/my_key",
            "known_hosts": known_hosts,
            "passphrase": "mypassphrase",
        }


class MockAirflowConnectionWithHostKey:
    def __init__(self, host_key: str | None = None, no_host_key_check: bool = False, port: int = 22):
        self.host = "localhost"
        self.port = port
        self.login = "username"
        self.password = "password"
        self.extra = f'{{ "no_host_key_check": {no_host_key_check}, "host_key": {host_key} }}'
        self.extra_dejson = {
            "no_host_key_check": no_host_key_check,
            "host_key": host_key,
            "key_file": "~/keys/my_key",
            "private_key": "~/keys/my_key",
        }


class MockAirflowConnectionWithPrivate:
    def __init__(self):
        self.host = "localhost"
        self.port = 22
        self.login = "username"
        self.password = "password"
        self.extra = """
                {
                    "private_key": "~/keys/my_key",
                    "known_hosts": "unused",
                    "passphrase": "mypassphrase"
                }
                """
        self.extra_dejson = {
            "private_key": "~/keys/my_key",
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }


class TestSFTPHookAsync:
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building_known_hosts_none(
        self, mock_get_connection, mock_connect, caplog
    ):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnection(known_hosts="None")

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": "~/keys/my_key",
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }

        mock_connect.assert_called_with(**expected_connection_details)

    @pytest.mark.parametrize(
        ("mock_port", "mock_host_key"),
        [
            (22, "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"),
            (2222, "AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"),
            (
                2222,
                "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBDDsXFe87LsBA1Hfi+mtw"
                "/EoQkv8bXVtfOwdMP1ETpHVsYpm5QG/7tsLlKdE8h6EoV/OFw7XQtoibNZp/l5ABjE=",
            ),
        ],
    )
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("asyncssh.import_private_key")
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_with_host_key(
        self,
        mock_get_connection,
        mock_import_private_key,
        mock_connect,
        mock_port,
        mock_host_key,
    ):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed to paramiko client for validating given host key.
        """
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(
            host_key=mock_host_key, no_host_key_check=False, port=mock_port
        )

        hook = SFTPHookAsync()
        await hook._get_conn()

        assert hook.known_hosts == f"localhost {mock_host_key}".encode()

    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_raises_valuerror(
        self, mock_get_connection, mock_connect
    ):
        """
        Assert that when both host_key and no_host_key_check are set, a valuerror is raised because no_host_key_check
        should be unset when host_key is given and the host_key needs to be validated.
        """
        host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(
            host_key=host_key, no_host_key_check=True
        )

        hook = SFTPHookAsync()
        with pytest.raises(ValueError, match="Host key check was skipped, but `host_key` value was given"):
            await hook._get_conn()

    @patch("paramiko.SSHClient.connect")
    @patch("asyncssh.import_private_key")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_no_host_key_check_set_logs_warning(
        self, mock_get_connection, mock_connect, mock_import_pkey, mock_ssh_connect, caplog
    ):
        """Assert that when no_host_key_check is set, a warning is logged for MITM attacks possibility."""
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(no_host_key_check=True)

        hook = SFTPHookAsync()
        await hook._get_conn()
        assert "No Host Key Verification. This won't protect against Man-In-The-Middle attacks" in caplog.text

    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building(self, mock_get_connection, mock_connect):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnection()

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": "~/keys/my_key",
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }

        mock_connect.assert_called_with(**expected_connection_details)

    @pytest.mark.asyncio
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("asyncssh.import_private_key")
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    async def test_connection_private(self, mock_get_connection, mock_import_private_key, mock_connect):
        """
        Assert that connection details with private key passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnectionWithPrivate()
        mock_import_private_key.return_value = "test"

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": ["test"],
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }

        mock_connect.assert_called_with(**expected_connection_details)

    @pytest.mark.asyncio
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    async def test_connection_port_default_to_22(self, mock_get_connection, mock_connect):
        from unittest.mock import Mock, call

        mock_get_connection.return_value = Mock(
            host="localhost",
            port=None,
            login="username",
            password="password",
            extra="{}",
            extra_dejson={},
        )

        hook = SFTPHookAsync()
        await hook._get_conn()
        assert mock_connect.mock_calls == [
            call(
                host="localhost",
                # Even if the port is not specified in conn_config, it should still default to 22.
                # This behavior is consistent with STPHook.
                port=22,
                username="username",
                password="password",
                known_hosts=None,
            ),
        ]

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_list_directory_path_does_not_exist(self, mock_hook_get_conn):
        """
        Assert that AirflowException is raised when path does not exist on SFTP server
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()

        hook = SFTPHookAsync()

        expected_files = None
        files = await hook.list_directory(path="/path/does_not/exist/")
        assert files == expected_files
        mock_hook_get_conn.return_value.__aexit__.assert_called()

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_read_directory_path_does_not_exist(self, mock_hook_get_conn):
        """
        Assert that AirflowException is raised when path does not exist on SFTP server
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        expected_files = None
        files = await hook.read_directory(path="/path/does_not/exist/")
        assert files == expected_files
        mock_hook_get_conn.return_value.__aexit__.assert_called()

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_list_directory_path_has_files(self, mock_hook_get_conn):
        """
        Assert that file list is returned when path exists on SFTP server
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        expected_files = ["..", ".", "file"]
        files = await hook.list_directory(path="/path/exists/")
        assert sorted(files) == sorted(expected_files)
        mock_hook_get_conn.return_value.__aexit__.assert_called()

    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_get_file_by_pattern_with_match(self, mock_hook_get_conn):
        """
        Assert that filename is returned when file pattern is matched on SFTP server
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        files = await hook.get_files_and_attrs_by_pattern(path="/path/exists/", fnmatch_pattern="file")

        assert len(files) == 1
        assert files[0].filename == "file"
        mock_hook_get_conn.return_value.__aexit__.assert_called()

    @pytest.mark.asyncio
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_mod_time(self, mock_hook_get_conn):
        """
        Assert that file attribute and return the modified time of the file
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        mod_time = await hook.get_mod_time("/path/exists/file")
        expected_value = datetime.datetime.fromtimestamp(1667302566).strftime("%Y%m%d%H%M%S")
        assert mod_time == expected_value

    @pytest.mark.asyncio
    @patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_mod_time_exception(self, mock_hook_get_conn):
        """
        Assert that get_mod_time raise exception when file does not exist
        """
        mock_hook_get_conn.return_value.__aenter__.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        with pytest.raises(AirflowException) as exc:
            await hook.get_mod_time("/path/does_not/exist/")
        assert str(exc.value) == "No files matching"
