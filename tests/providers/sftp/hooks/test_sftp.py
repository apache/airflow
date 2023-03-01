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

import json
import os
import shutil
from io import StringIO
from unittest import mock

import paramiko
import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.session import provide_session


def generate_host_key(pkey: paramiko.PKey):
    key_fh = StringIO()
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
    @provide_session
    def update_connection(self, login, session=None):
        connection = session.query(Connection).filter(Connection.conn_id == "sftp_default").first()
        old_login = connection.login
        connection.login = login
        connection.extra = ""  # clear out extra so it doesn't look for a key file
        session.commit()
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
        os.mkfifo(os.path.join(temp_dir, TMP_DIR_FOR_TESTS, FIFO_FOR_TESTS))

        self.temp_dir = str(temp_dir)

        yield

        shutil.rmtree(os.path.join(temp_dir, TMP_DIR_FOR_TESTS))
        for file_name in [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS, LOG_FILE_FOR_TESTS]:
            os.remove(os.path.join(temp_dir, file_name))
        self.update_connection(self.old_login)

    def test_get_conn(self):
        output = self.hook.get_conn()
        assert isinstance(output, paramiko.SFTPClient)

    def test_close_conn(self):
        self.hook.conn = self.hook.get_conn()
        assert self.hook.conn is not None
        self.hook.close_conn()
        assert self.hook.conn is None

    def test_describe_directory(self):
        output = self.hook.describe_directory(self.temp_dir)
        assert TMP_DIR_FOR_TESTS in output

    def test_list_directory(self):
        output = self.hook.list_directory(path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, FIFO_FOR_TESTS]

    def test_mkdir(self):
        new_dir_name = "mk_dir"
        self.hook.mkdir(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_name in output
        # test the directory has default permissions to 777 - umask
        umask = 0o022
        output = self.hook.get_conn().lstat(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        assert output.st_mode & 0o777 == 0o777 - umask

    def test_create_and_delete_directory(self):
        new_dir_name = "new_dir"
        self.hook.create_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS))
        assert new_dir_name in output
        # test the directory has default permissions to 777
        umask = 0o022
        output = self.hook.get_conn().lstat(os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, new_dir_name))
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

    def test_get_mod_time(self):
        self.hook.store_file(
            remote_full_path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(self.temp_dir, TMP_FILE_FOR_TESTS),
        )
        output = self.hook.get_mod_time(
            path=os.path.join(self.temp_dir, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS)
        )
        assert len(output) == 14

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_default(self, get_connection):
        connection = Connection(login="login", host="host")
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_enabled(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_disabled(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_ciphers(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"ciphers": ["A", "B", "C"]}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.ciphers == ["A", "B", "C"]

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_disabled_for_all_but_true(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"no_host_key_check": "foo"}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_no_host_key_check_ignore(self, get_connection):
        connection = Connection(login="login", host="host", extra='{"ignore_hostkey_verification": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_default(self, get_connection):
        connection = Connection(login="login", host="host")
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is None

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key(self, get_connection):
        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps({"host_key": TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_with_type(self, get_connection):
        connection = Connection(
            login="login",
            host="host",
            extra=json.dumps({"host_key": "ssh-rsa " + TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_host_key_with_no_host_key_check(self, get_connection):
        connection = Connection(login="login", host="host", extra=json.dumps({"host_key": TEST_HOST_KEY}))
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is not None

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
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

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
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
        "path, exists",
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
        "path, prefix, delimiter, match",
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

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_connection_failure(self, mock_get_connection):
        connection = Connection(
            login="login",
            host="host",
        )
        mock_get_connection.return_value = connection
        with mock.patch.object(SFTPHook, "get_conn") as get_conn:
            type(get_conn.return_value).normalize = mock.PropertyMock(
                side_effect=Exception("Connection Error")
            )

            hook = SFTPHook()
            status, msg = hook.test_connection()
        assert status is False
        assert msg == "Connection Error"

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_connection_success(self, mock_get_connection):
        connection = Connection(
            login="login",
            host="host",
        )
        mock_get_connection.return_value = connection

        with mock.patch.object(SFTPHook, "get_conn") as get_conn:
            get_conn.return_value.pwd = "/home/someuser"
            hook = SFTPHook()
            status, msg = hook.test_connection()
        assert status is True
        assert msg == "Connection successfully tested"

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_deprecation_ftp_conn_id(self, mock_get_connection):
        connection = Connection(conn_id="ftp_default", login="login", host="host")
        mock_get_connection.return_value = connection
        # If `ftp_conn_id` is provided, it will be used but would show a deprecation warning.
        with pytest.warns(DeprecationWarning, match=r"Parameter `ftp_conn_id` is deprecated"):
            assert SFTPHook(ftp_conn_id="ftp_default").ssh_conn_id == "ftp_default"

        # If both are provided, ftp_conn_id  will be used but would show a deprecation warning.
        with pytest.warns(DeprecationWarning, match=r"Parameter `ftp_conn_id` is deprecated"):
            assert (
                SFTPHook(ftp_conn_id="ftp_default", ssh_conn_id="sftp_default").ssh_conn_id == "ftp_default"
            )

        # If `ssh_conn_id` is provided, it should use it for ssh_conn_id
        assert SFTPHook(ssh_conn_id="sftp_default").ssh_conn_id == "sftp_default"
        # Default is 'sftp_default
        assert SFTPHook().ssh_conn_id == "sftp_default"

    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
    def test_invalid_ssh_hook(self, mock_get_connection):
        with pytest.raises(AirflowException, match="ssh_hook must be an instance of SSHHook"):
            connection = Connection(conn_id="sftp_default", login="root", host="localhost")
            mock_get_connection.return_value = connection
            with pytest.warns(DeprecationWarning, match=r"Parameter `ssh_hook` is deprecated.*"):
                SFTPHook(ssh_hook="invalid_hook")

    @mock.patch("airflow.providers.ssh.hooks.ssh.SSHHook.get_connection")
    def test_valid_ssh_hook(self, mock_get_connection):
        connection = Connection(conn_id="sftp_test", login="root", host="localhost")
        mock_get_connection.return_value = connection
        with pytest.warns(DeprecationWarning, match=r"Parameter `ssh_hook` is deprecated.*"):
            hook = SFTPHook(ssh_hook=SSHHook(ssh_conn_id="sftp_test"))
        assert hook.ssh_conn_id == "sftp_test"
        assert isinstance(hook.get_conn(), paramiko.SFTPClient)

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
        assert output == [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS]

    def test_get_matched_files_with_different_pattern(self):
        output = self.hook.get_files_by_pattern(self.temp_dir, "*_file_*.txt")
        assert output == [ANOTHER_FILE_FOR_TESTS]
