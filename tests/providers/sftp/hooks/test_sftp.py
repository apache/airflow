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
import json
import os
import shutil
import unittest
from io import StringIO
from unittest import mock

import paramiko
import pysftp
from parameterized import parameterized

from airflow.models import Connection
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.session import provide_session


def generate_host_key(pkey: paramiko.PKey):
    key_fh = StringIO()
    pkey.write_private_key(key_fh)
    key_fh.seek(0)
    key_obj = paramiko.RSAKey(file_obj=key_fh)
    return key_obj.get_base64()


TMP_PATH = '/tmp'
TMP_DIR_FOR_TESTS = 'tests_sftp_hook_dir'
SUB_DIR = "sub_dir"
TMP_FILE_FOR_TESTS = 'test_file.txt'
ANOTHER_FILE_FOR_TESTS = 'test_file_1.txt'
LOG_FILE_FOR_TESTS = 'test_log.log'

SFTP_CONNECTION_USER = "root"

TEST_PKEY = paramiko.RSAKey.generate(4096)
TEST_HOST_KEY = generate_host_key(pkey=TEST_PKEY)
TEST_KEY_FILE = "~/.ssh/id_rsa"


class TestSFTPHook(unittest.TestCase):
    @provide_session
    def update_connection(self, login, session=None):
        connection = session.query(Connection).filter(Connection.conn_id == "sftp_default").first()
        old_login = connection.login
        connection.login = login
        session.commit()
        return old_login

    def _create_additional_test_file(self, file_name):
        with open(os.path.join(TMP_PATH, file_name), 'a') as file:
            file.write('Test file')

    def setUp(self):
        self.old_login = self.update_connection(SFTP_CONNECTION_USER)
        self.hook = SFTPHook()
        os.makedirs(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, SUB_DIR))

        for file_name in [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS, LOG_FILE_FOR_TESTS]:
            with open(os.path.join(TMP_PATH, file_name), 'a') as file:
                file.write('Test file')
        with open(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, SUB_DIR, TMP_FILE_FOR_TESTS), 'a') as file:
            file.write('Test file')

    def test_get_conn(self):
        output = self.hook.get_conn()
        assert isinstance(output, pysftp.Connection)

    def test_close_conn(self):
        self.hook.conn = self.hook.get_conn()
        assert self.hook.conn is not None
        self.hook.close_conn()
        assert self.hook.conn is None

    def test_describe_directory(self):
        output = self.hook.describe_directory(TMP_PATH)
        assert TMP_DIR_FOR_TESTS in output

    def test_list_directory(self):
        output = self.hook.list_directory(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR]

    def test_create_and_delete_directory(self):
        new_dir_name = 'new_dir'
        self.hook.create_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert new_dir_name in output
        self.hook.delete_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert new_dir_name not in output

    def test_create_and_delete_directories(self):
        base_dir = "base_dir"
        sub_dir = "sub_dir"
        new_dir_path = os.path.join(base_dir, sub_dir)
        self.hook.create_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_path))
        output = self.hook.describe_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert base_dir in output
        output = self.hook.describe_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, base_dir))
        assert sub_dir in output
        self.hook.delete_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_path))
        self.hook.delete_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, base_dir))
        output = self.hook.describe_directory(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert new_dir_path not in output
        assert base_dir not in output

    def test_store_retrieve_and_delete_file(self):
        self.hook.store_file(
            remote_full_path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS),
        )
        output = self.hook.list_directory(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR, TMP_FILE_FOR_TESTS]
        retrieved_file_name = 'retrieved.txt'
        self.hook.retrieve_file(
            remote_full_path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, retrieved_file_name),
        )
        assert retrieved_file_name in os.listdir(TMP_PATH)
        os.remove(os.path.join(TMP_PATH, retrieved_file_name))
        self.hook.delete_file(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        output = self.hook.list_directory(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        assert output == [SUB_DIR]

    def test_get_mod_time(self):
        self.hook.store_file(
            remote_full_path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS),
        )
        output = self.hook.get_mod_time(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        assert len(output) == 14

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_default(self, get_connection):
        connection = Connection(login='login', host='host')
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_enabled(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"no_host_key_check": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_disabled(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"no_host_key_check": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_ciphers(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"ciphers": ["A", "B", "C"]}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.ciphers == ["A", "B", "C"]

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_disabled_for_all_but_true(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"no_host_key_check": "foo"}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_ignore(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"ignore_hostkey_verification": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is True

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_no_host_key_check_no_ignore(self, get_connection):
        connection = Connection(login='login', host='host', extra='{"ignore_hostkey_verification": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.no_host_key_check is False

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_host_key_default(self, get_connection):
        connection = Connection(login='login', host='host')
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is None

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_host_key(self, get_connection):
        connection = Connection(
            login='login',
            host='host',
            extra=json.dumps({"host_key": TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_host_key_with_type(self, get_connection):
        connection = Connection(
            login='login',
            host='host',
            extra=json.dumps({"host_key": "ssh-rsa " + TEST_HOST_KEY}),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key.get_base64() == TEST_HOST_KEY

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_host_key_with_no_host_key_check(self, get_connection):
        connection = Connection(login='login', host='host', extra=json.dumps({"host_key": TEST_HOST_KEY}))
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.host_key is not None

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_key_content_as_str(self, get_connection):
        file_obj = StringIO()
        TEST_PKEY.write_private_key(file_obj)
        file_obj.seek(0)
        key_content_str = file_obj.read()

        connection = Connection(
            login='login',
            host='host',
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

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_key_file(self, get_connection):
        connection = Connection(
            login='login',
            host='host',
            extra=json.dumps(
                {
                    "key_file": TEST_KEY_FILE,
                }
            ),
        )
        get_connection.return_value = connection
        hook = SFTPHook()
        assert hook.key_file == TEST_KEY_FILE

    @parameterized.expand(
        [
            (os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS), True),
            (os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS), True),
            (os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS + "abc"), False),
            (os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, "abc"), False),
        ]
    )
    def test_path_exists(self, path, exists):
        result = self.hook.path_exists(path)
        assert result == exists

    @parameterized.expand(
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
        ]
    )
    def test_path_match(self, path, prefix, delimiter, match):
        result = self.hook._is_path_match(path=path, prefix=prefix, delimiter=delimiter)
        assert result == match

    def test_get_tree_map(self):
        tree_map = self.hook.get_tree_map(path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        files, dirs, unknowns = tree_map

        assert files == [os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, SUB_DIR, TMP_FILE_FOR_TESTS)]
        assert dirs == [os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS, SUB_DIR)]
        assert unknowns == []

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_connection_failure(self, mock_get_connection):
        connection = Connection(
            login='login',
            host='host',
        )
        mock_get_connection.return_value = connection
        with mock.patch.object(SFTPHook, 'get_conn') as get_conn:
            type(get_conn.return_value).pwd = mock.PropertyMock(side_effect=Exception('Connection Error'))

            hook = SFTPHook()
            status, msg = hook.test_connection()
        assert status is False
        assert msg == 'Connection Error'

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_connection_success(self, mock_get_connection):
        connection = Connection(
            login='login',
            host='host',
        )
        mock_get_connection.return_value = connection

        with mock.patch.object(SFTPHook, 'get_conn') as get_conn:
            get_conn.return_value.pwd = '/home/someuser'
            hook = SFTPHook()
            status, msg = hook.test_connection()
        assert status is True
        assert msg == 'Connection successfully tested'

    @mock.patch('airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection')
    def test_deprecation_ftp_conn_id(self, mock_get_connection):
        connection = Connection(conn_id='ftp_default', login='login', host='host')
        mock_get_connection.return_value = connection
        # If `ftp_conn_id` is provided, it will be used but would show a deprecation warning.
        with self.assertWarnsRegex(DeprecationWarning, "Parameter `ftp_conn_id` is deprecated"):
            assert SFTPHook(ftp_conn_id='ftp_default').ssh_conn_id == 'ftp_default'

        # If both are provided, ftp_conn_id  will be used but would show a deprecation warning.
        with self.assertWarnsRegex(DeprecationWarning, "Parameter `ftp_conn_id` is deprecated"):
            assert (
                SFTPHook(ftp_conn_id='ftp_default', ssh_conn_id='sftp_default').ssh_conn_id == 'ftp_default'
            )

        # If `ssh_conn_id` is provided, it should use it for ssh_conn_id
        assert SFTPHook(ssh_conn_id='sftp_default').ssh_conn_id == 'sftp_default'
        # Default is 'sftp_default
        assert SFTPHook().ssh_conn_id == 'sftp_default'

    def test_get_suffix_pattern_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "*.txt")
        self.assertTrue(output, TMP_FILE_FOR_TESTS)

    def test_get_prefix_pattern_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "test*")
        self.assertTrue(output, TMP_FILE_FOR_TESTS)

    def test_get_pattern_not_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "*.text")
        self.assertFalse(output)

    def test_get_several_pattern_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "*.log")
        self.assertEqual(LOG_FILE_FOR_TESTS, output)

    def test_get_first_pattern_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "test_*.txt")
        self.assertEqual(TMP_FILE_FOR_TESTS, output)

    def test_get_middle_pattern_match(self):
        output = self.hook.get_file_by_pattern(TMP_PATH, "*_file_*.txt")
        self.assertEqual(ANOTHER_FILE_FOR_TESTS, output)

    def tearDown(self):
        shutil.rmtree(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        for file_name in [TMP_FILE_FOR_TESTS, ANOTHER_FILE_FOR_TESTS, LOG_FILE_FOR_TESTS]:
            os.remove(os.path.join(TMP_PATH, file_name))
        self.update_connection(self.old_login)
