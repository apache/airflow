# -*- coding: utf-8 -*-
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

import os
import shutil
import unittest
from unittest import mock

import pysftp

from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import Connection
from airflow.utils.db import provide_session

TMP_PATH = '/tmp'
TMP_DIR_FOR_TESTS = 'tests_sftp_hook_dir'
TMP_FILE_FOR_TESTS = 'test_file.txt'

SFTP_CONNECTION_USER = "root"


class TestSFTPHook(unittest.TestCase):

    @provide_session
    def update_connection(self, login, session=None):
        connection = (session.query(Connection).
                      filter(Connection.conn_id == "sftp_default")
                      .first())
        old_login = connection.login
        connection.login = login
        session.commit()
        return old_login

    def setUp(self):
        self.old_login = self.update_connection(SFTP_CONNECTION_USER)
        self.hook = SFTPHook()
        os.makedirs(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        with open(os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS), 'a') as file:
            file.write('Test file')

    def test_get_conn(self):
        output = self.hook.get_conn()
        self.assertEqual(type(output), pysftp.Connection)

    def test_close_conn(self):
        self.hook.conn = self.hook.get_conn()
        self.assertTrue(self.hook.conn is not None)
        self.hook.close_conn()
        self.assertTrue(self.hook.conn is None)

    def test_describe_directory(self):
        output = self.hook.describe_directory(TMP_PATH)
        self.assertTrue(TMP_DIR_FOR_TESTS in output)

    def test_list_directory(self):
        output = self.hook.list_directory(
            path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        self.assertEqual(output, [])

    def test_create_and_delete_directory(self):
        new_dir_name = 'new_dir'
        self.hook.create_directory(os.path.join(
            TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(
            os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        self.assertTrue(new_dir_name in output)
        self.hook.delete_directory(os.path.join(
            TMP_PATH, TMP_DIR_FOR_TESTS, new_dir_name))
        output = self.hook.describe_directory(
            os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        self.assertTrue(new_dir_name not in output)

    def test_store_retrieve_and_delete_file(self):
        self.hook.store_file(
            remote_full_path=os.path.join(
                TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS)
        )
        output = self.hook.list_directory(
            path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        self.assertEqual(output, [TMP_FILE_FOR_TESTS])
        retrieved_file_name = 'retrieved.txt'
        self.hook.retrieve_file(
            remote_full_path=os.path.join(
                TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, retrieved_file_name)
        )
        self.assertTrue(retrieved_file_name in os.listdir(TMP_PATH))
        os.remove(os.path.join(TMP_PATH, retrieved_file_name))
        self.hook.delete_file(path=os.path.join(
            TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        output = self.hook.list_directory(
            path=os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        self.assertEqual(output, [])

    def test_get_mod_time(self):
        self.hook.store_file(
            remote_full_path=os.path.join(
                TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS),
            local_full_path=os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS)
        )
        output = self.hook.get_mod_time(path=os.path.join(
            TMP_PATH, TMP_DIR_FOR_TESTS, TMP_FILE_FOR_TESTS))
        self.assertEqual(len(output), 14)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_default(self, get_connection):
        connection = Connection(login='login', host='host')
        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, False)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_enabled(self, get_connection):
        connection = Connection(
            login='login', host='host',
            extra='{"no_host_key_check": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, True)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_disabled(self, get_connection):
        connection = Connection(
            login='login', host='host',
            extra='{"no_host_key_check": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, False)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_disabled_for_all_but_true(self, get_connection):
        connection = Connection(
            login='login', host='host',
            extra='{"no_host_key_check": "foo"}')

        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, False)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_ignore(self, get_connection):
        connection = Connection(
            login='login', host='host',
            extra='{"ignore_hostkey_verification": true}')

        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, True)

    @mock.patch('airflow.contrib.hooks.sftp_hook.SFTPHook.get_connection')
    def test_no_host_key_check_no_ignore(self, get_connection):
        connection = Connection(
            login='login', host='host',
            extra='{"ignore_hostkey_verification": false}')

        get_connection.return_value = connection
        hook = SFTPHook()
        self.assertEqual(hook.no_host_key_check, False)

    def tearDown(self):
        shutil.rmtree(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        os.remove(os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS))
        self.update_connection(self.old_login)


if __name__ == '__main__':
    unittest.main()
