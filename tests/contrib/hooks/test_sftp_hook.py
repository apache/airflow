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

from __future__ import print_function

import unittest
import shutil
import os
import pysftp

from airflow import configuration
from airflow.contrib.hooks.sftp_hook import SFTPHook

TMP_PATH = '/tmp'
TMP_DIR_FOR_TESTS = 'tests_sftp_hook_dir'
TMP_FILE_FOR_TESTS = 'test_file.txt'


class SFTPHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.hook = SFTPHook()
        os.makedirs(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        with open(os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS), 'a') as f:
            f.write('Test file')

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

    def tearDown(self):
        shutil.rmtree(os.path.join(TMP_PATH, TMP_DIR_FOR_TESTS))
        os.remove(os.path.join(TMP_PATH, TMP_FILE_FOR_TESTS))


if __name__ == '__main__':
    unittest.main()
