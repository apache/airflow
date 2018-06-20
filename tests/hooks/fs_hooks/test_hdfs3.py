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
#

import unittest

import mock
from hdfs3.utils import MyNone

from airflow.hooks.fs_hooks.hdfs3 import Hdfs3Hook, hdfs3


class TestHdfs3Hook(unittest.TestCase):
    """
    Tests for the Hdfs3Hook class.

    Note that the HDFileSystem class is mocked in most of these tests
    to avoid the requirement of having a local HDFS instance for testing.
    """

    def setUp(self):
        self._mock_fs = mock.Mock()

        self._mocked_hook = Hdfs3Hook()
        self._mocked_hook._conn = self._mock_fs

    def test_open(self):
        """Tests the `open` method."""

        with self._mocked_hook as hook:
            hook.open('test.txt', mode='rb')

        self._mock_fs.open.assert_called_once_with('test.txt', mode='rb')

    def test_exists(self):
        """Tests the `exists` method."""

        with self._mocked_hook as hook:
            hook.exists('test.txt')

        self._mock_fs.exists.assert_called_once_with('test.txt')

    def test_isdir(self):
        """Tests the `isdir` method."""

        with self._mocked_hook as hook:
            hook.isdir('test.txt')

        self._mock_fs.isdir.assert_called_once_with('test.txt')

    def test_makedir(self):
        """Tests the `makedir` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755)

        self._mock_fs.mkdir.assert_called_once_with('path/to/dir')
        self._mock_fs.chmod.assert_called_once_with('path/to/dir', mode=0o755)

    def test_makedir_existing(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedir('path/to/dir', mode=0o755, exist_ok=False)

    def test_makedir_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedir('path/to/dir', mode=0o755, exist_ok=True)

        self._mock_fs.chmod.assert_not_called()

    def test_makedirs(self):
        """Tests the `makedirs` method with a non-existing dir."""

        self._mock_fs.exists.return_value = False

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755)

        self._mock_fs.makedirs.assert_called_once_with(
            'path/to/dir', mode=0o755)

    def test_makedirs_existing(self):
        """Tests the `makedirs` method with an existing dir
           and exist_ok = False.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            with self.assertRaises(IOError):
                hook.makedirs('path/to/dir', mode=0o755, exist_ok=False)

        self._mock_fs.makedirs.assert_not_called()

    def test_makedirs_existing_ok(self):
        """Tests the `makedir` method with an existing dir
           and exist_ok = True.
        """

        self._mock_fs.exists.return_value = True

        with self._mocked_hook as hook:
            hook.makedirs('path/to/dir', mode=0o755, exist_ok=True)

    def test_rm(self):
        """Tests the `rm` method."""

        with self._mocked_hook as hook:
            hook.rm('test_dir')

        self._mock_fs.rm.assert_called_once_with(
            'test_dir', recursive=False)

    def test_rmtree(self):
        """Tests the `rmtree` method."""

        with self._mocked_hook as hook:
            hook.rmtree('test_dir')

        self._mock_fs.rm.assert_called_once_with(
            'test_dir', recursive=True)

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(Hdfs3Hook, 'get_connection')
    def test_get_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call without ID."""

        with Hdfs3Hook() as hook:
            hook.get_conn()

        conn_mock.assert_not_called()
        hdfs3_mock.assert_called_once_with()

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(Hdfs3Hook, 'get_connection')
    def test_get_conn_with_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call with ID."""

        conn_mock.return_value = mock.Mock(
            host='namenode',
            login='hdfs_user',
            port=8020,
            extra_dejson={'pars': {'dfs.namenode.logging.level': 'info'}})

        with Hdfs3Hook(conn_id='hdfs_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('hdfs_default')

        hdfs3_mock.assert_called_once_with(
            host='namenode',
            port=8020,
            pars={'dfs.namenode.logging.level': 'info'},
            user='hdfs_user')

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(Hdfs3Hook, 'get_connection')
    def test_get_conn_with_empty_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call with empty connection."""

        conn_mock.return_value = mock.Mock(
            host='',
            login='',
            port='',
            extra_dejson={})

        with Hdfs3Hook(conn_id='hdfs_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('hdfs_default')
        hdfs3_mock.assert_called_once_with(host=MyNone, port=MyNone, pars={})


if __name__ == '__main__':
    unittest.main()
