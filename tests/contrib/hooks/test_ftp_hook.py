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

import mock
import six
import unittest

from airflow.contrib.hooks import ftp_hook as fh


class TestFTPHook(unittest.TestCase):

    def setUp(self):
        super(TestFTPHook, self).setUp()
        self.path = '/some/path'
        self.conn_mock = mock.MagicMock(name='conn')
        self.get_conn_orig = fh.FTPHook.get_conn

        def _get_conn_mock(hook):
            hook.conn = self.conn_mock
            return self.conn_mock

        fh.FTPHook.get_conn = _get_conn_mock

    def tearDown(self):
        fh.FTPHook.get_conn = self.get_conn_orig
        super(TestFTPHook, self).tearDown()

    def test_close_conn(self):
        ftp_hook = fh.FTPHook()
        ftp_hook.get_conn()
        ftp_hook.close_conn()

        self.conn_mock.quit.assert_called_once_with()

    def test_list_directory(self):
        with fh.FTPHook() as ftp_hook:
            ftp_hook.list_directory(self.path)

        self.conn_mock.cwd.assert_called_once_with(self.path)
        self.conn_mock.nlst.assert_called_once_with()

    def test_create_directory(self):
        with fh.FTPHook() as ftp_hook:
            ftp_hook.create_directory(self.path)

        self.conn_mock.mkd.assert_called_once_with(self.path)

    def test_delete_directory(self):
        with fh.FTPHook() as ftp_hook:
            ftp_hook.delete_directory(self.path)

        self.conn_mock.rmd.assert_called_once_with(self.path)

    def test_delete_file(self):
        with fh.FTPHook() as ftp_hook:
            ftp_hook.delete_file(self.path)

        self.conn_mock.delete.assert_called_once_with(self.path)

    def test_rename(self):
        from_path = '/path/from'
        to_path = '/path/to'
        with fh.FTPHook() as ftp_hook:
            ftp_hook.rename(from_path, to_path)

        self.conn_mock.rename.assert_called_once_with(from_path, to_path)
        self.conn_mock.quit.assert_called_once_with()

    def test_mod_time(self):
        self.conn_mock.sendcmd.return_value = '213 20170428010138'

        path = '/path/file'
        with fh.FTPHook() as ftp_hook:
            ftp_hook.get_mod_time(path)

        self.conn_mock.sendcmd.assert_called_once_with('MDTM ' + path)

    def test_mod_time_micro(self):
        self.conn_mock.sendcmd.return_value = '213 20170428010138.003'

        path = '/path/file'
        with fh.FTPHook() as ftp_hook:
            ftp_hook.get_mod_time(path)

        self.conn_mock.sendcmd.assert_called_once_with('MDTM ' + path)

    def test_get_size(self):
        self.conn_mock.size.return_value = 1942

        path = '/path/file'
        with fh.FTPHook() as ftp_hook:
            ftp_hook.get_size(path)

        self.conn_mock.size.assert_called_once_with(path)

    def test_retrieve_file(self):
        _buffer = six.StringIO('buffer')
        with fh.FTPHook() as ftp_hook:
            ftp_hook.retrieve_file(self.path, _buffer)
        self.conn_mock.retrbinary.assert_called_once_with('RETR path', _buffer.write)

    def test_retrieve_file_with_callback(self):
        func = mock.Mock()
        _buffer = six.StringIO('buffer')
        with fh.FTPHook() as ftp_hook:
            ftp_hook.retrieve_file(self.path, _buffer, callback=func)
        self.conn_mock.retrbinary.assert_called_once_with('RETR path', func)


if __name__ == '__main__':
    unittest.main()
