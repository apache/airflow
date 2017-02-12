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
#

import mock
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


class TestIntegrationFTPHook(unittest.TestCase):

    def setUp(self):
        super(TestIntegrationFTPHook, self).setUp()

        from airflow import configuration
        from airflow.utils import db
        from airflow import models

        configuration.load_test_config()
        db.merge_conn(
                models.Connection(
                        conn_id='ftp_passive', conn_type='ftp',
                        host='localhost',
                        extra='{"passive": true}'))

        db.merge_conn(
                models.Connection(
                        conn_id='ftp_active', conn_type='ftp',
                        host='localhost',
                        extra='{"passive": false}'))

    def _test_mode(self, hook_type, connection_id, expected_mode):
        hook = hook_type(connection_id)
        conn = hook.get_conn()
        conn.set_pasv.assert_called_with(expected_mode)

    @mock.patch("ftplib.FTP")
    def test_ftp_passive_mode(self, ftp_mock):
        from airflow.contrib.hooks.ftp_hook import FTPHook
        self._test_mode(FTPHook, "ftp_passive", True)

    @mock.patch("ftplib.FTP")
    def test_ftp_active_mode(self, ftp_mock):
        from airflow.contrib.hooks.ftp_hook import FTPHook
        self._test_mode(FTPHook, "ftp_active", False)

    @mock.patch("ftplib.FTP_TLS")
    def test_ftps_passive_mode(self, ftps_mock):
        from airflow.contrib.hooks.ftp_hook import FTPSHook
        self._test_mode(FTPSHook, "ftp_passive", True)

    @mock.patch("ftplib.FTP_TLS")
    def test_ftps_active_mode(self, ftps_mock):
        from airflow.contrib.hooks.ftp_hook import FTPSHook
        self._test_mode(FTPSHook, "ftp_active", False)

if __name__ == '__main__':
    unittest.main()
