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


import json
import unittest

from airflow import configuration
from airflow.models.connection import Connection
from airflow.utils import db
from tests.compat import mock


class TestAzureDataLakeHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='adl_test_key',
                conn_type='azure_data_lake',
                login='client_id',
                password='client secret',
                extra=json.dumps({"tenant": "tenant",
                                  "account_name": "accountname"})
            )
        )

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_conn(self, mock_lib):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        from azure.datalake.store import core
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        self.assertEqual(hook.conn_id, 'adl_test_key')
        self.assertIsInstance(hook.connection, core.AzureDLFileSystem)
        assert mock_lib.auth.called

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.core.AzureDLFileSystem',
                autospec=True)
    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_check_for_blob(self, mock_lib, mock_filesystem):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        hook.check_for_file('file_path')
        mock_filesystem.glob.called

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.multithread.ADLUploader',
                autospec=True)
    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_upload_file(self, mock_lib, mock_uploader):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        hook.upload_file(local_path='tests/hooks/test_adl_hook.py',
                         remote_path='/test_adl_hook.py',
                         nthreads=64, overwrite=True,
                         buffersize=4194304, blocksize=4194304)
        mock_uploader.assert_called_once_with(hook.connection,
                                              lpath='tests/hooks/test_adl_hook.py',
                                              rpath='/test_adl_hook.py',
                                              nthreads=64, overwrite=True,
                                              buffersize=4194304, blocksize=4194304)

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.multithread.ADLDownloader',
                autospec=True)
    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_download_file(self, mock_lib, mock_downloader):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        hook.download_file(local_path='test_adl_hook.py',
                           remote_path='/test_adl_hook.py',
                           nthreads=64, overwrite=True,
                           buffersize=4194304, blocksize=4194304)
        mock_downloader.assert_called_once_with(hook.connection,
                                                lpath='test_adl_hook.py',
                                                rpath='/test_adl_hook.py',
                                                nthreads=64, overwrite=True,
                                                buffersize=4194304, blocksize=4194304)

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.core.AzureDLFileSystem',
                autospec=True)
    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_list_glob(self, mock_lib, mock_fs):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        hook.list('file_path/*')
        mock_fs.return_value.glob.assert_called_with('file_path/*')

    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.core.AzureDLFileSystem',
                autospec=True)
    @mock.patch('airflow.contrib.hooks.azure_data_lake_hook.lib', autospec=True)
    def test_list_walk(self, mock_lib, mock_fs):
        from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
        hook = AzureDataLakeHook(azure_data_lake_conn_id='adl_test_key')
        hook.list('file_path/some_folder/')
        mock_fs.return_value.walk.assert_called_with('file_path/some_folder/')


if __name__ == '__main__':
    unittest.main()
