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
from airflow.contrib.hooks.azure_fileshare_hook import AzureFileShareHook
from airflow.models.connection import Connection
from airflow.utils import db
from tests.compat import mock


class TestAzureFileshareHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='wasb_test_key', conn_type='wasb',
                login='login', password='key'
            )
        )
        db.merge_conn(
            Connection(
                conn_id='wasb_test_sas_token', conn_type='wasb',
                login='login', extra=json.dumps({'sas_token': 'token'})
            )
        )

    def test_key(self):
        from azure.storage.file import FileService
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_key')
        self.assertEqual(hook.conn_id, 'wasb_test_key')
        self.assertIsInstance(hook.connection, FileService)

    def test_sas_token(self):
        from azure.storage.file import FileService
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        self.assertEqual(hook.conn_id, 'wasb_test_sas_token')
        self.assertIsInstance(hook.connection, FileService)

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_check_for_file(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        self.assertTrue(hook.check_for_file('share', 'directory', 'file', timeout=3))
        mock_instance.exists.assert_called_once_with(
            'share', 'directory', 'file', timeout=3
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_check_for_directory(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        self.assertTrue(hook.check_for_directory('share', 'directory', timeout=3))
        mock_instance.exists.assert_called_once_with(
            'share', 'directory', timeout=3
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_load_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_file('path', 'share', 'directory', 'file', max_connections=1)
        mock_instance.create_file_from_path.assert_called_once_with(
            'share', 'directory', 'file', 'path', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_load_string(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_string('big string', 'share', 'directory', 'file', timeout=1)
        mock_instance.create_file_from_text.assert_called_once_with(
            'share', 'directory', 'file', 'big string', timeout=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_load_stream(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_stream('stream', 'share', 'directory', 'file', 42, timeout=1)
        mock_instance.create_file_from_stream.assert_called_once_with(
            'share', 'directory', 'file', 'stream', 42, timeout=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_list_directories_and_files(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.list_directories_and_files('share', 'directory', timeout=1)
        mock_instance.list_directories_and_files.assert_called_once_with(
            'share', 'directory', timeout=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_create_directory(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.create_directory('share', 'directory', timeout=1)
        mock_instance.create_directory.assert_called_once_with(
            'share', 'directory', timeout=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_get_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.get_file('path', 'share', 'directory', 'file', max_connections=1)
        mock_instance.get_file_to_path.assert_called_once_with(
            'share', 'directory', 'file', 'path', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.azure_fileshare_hook.FileService',
                autospec=True)
    def test_get_file_to_stream(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(wasb_conn_id='wasb_test_sas_token')
        hook.get_file_to_stream('stream', 'share', 'directory', 'file', max_connections=1)
        mock_instance.get_file_to_stream.assert_called_once_with(
            'share', 'directory', 'file', 'stream', max_connections=1
        )


if __name__ == '__main__':
    unittest.main()
