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
"""
This module contains integration with Azure Storage FileShare.

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `azure_storage_fileshare` exists.
"""

import io
import json
import os
import tempfile
import unittest

import mock

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_storage_fileshare import AzureStorageFileShareHook
from airflow.utils import db

# connection string has a format
CONN_STRING = 'DefaultEndpointsProtocol=https;AccountName=testname;' \
              'AccountKey=wK7BOz;EndpointSuffix=core.windows.net'


class TestAzureFileshareHook(unittest.TestCase):

    def setUp(self):
        self.connection_type = 'azure_storage_fileshare'
        self.connection_string_id = 'azure_file_test_connection_string'
        self.shared_key_conn_id = 'azure_file_shared_key_test'
        self.ad_conn_id = 'azure_AD_test'
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"x" * 393216)
        self.testfile.flush()

        db.merge_conn(
            Connection(
                conn_id=self.connection_string_id, conn_type=self.connection_type,
                extra=json.dumps({'connection_string': CONN_STRING})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.shared_key_conn_id, conn_type=self.connection_type,
                host='https://accountname.file.core.windows.net',
                extra=json.dumps({'shared_access_key': 'token'})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.ad_conn_id, conn_type=self.connection_type,
                extra=json.dumps({'tenant_id': 'token', 'application_id': 'appID',
                                  'application_secret': "appsecret"})
            )
        )

    def tearDown(self):
        os.unlink(self.testfile.name)

    def test_connection_string(self):
        from azure.storage.fileshare import ShareServiceClient
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.connection_string_id)
        self.assertEqual(hook.conn_id, self.connection_string_id)
        self.assertIsInstance(hook.get_conn(), ShareServiceClient)

    def test_shared_key(self):
        from azure.storage.fileshare import ShareServiceClient
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        self.assertEqual(hook.conn_id, self.shared_key_conn_id)
        self.assertIsInstance(hook.get_conn(), ShareServiceClient)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_get_share_client(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook._get_share_client('myshare')
        mock_service.return_value.get_share_client.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_get_directory_client(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook._get_directory_client('myshare',
                                   directory_path='')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.get_directory_client.\
            assert_called_once_with(directory_path='')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_get_file_client(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook._get_file_client('myshare',
                              file_path='/mypath')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.get_file_client. \
            assert_called_once_with(file_path='/mypath')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_upload_file(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.upload_file(source_file=self.testfile.name,
                         share_name='myshare')
        share_client = mock_service.return_value.get_share_client
        file_client = share_client.return_value.get_file_client
        share_client.assert_called()
        file_client.assert_called()
        file_client.return_value.upload_file.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_download_file(self, mock_service):
        share_client = mock_service.return_value.get_share_client
        file_client = share_client.return_value.get_file_client
        file_client.return_value.download_file.return_value = io.FileIO(self.testfile.name)
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.download_file(dest_file=self.testfile.name,
                           share_name='myshare')

        share_client.assert_called()
        file_client.assert_called()
        file_client.return_value.download_file.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_create_file(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.create_file(size=100,
                         share_name='myshare')
        share_client = mock_service.return_value.get_share_client
        file_client = share_client.return_value.get_file_client
        share_client.assert_called()
        file_client.assert_called()
        file_client.return_value.create_file.assert_called_once_with(size=100)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_start_copy_from_url(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.start_copy_from_url(source_url='https://test-url.com',
                                 share_name='myshare')
        share_client = mock_service.return_value.get_share_client
        file_client = share_client.return_value.get_file_client
        share_client.assert_called()
        file_client.assert_called()
        file_client.return_value.start_copy_from_url.\
            assert_called_once_with('https://test-url.com')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_delete_file(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.delete_file(share_name='myshare')
        share_client = mock_service.return_value.get_share_client
        file_client = share_client.return_value.get_file_client
        share_client.assert_called()
        file_client.assert_called()
        file_client.return_value.delete_file.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_create_share(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.create_share(share_name='myshare')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.create_share.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_delete_share(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.delete_share(share_name='myshare')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.delete_share.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_create_snapshot(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.create_snapshot(share_name='myshare')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.create_snapshot.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_create_directory(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.create_directory(share_name='myshare',
                              snapshot=None,
                              directory_name='mydir')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.create_directory.\
            assert_called_once_with('mydir')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_delete_directory(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.delete_directory(share_name='myshare',
                              snapshot=None,
                              directory_name='mydir')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.delete_directory. \
            assert_called_once_with('mydir')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_list_directory_and_files(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.list_directories_and_files(share_name='myshare',
                                        snapshot=None,
                                        directory_name='mydir')
        mock_service.return_value.get_share_client.assert_called_once_with(share_name='myshare',
                                                                           snapshot=None)
        mock_service.return_value.get_share_client.return_value.list_directories_and_files. \
            assert_called_once_with(directory_name='mydir')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_fileshare.ShareServiceClient")
    def test_list_shares(self, mock_service):
        hook = AzureStorageFileShareHook(azure_fileshare_conn_id=self.shared_key_conn_id)
        hook.list_shares(name_starts_with='m',
                         include_snapshots=True,
                         include_metadata=False)
        mock_service.return_value.list_shares.assert_called_once_with(name_starts_with='m',
                                                                      include_snapshots=True,
                                                                      include_metadata=False)
