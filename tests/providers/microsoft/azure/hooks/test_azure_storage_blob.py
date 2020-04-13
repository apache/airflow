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
import io
import json
import os
import tempfile
import unittest

import mock
from azure.storage.blob import BlobServiceClient

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.utils import db

# connection_string has a format
CONN_STRING = 'DefaultEndpointsProtocol=https;AccountName=testname;' \
              'AccountKey=wK7BOz;EndpointSuffix=core.windows.net'

ACCESS_KEY_STRING = "AccountName=name;skdkskd"


class TestAzureStorageBlobHook(unittest.TestCase):

    def setUp(self):
        self.connection_type = 'azure_storage_blob'
        self.connection_string_id = 'azure_test_connection_string'
        self.shared_key_conn_id = 'azure_shared_key_test'
        self.ad_conn_id = 'azure_AD_test'
        self.sas_conn_id = 'sas_token_id'
        self.public_read_conn_id = 'pub_read_id'
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"x" * 393216)
        self.testfile.flush()

        db.merge_conn(
            Connection(
                conn_id=self.public_read_conn_id, conn_type=self.connection_type,
                host='https://accountname.blob.core.windows.net'
            )
        )

        db.merge_conn(
            Connection(
                conn_id=self.connection_string_id, conn_type=self.connection_type,
                extra=json.dumps({'connection_string': CONN_STRING})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.shared_key_conn_id, conn_type=self.connection_type,
                host='https://accountname.blob.core.windows.net',
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
        db.merge_conn(
            Connection(
                conn_id=self.sas_conn_id, conn_type=self.connection_type,
                extra=json.dumps({'sas_token': 'token'})
            )
        )

    def tearDown(self):
        os.unlink(self.testfile.name)

    def test_public_read(self):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.public_read_conn_id, public_read=True)
        self.assertIsInstance(hook.get_conn(), BlobServiceClient)

    def test_connection_string(self):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.connection_string_id)
        self.assertEqual(hook.conn_id, self.connection_string_id)
        self.assertIsInstance(hook.get_conn(), BlobServiceClient)

    def test_shared_key_connection(self):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        self.assertIsInstance(hook.get_conn(), BlobServiceClient)

    def test_sas_token_connection(self):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.sas_conn_id)
        self.assertIsInstance(hook.get_conn(), BlobServiceClient)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_get_container_client(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook._get_container_client('mycontainer')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_get_blob_client(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook._get_blob_client(container_name='mycontainer',
                              blob_name='myblob')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.get_blob_client.\
            assert_called_once_with('myblob')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_create_container(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.create_container(container_name='mycontainer')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.create_container.\
            assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_delete_container(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.delete_container('mycontainer')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.delete_container. \
            assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_list(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.list(container_name='mycontainer',
                  name_starts_with='my',
                  include=None,
                  delimiter='/')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.walk_blobs.\
            assert_called_once_with(name_starts_with='my',
                                    include=None,
                                    delimiter='/')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_upload(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.upload(container_name='mycontainer',
                    blob_name='myblob',
                    data=b'mydata',
                    blob_type='BlockBlob',
                    length=4)
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.get_blob_client.\
            assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value.get_blob_client.\
            return_value.upload_blob.assert_called_once_with(
                b'mydata',
                'BlockBlob',
                length=4
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_download(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        container_client = mock_service.return_value.get_container_client.return_value
        blob_client = container_client.get_blob_client.return_value
        blob_client.download_blob.return_value = io.FileIO(self.testfile.name)
        hook.download(container_name='mycontainer',
                      blob_name='myblob',
                      dest_file='dest',
                      offset=2,
                      length=4)
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            return_value.download_blob.assert_called_once_with(offset=2,
                                                               length=4)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_copy(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.copy(container_name='mycontainer',
                  blob_name='myblob',
                  source_url='https://testurl.com')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            return_value.start_copy_from_url.assert_called_once_with('https://testurl.com')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_check_copy_status(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.check_copy_status(container_name='mycontainer',
                               blob_name='myblob')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value.get_blob_client. \
            return_value.get_blob_properties.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_create_snapshot(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.create_snapshot(container_name='mycontainer',
                             blob_name='myblob',
                             metadata=None)
        mock_service.return_value.get_container_client.\
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value.\
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.create_snapshot.\
            assert_called_once_with(metadata=None)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_get_blob_info(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.get_blob_info(container_name='mycontainer',
                           blob_name='myblob')
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.get_blob_properties.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_delete_blob(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.delete_blob(container_name='mycontainer',
                         blob_name='myblob',
                         delete_snapshots=True)
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.delete_blob.assert_called_with(delete_snapshots=True)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_delete_blobs(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.delete_blobs('mycontainer', 'blob1', 'blob2', 'blob3')
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value. \
            delete_blobs.assert_called_once_with('blob1', 'blob2', 'blob3')

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_create_page_blob(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.create_page_blob(container_name='mycontainer',
                              blob_name='myblob',
                              size=2)
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.create_page_blob.assert_called_with(2)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_create_append_blob(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.create_append_blob(container_name='mycontainer',
                                blob_name='myblob')
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.create_append_blob.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_upload_page(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.upload_page(container_name='mycontainer',
                         blob_name='myblob',
                         page=b'page',
                         offset=2,
                         length=2
                         )
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.upload_page.assert_called_once_with(b'page', 2, 2)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_upload_page_from_url(self, mock_service):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        hook.upload_pages_from_url(container_name='mycontainer',
                                   blob_name='myblob',
                                   source_url='https://testurl.com',
                                   offset=2,
                                   length=2,
                                   source_offset=2
                                   )
        mock_service.return_value.get_container_client. \
            assert_called_once_with('mycontainer')

        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.assert_called_once_with('myblob')
        mock_service.return_value.get_container_client.return_value. \
            get_blob_client.return_value.upload_pages_from_url.assert_called_once_with(
                source_url='https://testurl.com',
                offset=2,
                length=2,
                source_offset=2
            )
