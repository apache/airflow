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
from unittest import mock

import pytest
from azure.storage.blob import BlobServiceClient

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils import db

# connection_string has a format
CONN_STRING = (
    'DefaultEndpointsProtocol=https;AccountName=testname;AccountKey=wK7BOz;EndpointSuffix=core.windows.net'
)

ACCESS_KEY_STRING = "AccountName=name;skdkskd"


class TestWasbHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(Connection(conn_id='wasb_test_key', conn_type='wasb', login='login', password='key'))
        self.connection_type = 'wasb'
        self.connection_string_id = 'azure_test_connection_string'
        self.shared_key_conn_id = 'azure_shared_key_test'
        self.ad_conn_id = 'azure_AD_test'
        self.sas_conn_id = 'sas_token_id'
        self.public_read_conn_id = 'pub_read_id'

        db.merge_conn(
            Connection(
                conn_id=self.public_read_conn_id,
                conn_type=self.connection_type,
                host='https://accountname.blob.core.windows.net',
            )
        )

        db.merge_conn(
            Connection(
                conn_id=self.connection_string_id,
                conn_type=self.connection_type,
                extra=json.dumps({'connection_string': CONN_STRING}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.shared_key_conn_id,
                conn_type=self.connection_type,
                host='https://accountname.blob.core.windows.net',
                extra=json.dumps({'shared_access_key': 'token'}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.ad_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps(
                    {'tenant_id': 'token', 'application_id': 'appID', 'application_secret': "appsecret"}
                ),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps({'sas_token': 'token'}),
            )
        )

    def test_key(self):
        hook = WasbHook(wasb_conn_id='wasb_test_key')
        self.assertEqual(hook.conn_id, 'wasb_test_key')
        self.assertIsInstance(hook.connection, BlobServiceClient)

    def test_public_read(self):
        hook = WasbHook(wasb_conn_id=self.public_read_conn_id, public_read=True)
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_connection_string(self):
        hook = WasbHook(wasb_conn_id=self.connection_string_id)
        assert hook.conn_id == self.connection_string_id
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_shared_key_connection(self):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_sas_token_connection(self):
        hook = WasbHook(wasb_conn_id=self.sas_conn_id)
        assert isinstance(hook.get_conn(), BlobServiceClient)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_check_for_blob(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert hook.check_for_blob(container_name='mycontainer', blob_name='myblob')
        mock_container_client = mock_service.return_value.get_container_client
        mock_container_client.assert_called_once_with('mycontainer')
        mock_container_client.return_value.get_blob_client.assert_called_once_with('myblob')
        mock_container_client.return_value.get_blob_client.return_value.get_blob_properties.assert_called()

    @mock.patch.object(WasbHook, 'get_blobs_list')
    def test_check_for_prefix(self, get_blobs_list):
        get_blobs_list.return_value = ['blobs']
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert hook.check_for_prefix('container', 'prefix', timeout=3)
        get_blobs_list.assert_called_once_with(container_name='container', prefix='prefix', timeout=3)

    @mock.patch.object(WasbHook, 'get_blobs_list')
    def test_check_for_prefix_empty(self, get_blobs_list):
        get_blobs_list.return_value = []
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert not hook.check_for_prefix('container', 'prefix', timeout=3)
        get_blobs_list.assert_called_once_with(container_name='container', prefix='prefix', timeout=3)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_get_blobs_list(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.get_blobs_list(container_name='mycontainer', prefix='my', include=None, delimiter='/')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.walk_blobs.assert_called_once_with(
            name_starts_with='my', include=None, delimiter='/'
        )

    @mock.patch.object(WasbHook, 'upload')
    def test_load_file(self, mock_upload):
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.load_file('path', 'container', 'blob', max_connections=1)
        mock_upload.assert_called()

    @mock.patch.object(WasbHook, 'upload')
    def test_load_string(self, mock_upload):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.load_string('big string', 'container', 'blob', max_connections=1)
        mock_upload.assert_called_once_with('container', 'blob', 'big string', max_connections=1)

    @mock.patch.object(WasbHook, 'download')
    def test_get_file(self, mock_download):
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.get_file('path', 'container', 'blob', max_connections=1)
        mock_download.assert_called_once_with(container_name='container', blob_name='blob', max_connections=1)
        mock_download.return_value.readall.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, 'download')
    def test_read_file(self, mock_download, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.read_file('container', 'blob', max_connections=1)
        mock_download.assert_called_once_with('container', 'blob', max_connections=1)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_upload(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.upload(
            container_name='mycontainer', blob_name='myblob', data=b'mydata', blob_type='BlockBlob', length=4
        )
        mock_cn_client = mock_service.return_value.get_container_client
        mock_cn_client.assert_called_once_with('mycontainer')
        mock_cn_client.return_value.get_blob_client.assert_called_once_with('myblob')
        mock_cn_client.return_value.get_blob_client.return_value.upload_blob.assert_called_once_with(
            b'mydata', 'BlockBlob', length=4
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_download(self, mock_service):
        container_client = mock_service.return_value.get_container_client
        blob_client = container_client.return_value.get_blob_client
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.download(container_name='mycontainer', blob_name='myblob', offset=2, length=4)
        container_client.assert_called_once_with('mycontainer')
        blob_client.assert_called_once_with('myblob')
        blob_client.return_value.download_blob.assert_called_once_with(offset=2, length=4)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_get_container_client(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook._get_container_client('mycontainer')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_get_blob_client(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook._get_blob_client(container_name='mycontainer', blob_name='myblob')
        mock_instance = mock_service.return_value.get_container_client
        mock_instance.assert_called_once_with('mycontainer')
        mock_instance.return_value.get_blob_client.assert_called_once_with('myblob')

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_create_container(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.create_container(container_name='mycontainer')
        mock_instance = mock_service.return_value.get_container_client
        mock_instance.assert_called_once_with('mycontainer')
        mock_instance.return_value.create_container.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    def test_delete_container(self, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_container('mycontainer')
        mock_service.return_value.get_container_client.assert_called_once_with('mycontainer')
        mock_service.return_value.get_container_client.return_value.delete_container.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, 'delete_blobs')
    def test_delete_single_blob(self, delete_blobs, mock_service):
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_file('container', 'blob', is_prefix=False)
        delete_blobs.assert_called_once_with('container', 'blob')

    @mock.patch.object(WasbHook, 'delete_blobs')
    @mock.patch.object(WasbHook, 'get_blobs_list')
    @mock.patch.object(WasbHook, 'check_for_blob')
    def test_delete_multiple_blobs(self, mock_check, mock_get_blobslist, mock_delete_blobs):
        mock_check.return_value = False
        mock_get_blobslist.return_value = ['blob_prefix/blob1', 'blob_prefix/blob2']
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_file('container', 'blob_prefix', is_prefix=True)
        mock_get_blobslist.assert_called_once_with('container', prefix='blob_prefix')
        mock_delete_blobs.assert_any_call(
            'container',
            'blob_prefix/blob1',
            'blob_prefix/blob2',
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, 'get_blobs_list')
    @mock.patch.object(WasbHook, 'check_for_blob')
    def test_delete_nonexisting_blob_fails(self, mock_check, mock_getblobs, mock_service):
        mock_getblobs.return_value = []
        mock_check.return_value = False
        with pytest.raises(Exception) as ctx:
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.delete_file('container', 'nonexisting_blob', is_prefix=False, ignore_if_missing=False)
        assert isinstance(ctx.value, AirflowException)

    @mock.patch.object(WasbHook, 'get_blobs_list')
    def test_delete_multiple_nonexisting_blobs_fails(self, mock_getblobs):
        mock_getblobs.return_value = []
        with pytest.raises(Exception) as ctx:
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.delete_file('container', 'nonexisting_blob_prefix', is_prefix=True, ignore_if_missing=False)
        assert isinstance(ctx.value, AirflowException)
