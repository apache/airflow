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
from collections import namedtuple

from airflow import configuration, AirflowException
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models.connection import Connection
from airflow.utils import db

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestWasbHook(unittest.TestCase):

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
        from azure.storage.blob import BlockBlobService
        hook = WasbHook(wasb_conn_id='wasb_test_key')
        self.assertEqual(hook.conn_id, 'wasb_test_key')
        self.assertIsInstance(hook.connection, BlockBlobService)

    def test_sas_token(self):
        from azure.storage.blob import BlockBlobService
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        self.assertEqual(hook.conn_id, 'wasb_test_sas_token')
        self.assertIsInstance(hook.connection, BlockBlobService)

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_check_for_blob(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        self.assertTrue(hook.check_for_blob('container', 'blob', timeout=3))
        mock_instance.exists.assert_called_once_with(
            'container', 'blob', timeout=3
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_check_for_blob_empty(self, mock_service):
        mock_service.return_value.exists.return_value = False
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        self.assertFalse(hook.check_for_blob('container', 'blob'))

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_check_for_prefix(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.list_blobs.return_value = iter(['blob_1'])
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        self.assertTrue(hook.check_for_prefix('container', 'prefix',
                                              timeout=3))
        mock_instance.list_blobs.assert_called_once_with(
            'container', 'prefix', num_results=1, timeout=3
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_check_for_prefix_empty(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.list_blobs.return_value = iter([])
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        self.assertFalse(hook.check_for_prefix('container', 'prefix'))

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_load_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_file('path', 'container', 'blob', max_connections=1)
        mock_instance.create_blob_from_path.assert_called_once_with(
            'container', 'blob', 'path', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_load_string(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_string('big string', 'container', 'blob', max_connections=1)
        mock_instance.create_blob_from_text.assert_called_once_with(
            'container', 'blob', 'big string', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_get_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.get_file('path', 'container', 'blob', max_connections=1)
        mock_instance.get_blob_to_path.assert_called_once_with(
            'container', 'blob', 'path', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_read_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.read_file('container', 'blob', max_connections=1)
        mock_instance.get_blob_to_text.assert_called_once_with(
            'container', 'blob', max_connections=1
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_delete_single_blob(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.delete_file('container', 'blob', is_prefix=False)
        mock_instance.delete_blob.assert_called_once_with(
            'container', 'blob', delete_snapshots='include'
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_delete_multiple_blobs(self, mock_service):
        mock_instance = mock_service.return_value
        Blob = namedtuple('Blob', ['name'])
        mock_instance.list_blobs.return_value = iter(
            [Blob('blob_prefix/blob1'), Blob('blob_prefix/blob2')]
        )
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.delete_file('container', 'blob_prefix', is_prefix=True)
        mock_instance.delete_blob.assert_any_call(
            'container', 'blob_prefix/blob1', delete_snapshots='include'
        )
        mock_instance.delete_blob.assert_any_call(
            'container', 'blob_prefix/blob2', delete_snapshots='include'
        )

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_delete_nonexisting_blob_fails(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = False
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        with self.assertRaises(Exception) as context:
            hook.delete_file(
                'container', 'nonexisting_blob',
                is_prefix=False, ignore_if_missing=False
            )
        self.assertIsInstance(context.exception, AirflowException)

    @mock.patch('airflow.contrib.hooks.wasb_hook.BlockBlobService',
                autospec=True)
    def test_delete_multiple_nonexisting_blobs_fails(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.list_blobs.return_value = iter([])
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        with self.assertRaises(Exception) as context:
            hook.delete_file(
                'container', 'nonexisting_blob_prefix',
                is_prefix=True, ignore_if_missing=False
            )
        self.assertIsInstance(context.exception, AirflowException)


if __name__ == '__main__':
    unittest.main()
