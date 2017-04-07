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


import json
import unittest

from airflow import configuration
from airflow import models
from airflow.contrib.hooks.wasb_hook import WasbHook
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
            models.Connection(
                conn_id='wasb_test_key', conn_type='wasb',
                login='login', password='key'
            )
        )
        db.merge_conn(
            models.Connection(
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
            'container', 'prefix', timeout=3
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
    def test_check_for_prefix(self, mock_service):
        mock_instance = mock_service.return_value
        hook = WasbHook(wasb_conn_id='wasb_test_sas_token')
        hook.load_file('path', 'container', 'blob', max_connections=1)
        mock_instance.create_blob_from_path.assert_called_once_with(
            'container', 'blob', 'path', max_connections=1
        )


if __name__ == '__main__':
    unittest.main()
