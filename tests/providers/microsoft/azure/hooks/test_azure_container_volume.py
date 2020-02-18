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

import unittest

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_container_volume import AzureContainerVolumeHook
from airflow.utils import db


class TestAzureContainerVolumeHook(unittest.TestCase):

    def test_get_file_volume(self):
        db.merge_conn(
            Connection(
                conn_id='wasb_test_key',
                conn_type='wasb',
                login='login',
                password='key'
            )
        )
        hook = AzureContainerVolumeHook(wasb_conn_id='wasb_test_key')
        volume = hook.get_file_volume(mount_name='mount',
                                      share_name='share',
                                      storage_account_name='storage',
                                      read_only=True)
        self.assertIsNotNone(volume)
        self.assertEqual(volume.name, 'mount')
        self.assertEqual(volume.azure_file.share_name, 'share')
        self.assertEqual(volume.azure_file.storage_account_key, 'key')
        self.assertEqual(volume.azure_file.storage_account_name, 'storage')
        self.assertEqual(volume.azure_file.read_only, True)
