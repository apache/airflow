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
from airflow.providers.microsoft.azure.hooks.azure_container_registry import AzureContainerRegistryHook
from airflow.utils import db


class TestAzureContainerRegistryHook(unittest.TestCase):

    def test_get_conn(self):
        db.merge_conn(
            Connection(
                conn_id='azure_container_registry',
                login='myuser',
                password='password',
                host='test.cr',
            )
        )
        hook = AzureContainerRegistryHook(conn_id='azure_container_registry')
        self.assertIsNotNone(hook.connection)
        self.assertEqual(hook.connection.username, 'myuser')
        self.assertEqual(hook.connection.password, 'password')
        self.assertEqual(hook.connection.server, 'test.cr')
