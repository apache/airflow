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

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import unittest

from airflow import configuration
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.models import Connection as Conn


class TestMongoHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def test_uri_empty(self):
        conn = Conn()
        uri = MongoHook.build_uri(conn)
        self.assertEqual(uri, 'mongodb://localhost')

    def test_uri_simple(self):
        conn = Conn(
            host='1.2.3.4,1.2.3.5',
            login='admin',
            password='password',
            schema='mydatabase',
            extra='authSource=admin&replicaSet=replset')

        uri = MongoHook.build_uri(conn)
        self.assertEqual(
            uri,
            'mongodb://admin:password@1.2.3.4,1.2.3.5/mydatabase?'
            'authSource=admin&replicaSet=replset')

        uri = MongoHook.build_uri(conn, shadow=True)
        self.assertEqual(
            uri,
            'mongodb://admin:***@1.2.3.4,1.2.3.5/mydatabase?'
            'authSource=admin&replicaSet=replset')

    def test_get_conn(self):
        hook = MongoHook()

        self.assertEqual(hook.mongo_conn_id, 'mongo_default')
        self.assertEqual(hook.reuse_connection, True)
        self.assertEqual(hook.mongo_client, None)

        client = hook.get_conn(check_connection=False)

        self.assertIsInstance(client, MongoClient)

    def test_get_conn_failure(self):
        hook = MongoHook()

        with self.assertRaises(ConnectionFailure):
            hook.get_conn()


if __name__ == '__main__':
    unittest.main()
