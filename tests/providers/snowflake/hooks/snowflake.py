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
import os
import unittest
from unittest import mock

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class TestSnowflakeHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        self.conn.login = 'user'
        self.conn.password = 'pw'
        self.conn.schema = 'public'
        self.conn.extra_dejson = {'database': 'db',
                                  'account': 'airflow',
                                  'warehouse': 'af_wh',
                                  'region': 'af_region',
                                  'role': 'af_role'}

        class UnitTestSnowflakeHook(SnowflakeHook):
            conn_name_attr = 'snowflake_conn_id'

            def get_conn(self):
                return conn

            def get_connection(self, _):
                return conn

        self.db_hook = UnitTestSnowflakeHook()

        self.non_encrypted_private_key = "/tmp/test_key.pem"
        self.encrypted_private_key = "/tmp/test_key.p8"

        # Write some temporary private keys. First is not encrypted, second is with a passphrase.
        key = rsa.generate_private_key(
            backend=default_backend(),
            public_exponent=65537,
            key_size=2048
        )
        private_key = key.private_bytes(serialization.Encoding.PEM,
                                        serialization.PrivateFormat.PKCS8,
                                        serialization.NoEncryption())

        with open(self.non_encrypted_private_key, "wb") as file:
            file.write(private_key)

        key = rsa.generate_private_key(
            backend=default_backend(),
            public_exponent=65537,
            key_size=2048
        )
        private_key = key.private_bytes(serialization.Encoding.PEM,
                                        serialization.PrivateFormat.PKCS8,
                                        encryption_algorithm=serialization.BestAvailableEncryption(
                                            self.conn.password.encode()))

        with open(self.encrypted_private_key, "wb") as file:
            file.write(private_key)

    def tearDown(self):
        os.remove(self.encrypted_private_key)
        os.remove(self.non_encrypted_private_key)

    def test_get_uri(self):
        uri_shouldbe = 'snowflake://user:pw@airflow/db/public?warehouse=af_wh&role=af_role'
        self.assertEqual(uri_shouldbe, self.db_hook.get_uri())

    def test_get_conn_params(self):
        conn_params_shouldbe = {'user': 'user',
                                'password': 'pw',
                                'schema': 'public',
                                'database': 'db',
                                'account': 'airflow',
                                'warehouse': 'af_wh',
                                'region': 'af_region',
                                'role': 'af_role'}
        self.assertEqual(conn_params_shouldbe, self.db_hook._get_conn_params())

    def test_get_conn(self):
        self.assertEqual(self.db_hook.get_conn(), self.conn)

    def test_key_pair_auth_encrypted(self):
        self.conn.extra_dejson = {'database': 'db',
                                  'account': 'airflow',
                                  'warehouse': 'af_wh',
                                  'region': 'af_region',
                                  'role': 'af_role',
                                  'private_key_file': self.encrypted_private_key}

        params = self.db_hook._get_conn_params()
        self.assertTrue('private_key' in params)

    def test_key_pair_auth_not_encrypted(self):
        self.conn.extra_dejson = {'database': 'db',
                                  'account': 'airflow',
                                  'warehouse': 'af_wh',
                                  'region': 'af_region',
                                  'role': 'af_role',
                                  'private_key_file': self.non_encrypted_private_key}

        self.conn.password = ''
        params = self.db_hook._get_conn_params()
        self.assertTrue('private_key' in params)

        self.conn.password = None
        params = self.db_hook._get_conn_params()
        self.assertTrue('private_key' in params)
