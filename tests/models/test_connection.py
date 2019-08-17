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

import unittest
from collections import namedtuple

from cryptography.fernet import Fernet
from parameterized import parameterized

from airflow.models import Connection, crypto
from tests.test_utils.config import conf_vars

ConnectionParts = namedtuple("ConnectionParts", ["conn_type", "login", "password", "host", "port", "schema"])


class TestConnection(unittest.TestCase):
    def setUp(self):
        crypto._fernet = None

    def tearDown(self):
        crypto._fernet = None

    @conf_vars({('core', 'FERNET_KEY'): ''})
    def test_connection_extra_no_encryption(self):
        """
        Tests extras on a new connection without encryption. The fernet key
        is set to a non-base64-encoded string and the extra is stored without
        encryption.
        """
        test_connection = Connection(extra='testextra')
        self.assertFalse(test_connection.is_extra_encrypted)
        self.assertEqual(test_connection.extra, 'testextra')

    @conf_vars({('core', 'FERNET_KEY'): Fernet.generate_key().decode()})
    def test_connection_extra_with_encryption(self):
        """
        Tests extras on a new connection with encryption.
        """
        test_connection = Connection(extra='testextra')
        self.assertTrue(test_connection.is_extra_encrypted)
        self.assertEqual(test_connection.extra, 'testextra')

    def test_connection_extra_with_encryption_rotate_fernet_key(self):
        """
        Tests rotating encrypted extras.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({('core', 'FERNET_KEY'): key1.decode()}):
            test_connection = Connection(extra='testextra')
            self.assertTrue(test_connection.is_extra_encrypted)
            self.assertEqual(test_connection.extra, 'testextra')
            self.assertEqual(Fernet(key1).decrypt(test_connection._extra.encode()), b'testextra')

        # Test decrypt of old value with new key
        with conf_vars({('core', 'FERNET_KEY'): ','.join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            self.assertEqual(test_connection.extra, 'testextra')

            # Test decrypt of new value with new key
            test_connection.rotate_fernet_key()
            self.assertTrue(test_connection.is_extra_encrypted)
            self.assertEqual(test_connection.extra, 'testextra')
            self.assertEqual(Fernet(key2).decrypt(test_connection._extra.encode()), b'testextra')

    def test_connection_from_uri_without_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertIsNone(connection.extra)

    def test_connection_from_uri_with_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema?' \
              'extra1=a%20value&extra2=%2fpath%2f'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': '/path/'})

    def test_connection_from_uri_with_empty_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema?' \
              'extra1=a%20value&extra2='
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': ''})

    def test_connection_from_uri_with_colon_in_hostname(self):
        uri = 'scheme://user:password@host%2flocation%3ax%3ay:1234/schema?' \
              'extra1=a%20value&extra2=%2fpath%2f'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': '/path/'})

    def test_connection_from_uri_with_encoded_password(self):
        uri = 'scheme://user:password%20with%20space@host%2flocation%3ax%3ay:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_encoded_user(self):
        uri = 'scheme://domain%2fuser:password@host%2flocation%3ax%3ay:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'domain/user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_encoded_schema(self):
        uri = 'scheme://user:password%20with%20space@host:1234/schema%2ftest'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host')
        self.assertEqual(connection.schema, 'schema/test')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_no_schema(self):
        uri = 'scheme://user:password%20with%20space@host:1234'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host')
        self.assertEqual(connection.schema, '')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_underscore(self):
        uri = 'google-cloud-platform://?extra__google_cloud_platform__key_' \
              'path=%2Fkeys%2Fkey.json&extra__google_cloud_platform__scope=' \
              'https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&extra' \
              '__google_cloud_platform__project=airflow'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'google_cloud_platform')
        self.assertEqual(connection.host, '')
        self.assertEqual(connection.schema, '')
        self.assertEqual(connection.login, None)
        self.assertEqual(connection.password, None)
        self.assertEqual(connection.extra_dejson, dict(
            extra__google_cloud_platform__key_path='/keys/key.json',
            extra__google_cloud_platform__project='airflow',
            extra__google_cloud_platform__scope='https://www.googleapis.com/'
                                                'auth/cloud-platform'))

    def test_connection_from_uri_without_authinfo(self):
        uri = 'scheme://host:1234'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host')
        self.assertEqual(connection.schema, '')
        self.assertEqual(connection.login, None)
        self.assertEqual(connection.password, None)
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_path(self):
        uri = 'scheme://%2FTmP%2F:1234'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, '/TmP/')
        self.assertEqual(connection.schema, '')
        self.assertEqual(connection.login, None)
        self.assertEqual(connection.password, None)
        self.assertEqual(connection.port, 1234)

    @parameterized.expand(
        [
            (
                "http://:password@host:80/database",
                ConnectionParts(
                    conn_type="http", login='', password="password", host="host", port=80, schema="database"
                ),
            ),
            (
                "http://user:@host:80/database",
                ConnectionParts(
                    conn_type="http", login="user", password=None, host="host", port=80, schema="database"
                ),
            ),
            (
                "http://user:password@/database",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="", port=None, schema="database"
                ),
            ),
            (
                "http://user:password@host:80/",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="host", port=80, schema=""
                ),
            ),
            (
                "http://user:password@/",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="", port=None, schema=""
                ),
            ),
            (
                "postgresql://user:password@%2Ftmp%2Fz6rqdzqh%2Fexample%3Awest1%3Atestdb/testdb",
                ConnectionParts(
                    conn_type="postgres",
                    login="user",
                    password="password",
                    host="/tmp/z6rqdzqh/example:west1:testdb",
                    port=None,
                    schema="testdb",
                ),
            ),
            (
                "postgresql://user@%2Ftmp%2Fz6rqdzqh%2Fexample%3Aeurope-west1%3Atestdb/testdb",
                ConnectionParts(
                    conn_type="postgres",
                    login="user",
                    password=None,
                    host="/tmp/z6rqdzqh/example:europe-west1:testdb",
                    port=None,
                    schema="testdb",
                ),
            ),
            (
                "postgresql://%2Ftmp%2Fz6rqdzqh%2Fexample%3Aeurope-west1%3Atestdb",
                ConnectionParts(
                    conn_type="postgres",
                    login=None,
                    password=None,
                    host="/tmp/z6rqdzqh/example:europe-west1:testdb",
                    port=None,
                    schema="",
                ),
            ),
        ]
    )
    def test_connection_from_with_auth_info(self, uri, uri_parts):
        connection = Connection(uri=uri)

        self.assertEqual(connection.conn_type, uri_parts.conn_type)
        self.assertEqual(connection.login, uri_parts.login)
        self.assertEqual(connection.password, uri_parts.password)
        self.assertEqual(connection.host, uri_parts.host)
        self.assertEqual(connection.port, uri_parts.port)
        self.assertEqual(connection.schema, uri_parts.schema)
