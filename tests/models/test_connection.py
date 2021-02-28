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
import json
import re
import unittest
from collections import namedtuple
from unittest import mock

import pytest
import sqlalchemy
from cryptography.fernet import Fernet
from parameterized import parameterized

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection, crypto
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from tests.test_utils.config import conf_vars

ConnectionParts = namedtuple("ConnectionParts", ["conn_type", "login", "password", "host", "port", "schema"])


class UriTestCaseConfig:
    def __init__(
        self,
        test_conn_uri: str,
        test_conn_attributes: dict,
        description: str,
    ):
        """

        :param test_conn_uri: URI that we use to create connection
        :param test_conn_attributes: we expect a connection object created with `test_uri` to have these
        attributes
        :param description: human-friendly name appended to parameterized test
        """
        self.test_uri = test_conn_uri
        self.test_conn_attributes = test_conn_attributes
        self.description = description

    @staticmethod
    def uri_test_name(func, num, param):
        return f"{func.__name__}_{num}_{param.args[0].description.replace(' ', '_')}"


class TestConnection(unittest.TestCase):
    def setUp(self):
        crypto._fernet = None

    def tearDown(self):
        crypto._fernet = None

    @conf_vars({('core', 'fernet_key'): ''})
    def test_connection_extra_no_encryption(self):
        """
        Tests extras on a new connection without encryption. The fernet key
        is set to a non-base64-encoded string and the extra is stored without
        encryption.
        """
        test_connection = Connection(extra='testextra')
        assert not test_connection.is_extra_encrypted
        assert test_connection.extra == 'testextra'

    @conf_vars({('core', 'fernet_key'): Fernet.generate_key().decode()})
    def test_connection_extra_with_encryption(self):
        """
        Tests extras on a new connection with encryption.
        """
        test_connection = Connection(extra='testextra')
        assert test_connection.is_extra_encrypted
        assert test_connection.extra == 'testextra'

    def test_connection_extra_with_encryption_rotate_fernet_key(self):
        """
        Tests rotating encrypted extras.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({('core', 'fernet_key'): key1.decode()}):
            test_connection = Connection(extra='testextra')
            assert test_connection.is_extra_encrypted
            assert test_connection.extra == 'testextra'
            assert Fernet(key1).decrypt(test_connection._extra.encode()) == b'testextra'

        # Test decrypt of old value with new key
        with conf_vars({('core', 'fernet_key'): ','.join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            assert test_connection.extra == 'testextra'

            # Test decrypt of new value with new key
            test_connection.rotate_fernet_key()
            assert test_connection.is_extra_encrypted
            assert test_connection.extra == 'testextra'
            assert Fernet(key2).decrypt(test_connection._extra.encode()) == b'testextra'

    test_from_uri_params = [
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password@host%2Flocation:1234/schema',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location',
                schema='schema',
                login='user',
                password='password',
                port=1234,
                extra=None,
            ),
            description='without extras',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password@host%2Flocation:1234/schema?'
            'extra1=a%20value&extra2=%2Fpath%2F',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location',
                schema='schema',
                login='user',
                password='password',
                port=1234,
                extra_dejson={'extra1': 'a value', 'extra2': '/path/'},
            ),
            description='with extras',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password@host%2Flocation:1234/schema?extra1=a%20value&extra2=',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location',
                schema='schema',
                login='user',
                password='password',
                port=1234,
                extra_dejson={'extra1': 'a value', 'extra2': ''},
            ),
            description='with empty extras',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password@host%2Flocation%3Ax%3Ay:1234/schema?'
            'extra1=a%20value&extra2=%2Fpath%2F',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location:x:y',
                schema='schema',
                login='user',
                password='password',
                port=1234,
                extra_dejson={'extra1': 'a value', 'extra2': '/path/'},
            ),
            description='with colon in hostname',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password%20with%20space@host%2Flocation%3Ax%3Ay:1234/schema',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location:x:y',
                schema='schema',
                login='user',
                password='password with space',
                port=1234,
            ),
            description='with encoded password',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://domain%2Fuser:password@host%2Flocation%3Ax%3Ay:1234/schema',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host/location:x:y',
                schema='schema',
                login='domain/user',
                password='password',
                port=1234,
            ),
            description='with encoded user',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password%20with%20space@host:1234/schema%2Ftest',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host',
                schema='schema/test',
                login='user',
                password='password with space',
                port=1234,
            ),
            description='with encoded schema',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://user:password%20with%20space@host:1234',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host',
                schema='',
                login='user',
                password='password with space',
                port=1234,
            ),
            description='no schema',
        ),
        UriTestCaseConfig(
            test_conn_uri='google-cloud-platform://?extra__google_cloud_platform__key_'
            'path=%2Fkeys%2Fkey.json&extra__google_cloud_platform__scope='
            'https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&extra'
            '__google_cloud_platform__project=airflow',
            test_conn_attributes=dict(
                conn_type='google_cloud_platform',
                host='',
                schema='',
                login=None,
                password=None,
                port=None,
                extra_dejson=dict(
                    extra__google_cloud_platform__key_path='/keys/key.json',
                    extra__google_cloud_platform__scope='https://www.googleapis.com/auth/cloud-platform',
                    extra__google_cloud_platform__project='airflow',
                ),
            ),
            description='with underscore',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://host:1234',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='host',
                schema='',
                login=None,
                password=None,
                port=1234,
            ),
            description='without auth info',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://%2FTmP%2F:1234',
            test_conn_attributes=dict(
                conn_type='scheme',
                host='/TmP/',
                schema='',
                login=None,
                password=None,
                port=1234,
            ),
            description='with path',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme:///airflow',
            test_conn_attributes=dict(
                conn_type='scheme',
                schema='airflow',
            ),
            description='schema only',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://@:1234',
            test_conn_attributes=dict(
                conn_type='scheme',
                port=1234,
            ),
            description='port only',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://:password%2F%21%40%23%24%25%5E%26%2A%28%29%7B%7D@',
            test_conn_attributes=dict(
                conn_type='scheme',
                password='password/!@#$%^&*(){}',
            ),
            description='password only',
        ),
        UriTestCaseConfig(
            test_conn_uri='scheme://login%2F%21%40%23%24%25%5E%26%2A%28%29%7B%7D@',
            test_conn_attributes=dict(
                conn_type='scheme',
                login='login/!@#$%^&*(){}',
            ),
            description='login only',
        ),
    ]

    # pylint: disable=undefined-variable
    @parameterized.expand([(x,) for x in test_from_uri_params], UriTestCaseConfig.uri_test_name)
    def test_connection_from_uri(self, test_config: UriTestCaseConfig):

        connection = Connection(uri=test_config.test_uri)
        for conn_attr, expected_val in test_config.test_conn_attributes.items():
            actual_val = getattr(connection, conn_attr)
            if expected_val is None:
                assert expected_val is None
            if isinstance(expected_val, dict):
                assert expected_val == actual_val
            else:
                assert expected_val == actual_val

    # pylint: disable=undefined-variable
    @parameterized.expand([(x,) for x in test_from_uri_params], UriTestCaseConfig.uri_test_name)
    def test_connection_get_uri_from_uri(self, test_config: UriTestCaseConfig):
        """
        This test verifies that when we create a conn_1 from URI, and we generate a URI from that conn, that
        when we create a conn_2 from the generated URI, we get an equivalent conn.
        1. Parse URI to create `Connection` object, `connection`.
        2. Using this connection, generate URI `generated_uri`..
        3. Using this`generated_uri`, parse and create new Connection `new_conn`.
        4. Verify that `new_conn` has same attributes as `connection`.
        """
        connection = Connection(uri=test_config.test_uri)
        generated_uri = connection.get_uri()
        new_conn = Connection(uri=generated_uri)
        assert connection.conn_type == new_conn.conn_type
        assert connection.login == new_conn.login
        assert connection.password == new_conn.password
        assert connection.host == new_conn.host
        assert connection.port == new_conn.port
        assert connection.schema == new_conn.schema
        assert connection.extra_dejson == new_conn.extra_dejson

    # pylint: disable=undefined-variable
    @parameterized.expand([(x,) for x in test_from_uri_params], UriTestCaseConfig.uri_test_name)
    def test_connection_get_uri_from_conn(self, test_config: UriTestCaseConfig):
        """
        This test verifies that if we create conn_1 from attributes (rather than from URI), and we generate a
        URI, that when we create conn_2 from this URI, we get an equivalent conn.
        1. Build conn init params using `test_conn_attributes` and store in `conn_kwargs`
        2. Instantiate conn `connection` from `conn_kwargs`.
        3. Generate uri `get_uri` from this conn.
        4. Create conn `new_conn` from this uri.
        5. Verify `new_conn` has same attributes as `connection`.
        """
        conn_kwargs = {}
        for k, v in test_config.test_conn_attributes.items():
            if k == 'extra_dejson':
                conn_kwargs.update({'extra': json.dumps(v)})
            else:
                conn_kwargs.update({k: v})

        connection = Connection(conn_id='test_conn', **conn_kwargs)  # type: ignore
        gen_uri = connection.get_uri()
        new_conn = Connection(conn_id='test_conn', uri=gen_uri)
        for conn_attr, expected_val in test_config.test_conn_attributes.items():
            actual_val = getattr(new_conn, conn_attr)
            if expected_val is None:
                assert expected_val is None
            if isinstance(expected_val, dict):
                assert expected_val == actual_val
            else:
                assert expected_val == actual_val

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

        assert connection.conn_type == uri_parts.conn_type
        assert connection.login == uri_parts.login
        assert connection.password == uri_parts.password
        assert connection.host == uri_parts.host
        assert connection.port == uri_parts.port
        assert connection.schema == uri_parts.schema

    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
        },
    )
    def test_using_env_var(self):
        conn = SqliteHook.get_connection(conn_id='test_uri')
        assert 'ec2.compute.com' == conn.host
        assert 'the_database' == conn.schema
        assert 'username' == conn.login
        assert 'password' == conn.password
        assert 5432 == conn.port

    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
        },
    )
    def test_using_unix_socket_env_var(self):
        conn = SqliteHook.get_connection(conn_id='test_uri_no_creds')
        assert 'ec2.compute.com' == conn.host
        assert 'the_database' == conn.schema
        assert conn.login is None
        assert conn.password is None
        assert conn.port is None

    def test_param_setup(self):
        conn = Connection(
            conn_id='local_mysql',
            conn_type='mysql',
            host='localhost',
            login='airflow',
            password='airflow',
            schema='airflow',
        )
        assert 'localhost' == conn.host
        assert 'airflow' == conn.schema
        assert 'airflow' == conn.login
        assert 'airflow' == conn.password
        assert conn.port is None

    def test_env_var_priority(self):
        conn = SqliteHook.get_connection(conn_id='airflow_db')
        assert 'ec2.compute.com' != conn.host

        with mock.patch.dict(
            'os.environ',
            {
                'AIRFLOW_CONN_AIRFLOW_DB': 'postgres://username:password@ec2.compute.com:5432/the_database',
            },
        ):
            conn = SqliteHook.get_connection(conn_id='airflow_db')
            assert 'ec2.compute.com' == conn.host
            assert 'the_database' == conn.schema
            assert 'username' == conn.login
            assert 'password' == conn.password
            assert 5432 == conn.port

    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
            'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
        },
    )
    def test_dbapi_get_uri(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        assert 'postgres://username:password@ec2.compute.com:5432/the_database' == hook.get_uri()
        conn2 = BaseHook.get_connection(conn_id='test_uri_no_creds')
        hook2 = conn2.get_hook()
        assert 'postgres://ec2.compute.com/the_database' == hook2.get_uri()

    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
            'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
        },
    )
    def test_dbapi_get_sqlalchemy_engine(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        engine = hook.get_sqlalchemy_engine()
        assert isinstance(engine, sqlalchemy.engine.Engine)
        assert 'postgres://username:password@ec2.compute.com:5432/the_database' == str(engine.url)

    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
            'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
        },
    )
    def test_get_connections_env_var(self):
        conns = SqliteHook.get_connections(conn_id='test_uri')
        assert len(conns) == 1
        assert conns[0].host == 'ec2.compute.com'
        assert conns[0].schema == 'the_database'
        assert conns[0].login == 'username'
        assert conns[0].password == 'password'
        assert conns[0].port == 5432

    def test_connection_mixed(self):
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "You must create an object using the URI or individual values (conn_type, host, login, "
                "password, schema, port or extra).You can't mix these two ways to create this object."
            ),
        ):
            Connection(conn_id="TEST_ID", uri="mysql://", schema="AAA")
