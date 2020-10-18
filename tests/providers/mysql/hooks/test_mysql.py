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
import os
import unittest
import uuid
from unittest import mock

import MySQLdb.cursors
import pytest
from parameterized import parameterized

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

SSL_DICT = {'cert': '/tmp/client-cert.pem', 'ca': '/tmp/server-ca.pem', 'key': '/tmp/client-key.pem'}


class TestMySqlHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(
            conn_type='mysql',
            login='login',
            password='password',
            host='host',
            schema='schema',
        )

        self.db_hook = MySqlHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('MySQLdb.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['user'], 'login')
        self.assertEqual(kwargs['passwd'], 'password')
        self.assertEqual(kwargs['host'], 'host')
        self.assertEqual(kwargs['db'], 'schema')

    @mock.patch('MySQLdb.connect')
    def test_get_uri(self, mock_connect):
        self.connection.extra = json.dumps({'charset': 'utf-8'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(self.db_hook.get_uri(), "mysql://login:password@host/schema?charset=utf-8")

    @mock.patch('MySQLdb.connect')
    def test_get_conn_from_connection(self, mock_connect):
        conn = Connection(login='login-conn', password='password-conn', host='host', schema='schema')
        hook = MySqlHook(connection=conn)
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login-conn', passwd='password-conn', host='host', db='schema', port=3306
        )

    @mock.patch('MySQLdb.connect')
    def test_get_conn_from_connection_with_schema(self, mock_connect):
        conn = Connection(login='login-conn', password='password-conn', host='host', schema='schema')
        hook = MySqlHook(connection=conn, schema='schema-override')
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login-conn', passwd='password-conn', host='host', db='schema-override', port=3306
        )

    @mock.patch('MySQLdb.connect')
    def test_get_conn_port(self, mock_connect):
        self.connection.port = 3307
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['port'], 3307)

    @mock.patch('MySQLdb.connect')
    def test_get_conn_charset(self, mock_connect):
        self.connection.extra = json.dumps({'charset': 'utf-8'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['charset'], 'utf-8')
        self.assertEqual(kwargs['use_unicode'], True)

    @mock.patch('MySQLdb.connect')
    def test_get_conn_cursor(self, mock_connect):
        self.connection.extra = json.dumps({'cursor': 'sscursor'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['cursorclass'], MySQLdb.cursors.SSCursor)

    @mock.patch('MySQLdb.connect')
    def test_get_conn_local_infile(self, mock_connect):
        self.connection.extra = json.dumps({'local_infile': True})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['local_infile'], 1)

    @mock.patch('MySQLdb.connect')
    def test_get_con_unix_socket(self, mock_connect):
        self.connection.extra = json.dumps({'unix_socket': "/tmp/socket"})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['unix_socket'], '/tmp/socket')

    @mock.patch('MySQLdb.connect')
    def test_get_conn_ssl_as_dictionary(self, mock_connect):
        self.connection.extra = json.dumps({'ssl': SSL_DICT})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['ssl'], SSL_DICT)

    @mock.patch('MySQLdb.connect')
    def test_get_conn_ssl_as_string(self, mock_connect):
        self.connection.extra = json.dumps({'ssl': json.dumps(SSL_DICT)})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['ssl'], SSL_DICT)

    @mock.patch('MySQLdb.connect')
    @mock.patch('airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.get_client_type')
    def test_get_conn_rds_iam(self, mock_client, mock_connect):
        self.connection.extra = '{"iam":true}'
        mock_client.return_value.generate_db_auth_token.return_value = 'aws_token'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login',
            passwd='aws_token',
            host='host',
            db='schema',
            port=3306,
            read_default_group='enable-cleartext-plugin',
        )


class TestMySqlHookConnMySqlConnectorPython(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(
            login='login',
            password='password',
            host='host',
            schema='schema',
            extra='{"client": "mysql-connector-python"}',
        )

        self.db_hook = MySqlHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('mysql.connector.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['user'], 'login')
        self.assertEqual(kwargs['password'], 'password')
        self.assertEqual(kwargs['host'], 'host')
        self.assertEqual(kwargs['database'], 'schema')

    @mock.patch('mysql.connector.connect')
    def test_get_conn_port(self, mock_connect):
        self.connection.port = 3307
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['port'], 3307)

    @mock.patch('mysql.connector.connect')
    def test_get_conn_allow_local_infile(self, mock_connect):
        extra_dict = self.connection.extra_dejson
        extra_dict.update(allow_local_infile=True)
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['allow_local_infile'], 1)


class TestMySqlHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class SubMySqlHook(MySqlHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = SubMySqlHook()

    def test_set_autocommit(self):
        autocommit = True
        self.db_hook.set_autocommit(self.conn, autocommit)

        self.conn.autocommit.assert_called_once_with(autocommit)

    def test_run_without_autocommit(self):
        sql = 'SQL'
        self.conn.get_autocommit.return_value = False

        # Default autocommit setting should be False.
        # Testing default autocommit value as well as run() behavior.
        self.db_hook.run(sql, autocommit=False)
        self.conn.autocommit.assert_called_once_with(False)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.call_count == 1

    def test_run_with_autocommit(self):
        sql = 'SQL'
        self.db_hook.run(sql, autocommit=True)
        self.conn.autocommit.assert_called_once_with(True)
        self.cur.execute.assert_called_once_with(sql)
        self.conn.commit.assert_not_called()

    def test_run_with_parameters(self):
        sql = 'SQL'
        parameters = ('param1', 'param2')
        self.db_hook.run(sql, autocommit=True, parameters=parameters)
        self.conn.autocommit.assert_called_once_with(True)
        self.cur.execute.assert_called_once_with(sql, parameters)
        self.conn.commit.assert_not_called()

    def test_run_multi_queries(self):
        sql = ['SQL1', 'SQL2']
        self.db_hook.run(sql, autocommit=True)
        self.conn.autocommit.assert_called_once_with(True)
        for i in range(len(self.cur.execute.call_args_list)):
            args, kwargs = self.cur.execute.call_args_list[i]
            self.assertEqual(len(args), 1)
            self.assertEqual(args[0], sql[i])
            self.assertEqual(kwargs, {})
        calls = [mock.call(sql[0]), mock.call(sql[1])]
        self.cur.execute.assert_has_calls(calls, any_order=True)
        self.conn.commit.assert_not_called()

    def test_bulk_load(self):
        self.db_hook.bulk_load('table', '/tmp/file')
        self.cur.execute.assert_called_once_with(
            """
            LOAD DATA LOCAL INFILE '/tmp/file'
            INTO TABLE table
            """
        )

    def test_bulk_dump(self):
        self.db_hook.bulk_dump('table', '/tmp/file')
        self.cur.execute.assert_called_once_with(
            """
            SELECT * INTO OUTFILE '/tmp/file'
            FROM table
            """
        )

    def test_serialize_cell(self):
        self.assertEqual('foo', self.db_hook._serialize_cell('foo', None))

    def test_bulk_load_custom(self):
        self.db_hook.bulk_load_custom(
            'table',
            '/tmp/file',
            'IGNORE',
            """FIELDS TERMINATED BY ';'
            OPTIONALLY ENCLOSED BY '"'
            IGNORE 1 LINES""",
        )
        self.cur.execute.assert_called_once_with(
            """
            LOAD DATA LOCAL INFILE '/tmp/file'
            IGNORE
            INTO TABLE table
            FIELDS TERMINATED BY ';'
            OPTIONALLY ENCLOSED BY '"'
            IGNORE 1 LINES
            """
        )


DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


class MySqlContext:
    def __init__(self, client):
        self.client = client
        self.connection = MySqlHook.get_connection(MySqlHook.default_conn_name)
        self.init_client = self.connection.extra_dejson.get('client', 'mysqlclient')

    def __enter__(self):
        self.connection.set_extra('{{"client": "{}"}}'.format(self.client))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.set_extra('{{"client": "{}"}}'.format(self.init_client))


@pytest.mark.backend("mysql")
class TestMySql(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        drop_tables = {'test_mysql_to_mysql', 'test_airflow'}
        with MySqlHook().get_conn() as conn:
            for table in drop_tables:
                conn.execute("DROP TABLE IF EXISTS {}".format(table))

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_AIRFLOW_DB': 'mysql://root@mysql/airflow?charset=utf8mb4&local_infile=1',
        },
    )
    def test_mysql_hook_test_bulk_load(self, client):
        with MySqlContext(client):
            records = ("foo", "bar", "baz")

            import tempfile

            with tempfile.NamedTemporaryFile() as f:
                f.write("\n".join(records).encode('utf8'))
                f.flush()

                hook = MySqlHook('airflow_db')
                with hook.get_conn() as conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS test_airflow (
                            dummy VARCHAR(50)
                        )
                    """
                    )
                    conn.execute("TRUNCATE TABLE test_airflow")
                    hook.bulk_load("test_airflow", f.name)
                    conn.execute("SELECT dummy FROM test_airflow")
                    results = tuple(result[0] for result in conn.fetchall())
                    self.assertEqual(sorted(results), sorted(records))

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    def test_mysql_hook_test_bulk_dump(self, client):
        with MySqlContext(client):
            hook = MySqlHook('airflow_db')
            priv = hook.get_first("SELECT @@global.secure_file_priv")
            # Use random names to alllow re-running
            if priv and priv[0]:
                # Confirm that no error occurs
                hook.bulk_dump(
                    "INFORMATION_SCHEMA.TABLES",
                    os.path.join(priv[0], "TABLES_{}-{}".format(client, uuid.uuid1())),
                )
            elif priv == ("",):
                hook.bulk_dump("INFORMATION_SCHEMA.TABLES", "TABLES_{}_{}".format(client, uuid.uuid1()))
            else:
                self.skipTest("Skip test_mysql_hook_test_bulk_load " "since file output is not permitted")

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    @mock.patch('airflow.providers.mysql.hooks.mysql.MySqlHook.get_conn')
    def test_mysql_hook_test_bulk_dump_mock(self, client, mock_get_conn):
        with MySqlContext(client):
            mock_execute = mock.MagicMock()
            mock_get_conn.return_value.cursor.return_value.execute = mock_execute

            hook = MySqlHook('airflow_db')
            table = "INFORMATION_SCHEMA.TABLES"
            tmp_file = "/path/to/output/file"
            hook.bulk_dump(table, tmp_file)

            from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces

            assert mock_execute.call_count == 1
            query = """
                SELECT * INTO OUTFILE '{tmp_file}'
                FROM {table}
            """.format(
                tmp_file=tmp_file, table=table
            )
            assert_equal_ignore_multiple_spaces(self, mock_execute.call_args[0][0], query)
