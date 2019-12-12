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

import json
import unittest
from datetime import datetime

import numpy

from airflow.hooks.oracle_hook import OracleHook
from airflow.models import Connection
from tests.compat import mock

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None


@unittest.skipIf(cx_Oracle is None, 'cx_Oracle package not present')
class TestOracleHookConn(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.connection = Connection(
            login='login',
            password='password',
            host='host',
            port=1521
        )

        self.db_hook = OracleHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_host(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['user'], 'login')
        self.assertEqual(kwargs['password'], 'password')
        self.assertEqual(kwargs['dsn'], 'host')

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_sid(self, mock_connect):
        dsn_sid = {'dsn': 'dsn', 'sid': 'sid'}
        self.connection.extra = json.dumps(dsn_sid)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['dsn'],
                         cx_Oracle.makedsn(dsn_sid['dsn'],
                                           self.connection.port, dsn_sid['sid']))

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_service_name(self, mock_connect):
        dsn_service_name = {'dsn': 'dsn', 'service_name': 'service_name'}
        self.connection.extra = json.dumps(dsn_service_name)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['dsn'], cx_Oracle.makedsn(
            dsn_service_name['dsn'], self.connection.port,
            service_name=dsn_service_name['service_name']))

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_encoding_without_nencoding(self, mock_connect):
        self.connection.extra = json.dumps({'encoding': 'UTF-8'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['encoding'], 'UTF-8')
        self.assertEqual(kwargs['nencoding'], 'UTF-8')

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_encoding_with_nencoding(self, mock_connect):
        self.connection.extra = json.dumps({'encoding': 'UTF-8', 'nencoding': 'gb2312'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['encoding'], 'UTF-8')
        self.assertEqual(kwargs['nencoding'], 'gb2312')

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_nencoding(self, mock_connect):
        self.connection.extra = json.dumps({'nencoding': 'UTF-8'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertNotIn('encoding', kwargs)
        self.assertEqual(kwargs['nencoding'], 'UTF-8')

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_mode(self, mock_connect):
        mode = {
            'sysdba': cx_Oracle.SYSDBA,
            'sysasm': cx_Oracle.SYSASM,
            'sysoper': cx_Oracle.SYSOPER,
            'sysbkp': cx_Oracle.SYSBKP,
            'sysdgd': cx_Oracle.SYSDGD,
            'syskmt': cx_Oracle.SYSKMT,
        }
        first = True
        for m in mode:
            self.connection.extra = json.dumps({'mode': m})
            self.db_hook.get_conn()
            if first:
                assert mock_connect.call_count == 1
                first = False
            args, kwargs = mock_connect.call_args
            self.assertEqual(args, ())
            self.assertEqual(kwargs['mode'], mode.get(m))

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_threaded(self, mock_connect):
        self.connection.extra = json.dumps({'threaded': True})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['threaded'], True)

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_events(self, mock_connect):
        self.connection.extra = json.dumps({'events': True})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['events'], True)

    @mock.patch('airflow.hooks.oracle_hook.cx_Oracle.connect')
    def test_get_conn_purity(self, mock_connect):
        purity = {
            'new': cx_Oracle.ATTR_PURITY_NEW,
            'self': cx_Oracle.ATTR_PURITY_SELF,
            'default': cx_Oracle.ATTR_PURITY_DEFAULT
        }
        first = True
        for p in purity:
            self.connection.extra = json.dumps({'purity': p})
            self.db_hook.get_conn()
            if first:
                assert mock_connect.call_count == 1
                first = False
            args, kwargs = mock_connect.call_args
            self.assertEqual(args, ())
            self.assertEqual(kwargs['purity'], purity.get(p))


@unittest.skipIf(cx_Oracle is None, 'cx_Oracle package not present')
class TestOracleHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestOracleHook(OracleHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestOracleHook()

    def test_run_without_parameters(self):
        sql = 'SQL'
        self.db_hook.run(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_with_parameters(self):
        sql = 'SQL'
        param = ('p1', 'p2')
        self.db_hook.run(sql, parameters=param)
        self.cur.execute.assert_called_once_with(sql, param)
        assert self.conn.commit.called

    def test_insert_rows_with_fields(self):
        rows = [("'basestr_with_quote", None, numpy.NAN,
                 numpy.datetime64('2019-01-24T01:02:03'),
                 datetime(2019, 1, 24), 1, 10.24, 'str')]
        target_fields = ['basestring', 'none', 'numpy_nan', 'numpy_datetime64',
                         'datetime', 'int', 'float', 'str']
        self.db_hook.insert_rows('table', rows, target_fields)
        self.cur.execute.assert_called_once_with(
            "INSERT /*+ APPEND */ INTO table "
            "(basestring, none, numpy_nan, numpy_datetime64, datetime, int, float, str) "
            "VALUES ('''basestr_with_quote',NULL,NULL,'2019-01-24T01:02:03',"
            "to_date('2019-01-24 00:00:00','YYYY-MM-DD HH24:MI:SS'),1,10.24,'str')")

    def test_insert_rows_without_fields(self):
        rows = [("'basestr_with_quote", None, numpy.NAN,
                 numpy.datetime64('2019-01-24T01:02:03'),
                 datetime(2019, 1, 24), 1, 10.24, 'str')]
        self.db_hook.insert_rows('table', rows)
        self.cur.execute.assert_called_once_with(
            "INSERT /*+ APPEND */ INTO table "
            " VALUES ('''basestr_with_quote',NULL,NULL,'2019-01-24T01:02:03',"
            "to_date('2019-01-24 00:00:00','YYYY-MM-DD HH24:MI:SS'),1,10.24,'str')")

    def test_bulk_insert_rows_with_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ['col1', 'col2', 'col3']
        self.db_hook.bulk_insert_rows('table', rows, target_fields)
        self.cur.prepare.assert_called_once_with(
            "insert into table (col1, col2, col3) values (:1, :2, :3)")
        self.cur.executemany.assert_called_once_with(None, rows)

    def test_bulk_insert_rows_with_commit_every(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ['col1', 'col2', 'col3']
        self.db_hook.bulk_insert_rows('table', rows, target_fields, commit_every=2)
        calls = [
            mock.call("insert into table (col1, col2, col3) values (:1, :2, :3)"),
            mock.call("insert into table (col1, col2, col3) values (:1, :2, :3)"),
        ]
        self.cur.prepare.assert_has_calls(calls)
        calls = [
            mock.call(None, rows[:2]),
            mock.call(None, rows[2:]),
        ]
        self.cur.executemany.assert_has_calls(calls, any_order=True)

    def test_bulk_insert_rows_without_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        self.db_hook.bulk_insert_rows('table', rows)
        self.cur.prepare.assert_called_once_with(
            "insert into table  values (:1, :2, :3)")
        self.cur.executemany.assert_called_once_with(None, rows)

    def test_bulk_insert_rows_no_rows(self):
        rows = []
        self.assertRaises(ValueError, self.db_hook.bulk_insert_rows, 'table', rows)
