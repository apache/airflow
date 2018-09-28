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
import mock
import unittest

import MySQLdb.cursors

from airflow import models
from airflow.hooks.mysql_hook import MySqlHook


class TestMySqlHookConn(unittest.TestCase):

    def setUp(self):
        super(TestMySqlHookConn, self).setUp()

        self.connection = models.Connection(
            login='login',
            password='password',
            host='host',
            schema='schema',
        )

        self.db_hook = MySqlHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('airflow.hooks.mysql_hook.MySQLdb.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['user'], 'login')
        self.assertEqual(kwargs['passwd'], 'password')
        self.assertEqual(kwargs['host'], 'host')
        self.assertEqual(kwargs['db'], 'schema')

    @mock.patch('airflow.hooks.mysql_hook.MySQLdb.connect')
    def test_get_conn_port(self, mock_connect):
        self.connection.port = 3307
        self.db_hook.get_conn()
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['port'], 3307)

    @mock.patch('airflow.hooks.mysql_hook.MySQLdb.connect')
    def test_get_conn_charset(self, mock_connect):
        self.connection.extra = json.dumps({'charset': 'utf-8'})
        self.db_hook.get_conn()
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['charset'], 'utf-8')
        self.assertEqual(kwargs['use_unicode'], True)

    @mock.patch('airflow.hooks.mysql_hook.MySQLdb.connect')
    def test_get_conn_cursor(self, mock_connect):
        self.connection.extra = json.dumps({'cursor': 'sscursor'})
        self.db_hook.get_conn()
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['cursorclass'], MySQLdb.cursors.SSCursor)

    @mock.patch('airflow.hooks.mysql_hook.MySQLdb.connect')
    def test_get_conn_local_infile(self, mock_connect):
        self.connection.extra = json.dumps({'local_infile': True})
        self.db_hook.get_conn()
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['local_infile'], 1)


class TestMySqlHook(unittest.TestCase):

    def setUp(self):
        super(TestMySqlHook, self).setUp()

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
        self.conn.commit.assert_called_once()

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
        self.cur.execute.assert_called_with(sql[1])
        self.conn.commit.assert_not_called()

    def test_bulk_load(self):
        self.db_hook.bulk_load('table', '/tmp/file')
        self.cur.execute.assert_called_once_with("""
            LOAD DATA LOCAL INFILE '/tmp/file'
            INTO TABLE table
            """)

    def test_bulk_dump(self):
        self.db_hook.bulk_dump('table', '/tmp/file')
        self.cur.execute.assert_called_once_with("""
            SELECT * INTO OUTFILE '/tmp/file'
            FROM table
            """)

    def test_serialize_cell(self):
        self.assertEqual('foo', self.db_hook._serialize_cell('foo', None))
