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
import logging
import unittest
import mock
from mock import MagicMock, Mock
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks.postgres_hook import PostgresHook

class TestDbApiHook(unittest.TestCase):

    def test_set_autocommit(self):     
        hook = JdbcHook(jdbc_conn_id='jdbc_default')
        conn = MagicMock(name='conn')
        conn.jconn.setAutoCommit = Mock(return_value=None)

        hook.set_autocommit(conn, False)
        conn.jconn.setAutoCommit.assert_called_with(False)

        hook.set_autocommit(conn, True)
        conn.jconn.setAutoCommit.assert_called_with(True)

    def test_autocommit(self):
        logging.info("Test autocommit when connection supports autocommit")
        jdbc_hook = JdbcHook(jdbc_conn_id='jdbc_default')
        jdbc_hook.run(sql='SELECT 1', autocommit=True)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=True')
        jdbc_hook.run(sql='SELECT 1', autocommit=False)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=False')

    def test_autocommit_unsupported(self):
        logging.info("Test autocommit when connection doesn't support autocommit")
        pg_hook = PostgresHook(conn_id='postgres_default')
        pg_hook.run(sql='SELECT 1', autocommit=True)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=True')
        pg_hook.run(sql='SELECT 1', autocommit=False)
        self.assertTrue('Query ran success with supports_autocommit=True, autocommit=False')
        
if __name__ == '__main__':
    unittest.main()
