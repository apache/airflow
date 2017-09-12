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

import mock
import unittest
import os

from airflow import settings, models
from airflow.settings import Session
from airflow.hooks.sqlite_hook import SqliteHook


@unittest.skipIf(not settings.SQL_ALCHEMY_CONN.startswith('sqlite'), 'SqliteHook won\'t work without backend SQLite. No need to test anything here')
class TestSqliteHook(unittest.TestCase):

    CONN_ID = 'sqlite_hook_test'

    @classmethod
    def setUpClass(cls):
        super(TestSqliteHook, cls).setUpClass()
        session = Session()
        session.query(models.Connection).filter_by(conn_id=cls.CONN_ID).delete()
        session.commit()
        connection = models.Connection(conn_id=cls.CONN_ID, uri=settings.SQL_ALCHEMY_CONN)
        session.add(connection)
        session.commit()
        session.close()

    def test_sql_hook(self):
        hook = SqliteHook(sqlite_conn_id=self.CONN_ID)
        conn_id, = hook.get_first('SELECT conn_id FROM connection WHERE conn_id = :conn_id',
                                  {'conn_id': self.CONN_ID})
        self.assertEqual(conn_id, self.CONN_ID)

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.Connection).filter_by(conn_id=cls.CONN_ID).delete()
        session.commit()
        session.close()
        super(TestSqliteHook, cls).tearDownClass()
