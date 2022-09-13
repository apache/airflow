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
from __future__ import annotations

import unittest
from unittest import mock

from elasticsearch import Elasticsearch

from airflow.models import Connection
from airflow.providers.elasticsearch.hooks.elasticsearch import (
    ElasticsearchHook,
    ElasticsearchPythonHook,
    ElasticsearchSQLHook,
)


class TestElasticsearchHook(unittest.TestCase):
    def test_throws_warning(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn
        self.connection = Connection(host='localhost', port=9200, schema='http')

        with self.assertWarns(DeprecationWarning):

            class UnitTestElasticsearchHook(ElasticsearchHook):
                conn_name_attr = 'test_conn_id'

                def get_conn(self):
                    return conn

            self.db_hook = UnitTestElasticsearchHook()


class TestElasticsearchSQLHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(host='localhost', port=9200, schema='http')

        class UnitTestElasticsearchHook(ElasticsearchSQLHook):
            conn_name_attr = 'elasticsearch_conn_id'

        self.db_hook = UnitTestElasticsearchHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('airflow.providers.elasticsearch.hooks.elasticsearch.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.test_conn_id = 'non_default'
        self.db_hook.get_conn()
        mock_connect.assert_called_with(host='localhost', port=9200, scheme='http', user=None, password=None)


class TestElasticsearcSQLhHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestElasticsearchHook(ElasticsearchSQLHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = UnitTestElasticsearchHook()

    def test_get_first_record(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook.get_first(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = 'SQL'
        result_sets = [('row1',), ('row2',)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.get_records(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_pandas_df(self):
        statement = 'SQL'
        column = 'col'
        result_sets = [('row1',), ('row2',)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

        self.cur.execute.assert_called_once_with(statement)


class MockElasticsearch:
    def __init__(self, data: dict):
        self.data = data

    def search(self, **kwargs):
        return self.data


class TestElasticsearchPythonHook:
    def setup(self):
        self.elasticsearch_hook = ElasticsearchPythonHook(hosts=["http://localhost:9200"])

    def test_client(self):
        es_connection = self.elasticsearch_hook.get_conn
        assert isinstance(es_connection, Elasticsearch)

    @mock.patch(
        "airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchPythonHook._get_elastic_connection"
    )
    def test_search(self, elastic_mock):
        es_data = {"hits": "test_hit"}
        es_client = MockElasticsearch(es_data)
        elastic_mock.return_value = es_client

        query = {"test_query": "test_filter"}

        result = self.elasticsearch_hook.search(index="test_index", query=query)

        assert result == es_data['hits']
