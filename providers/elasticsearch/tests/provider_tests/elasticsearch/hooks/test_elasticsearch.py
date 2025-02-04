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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from elasticsearch import Elasticsearch

from airflow.models import Connection
from airflow.providers.elasticsearch.hooks.elasticsearch import (
    ElasticsearchPythonHook,
    ElasticsearchSQLCursor,
    ElasticsearchSQLHook,
    ESConnection,
)
from elasticsearch._sync.client import SqlClient


class TestElasticsearchSQLHookConn:
    def setup_method(self):
        self.connection = Connection(host="localhost", port=9200, schema="http")

        class UnitTestElasticsearchSQLHook(ElasticsearchSQLHook):
            conn_name_attr = "elasticsearch_conn_id"

        self.db_hook = UnitTestElasticsearchSQLHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("airflow.providers.elasticsearch.hooks.elasticsearch.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.test_conn_id = "non_default"
        self.db_hook.get_conn()
        mock_connect.assert_called_with(host="localhost", port=9200, scheme="http", user=None, password=None)


class TestElasticsearchSQLCursor:
    rows = [
        [1, "Stallone", "Sylvester", "78"],
        [2, "Statham", "Jason", "57"],
        [3, "Li", "Jet", "61"],
        [4, "Lundgren", "Dolph", "66"],
        [5, "Norris", "Chuck", "84"],
    ]
    columns = [
        {'name': 'index', 'type': 'long'},
        {'name': 'name', 'type': 'text'},
        {'name': 'firstname', 'type': 'text'},
        {'name': 'age', 'type': 'long'},
    ]
    response = {
        "columns": columns,
        "rows": rows
    }

    def setup_method(self):
        sql = MagicMock(spec=SqlClient)
        sql.query.side_effect = lambda body: self.response
        self.es = MagicMock(sql=sql, spec=Elasticsearch)

    def test_execute(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})

        assert cursor.execute("SELECT * FROM hollywood.actors") == self.response

    def test_rowcount(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.rowcount == len(self.rows)

    def test_description(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.description == self.columns

    def test_fetchone(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.fetchone() == self.rows[0]

    def test_fetchmany(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        with pytest.raises(NotImplementedError):
            cursor.fetchmany()

    def test_fetchall(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.fetchall() == self.rows


class TestElasticsearchSQLHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0, spec=ElasticsearchSQLCursor)
        self.conn = mock.MagicMock(spec=ESConnection)
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestElasticsearchSQLHook(ElasticsearchSQLHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestElasticsearchSQLHook()

    def test_get_first_record(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook.get_first(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.get_records(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_pandas_df(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

        self.cur.execute.assert_called_once_with(statement)

    @mock.patch("airflow.providers.elasticsearch.hooks.elasticsearch.Elasticsearch")
    def test_execute_sql_query(self, mock_es):
        mock_es_sql_client = MagicMock()
        mock_es_sql_client.query.return_value = {
            "columns": [{"name": "id"}, {"name": "first_name"}],
            "rows": [[1, "John"], [2, "Jane"]],
        }
        mock_es.return_value.sql = mock_es_sql_client

        es_connection = ESConnection(host="localhost", port=9200)
        response = es_connection.execute_sql("SELECT * FROM index1")
        mock_es_sql_client.query.assert_called_once_with(body={"query": "SELECT * FROM index1"})

        assert response["rows"] == [[1, "John"], [2, "Jane"]]
        assert response["columns"] == [{"name": "id"}, {"name": "first_name"}]


class MockElasticsearch:
    def __init__(self, data: dict):
        self.data = data

    def search(self, **kwargs):
        return self.data


class TestElasticsearchPythonHook:
    def setup_method(self):
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

        assert result == es_data["hits"]
