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
from elasticsearch._sync.client import SqlClient
from kgb import SpyAgency

from airflow.models import Connection
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.elasticsearch.hooks.elasticsearch import (
    ElasticsearchPythonHook,
    ElasticsearchSQLCursor,
    ElasticsearchSQLHook,
    ESConnection,
)

ROWS = [
    [1, "Stallone", "Sylvester", "78"],
    [2, "Statham", "Jason", "57"],
    [3, "Li", "Jet", "61"],
    [4, "Lundgren", "Dolph", "66"],
    [5, "Norris", "Chuck", "84"],
]
RESPONSE_WITHOUT_CURSOR = {
    "columns": [
        {"name": "index", "type": "long"},
        {"name": "name", "type": "text"},
        {"name": "firstname", "type": "text"},
        {"name": "age", "type": "long"},
    ],
    "rows": ROWS,
}
RESPONSE = {**RESPONSE_WITHOUT_CURSOR, **{"cursor": "e7f8QwXUruW2mIebzudH4BwAA//8DAA=="}}
RESPONSES = [
    RESPONSE,
    RESPONSE_WITHOUT_CURSOR,
]


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
    def setup_method(self):
        sql = MagicMock(spec=SqlClient)
        sql.query.side_effect = RESPONSES
        self.es = MagicMock(sql=sql, spec=Elasticsearch)

    def test_execute(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})

        assert cursor.execute("SELECT * FROM hollywood.actors") == RESPONSE

    def test_rowcount(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.rowcount == len(ROWS)

    def test_description(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.description == [
            ("index", "long"),
            ("name", "text"),
            ("firstname", "text"),
            ("age", "long"),
        ]

    def test_fetchone(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        assert cursor.fetchone() == ROWS[0]

    def test_fetchmany(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        with pytest.raises(NotImplementedError):
            cursor.fetchmany()

    def test_fetchall(self):
        cursor = ElasticsearchSQLCursor(es=self.es, options={})
        cursor.execute("SELECT * FROM hollywood.actors")

        records = cursor.fetchall()

        assert len(records) == 10
        assert records == ROWS


class TestElasticsearchSQLHook:
    def setup_method(self):
        sql = MagicMock(spec=SqlClient)
        sql.query.side_effect = RESPONSES
        es = MagicMock(sql=sql, spec=Elasticsearch)
        self.cur = ElasticsearchSQLCursor(es=es, options={})
        self.spy_agency = SpyAgency()
        self.spy_agency.spy_on(self.cur.close, call_original=True)
        self.spy_agency.spy_on(self.cur.execute, call_original=True)
        self.spy_agency.spy_on(self.cur.fetchall, call_original=True)
        self.conn = MagicMock(spec=ESConnection)
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestElasticsearchSQLHook(ElasticsearchSQLHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestElasticsearchSQLHook()

    def test_get_first_record(self):
        statement = "SELECT * FROM hollywood.actors"

        assert self.db_hook.get_first(statement) == ROWS[0]

        self.conn.close.assert_called_once_with()
        self.spy_agency.assert_spy_called(self.cur.close)
        self.spy_agency.assert_spy_called(self.cur.execute)

    def test_get_records(self):
        statement = "SELECT * FROM hollywood.actors"

        assert self.db_hook.get_records(statement) == ROWS

        self.conn.close.assert_called_once_with()
        self.spy_agency.assert_spy_called(self.cur.close)
        self.spy_agency.assert_spy_called(self.cur.execute)

    def test_get_df_pandas(self):
        statement = "SELECT * FROM hollywood.actors"
        df = self.db_hook.get_df(statement, df_type="pandas")

        assert list(df.columns) == ["index", "name", "firstname", "age"]
        assert df.values.tolist() == ROWS

        self.conn.close.assert_called_once_with()
        self.spy_agency.assert_spy_called(self.cur.close)
        self.spy_agency.assert_spy_called(self.cur.execute)

    def test_get_df_polars(self):
        with pytest.raises(NotImplementedError):
            self.db_hook.get_df("SQL", df_type="polars")

    def test_run(self):
        statement = "SELECT * FROM hollywood.actors"

        assert self.db_hook.run(statement, handler=fetch_all_handler) == ROWS

        self.conn.close.assert_called_once_with()
        self.spy_agency.assert_spy_called(self.cur.close)
        self.spy_agency.assert_spy_called(self.cur.execute)

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_run_hook_lineage(self, mock_send_lineage):
        statement = "SELECT * FROM hollywood.actors"
        self.db_hook.run(statement, handler=fetch_all_handler)

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == statement
        assert call_kw["sql_parameters"] is None
        assert call_kw["cur"] is self.cur

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df")
    def test_get_df_hook_lineage(self, mock_get_pandas_df, mock_send_lineage):
        statement = "SELECT 1"
        self.db_hook.get_df(statement, df_type="pandas")

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == statement
        assert call_kw["sql_parameters"] is None

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
    def test_get_df_by_chunks_hook_lineage(self, mock_get_pandas_df_by_chunks, mock_send_lineage):
        sql = "SELECT 1"
        parameters = ("x",)
        self.db_hook.get_df_by_chunks(sql, parameters=parameters, chunksize=1)

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == sql
        assert call_kw["sql_parameters"] == parameters

    @mock.patch("airflow.providers.elasticsearch.hooks.elasticsearch.Elasticsearch")
    def test_execute_sql_query(self, mock_es):
        mock_es_sql_client = MagicMock()
        mock_es_sql_client.query.return_value = RESPONSE_WITHOUT_CURSOR
        mock_es.return_value.sql = mock_es_sql_client

        es_connection = ESConnection(host="localhost", port=9200)
        response = es_connection.execute_sql("SELECT * FROM hollywood.actors")
        mock_es_sql_client.query.assert_called_once_with(
            body={
                "fetch_size": 1000,
                "field_multi_value_leniency": False,
                "query": "SELECT * FROM hollywood.actors",
            }
        )

        assert response == RESPONSE_WITHOUT_CURSOR

    def test_connection_ignore_cursor_parameters(self):
        assert ESConnection(
            host="localhost",
            port=9200,
            fetch_size=1000,
            field_multi_value_leniency=True,
        )


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
