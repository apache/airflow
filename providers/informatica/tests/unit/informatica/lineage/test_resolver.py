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

import pytest

from airflow.providers.informatica.lineage.resolver import (
    SQLLineageResolver,
    _dialect_from_conn_id_str,
    _infer_dialect,
    get_resolver,
)


class _FakeTask:
    """Minimal task stand-in with configurable attributes."""

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)


class TestGetResolver:
    def test_returns_sql_resolver_when_sql_present(self):
        task = _FakeTask(sql="SELECT 1")
        assert get_resolver(task) is not None

    def test_returns_none_when_no_sql(self):
        task = _FakeTask()
        assert get_resolver(task) is None

    def test_returns_none_when_sql_is_empty_string(self):
        task = _FakeTask(sql="")
        assert get_resolver(task) is None


class TestDialectInference:
    @pytest.mark.parametrize(
        ("conn_id", "expected"),
        [
            ("postgres_default", "postgres"),
            ("my_snowflake_conn", "snowflake"),
            ("mysql_prod", "mysql"),
            ("redshift_warehouse", "redshift"),
            ("databricks_cluster", "databricks"),
            ("hive_metastore", "hive"),
            ("unknown_connection", None),
        ],
    )
    def test_dialect_from_conn_id_str(self, conn_id, expected):
        assert _dialect_from_conn_id_str(conn_id) == expected

    def test_infer_uses_conn_id_field_first(self):
        task = _FakeTask(conn_id_field="snowflake_conn_id", snowflake_conn_id="my_snowflake")
        assert _infer_dialect(task) == "snowflake"

    def test_infer_falls_back_to_conn_id(self):
        task = _FakeTask(conn_id="postgres_default")
        assert _infer_dialect(task) == "postgres"

    def test_infer_falls_back_to_source_conn_id(self):
        task = _FakeTask(source_conn_id="mysql_source")
        assert _infer_dialect(task) == "mysql"

    def test_infer_returns_none_when_no_conn_id(self):
        task = _FakeTask(sql="SELECT 1")
        assert _infer_dialect(task) is None


class TestSQLLineageResolver:
    def test_simple_select_produces_inlets_only(self):
        task = _FakeTask(sql="SELECT * FROM orders", conn_id="postgres_default")
        resolver = SQLLineageResolver()
        result = resolver.resolve(task)
        assert result is not None
        inlets, outlets = result
        assert len(inlets) == 1
        assert inlets[0].table == "orders"
        assert inlets[0].schema is None
        assert outlets == []

    def test_insert_into_select_produces_inlets_and_outlets(self):
        task = _FakeTask(
            sql="INSERT INTO summary SELECT region, amount FROM sales",
            conn_id="postgres_default",
        )
        resolver = SQLLineageResolver()
        result = resolver.resolve(task)
        assert result is not None
        inlets, outlets = result
        assert any(t.table == "sales" for t in inlets)
        assert any(t.table == "summary" for t in outlets)

    def test_destination_table_attr_supplements_missing_targets(self):
        task = _FakeTask(
            sql="SELECT * FROM source_table",
            conn_id="mysql_default",
            destination_table="dest_table",
        )
        resolver = SQLLineageResolver()
        result = resolver.resolve(task)
        assert result is not None
        _, outlets = result
        assert any(t.table == "dest_table" for t in outlets)

    def test_no_sql_returns_none(self):
        task = _FakeTask()
        resolver = SQLLineageResolver()
        assert resolver.resolve(task) is None

    def test_jinja_sql_returns_none(self):
        task = _FakeTask(sql="SELECT * FROM {{ params.table }}", conn_id="postgres_default")
        resolver = SQLLineageResolver()
        assert resolver.resolve(task) is None

    def test_schema_qualified_table_in_uri(self):
        task = _FakeTask(sql="SELECT * FROM public.users", conn_id="postgres_default")
        resolver = SQLLineageResolver()
        result = resolver.resolve(task)
        assert result is not None
        inlets, _ = result
        ref = next(t for t in inlets if t.table == "users")
        assert ref.schema == "public"

    def test_database_applied_as_default(self):
        task = _FakeTask(
            sql="SELECT * FROM orders",
            conn_id="postgres_default",
            database="mydb",
        )
        resolver = SQLLineageResolver()
        result = resolver.resolve(task)
        assert result is not None
        inlets, _ = result
        ref = next(t for t in inlets if t.table == "orders")
        assert ref.database == "mydb"

    def test_no_tables_in_sql_returns_none(self):
        task = _FakeTask(sql="SELECT 1 + 1", conn_id="postgres_default")
        resolver = SQLLineageResolver()
        assert resolver.resolve(task) is None
