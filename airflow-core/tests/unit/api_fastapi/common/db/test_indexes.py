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

import uuid

import pytest
from sqlalchemy import inspect, text

from airflow.api_fastapi.common.db import indexes
from airflow.settings import get_engine

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


def _backend() -> str:
    return get_engine().dialect.name


class TestParseIndexSpec:
    def test_valid(self):
        table, cols = indexes._parse_index_spec("task_instance(dag_id, task_id, run_id)")
        assert table == "task_instance"
        assert cols == ["dag_id", "task_id", "run_id"]

    def test_invalid(self):
        with pytest.raises(ValueError, match="Invalid index spec 'task_instance'. Expected*"):
            indexes._parse_index_spec("task_instance")
        with pytest.raises(ValueError, match=r"Invalid index spec 'task_instance\(,\)'. Table name and"):
            indexes._parse_index_spec("task_instance(,)")


class TestBuildIndexName:
    @pytest.mark.parametrize(
        ("table", "cols", "expected"),
        [
            ["task_instance", ["dag_id", "run_id", "task_id"], "idx_task_instance_dag_id_run_id_task_id"],
            [
                "some_table",
                ["a" * 40, "b" * 40, "c" * 40],
                "idx_some_table_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_e28fa017",
            ],
        ],
    )
    def test_build_index_names(self, table, cols, expected):
        name = indexes._build_index_name(table, cols)
        assert name == expected


class TestIndexExists:
    def test_true_after_creation(self):
        engine = get_engine()
        table = f"t_exists_{uuid.uuid4().hex[:8]}"
        index_name = f"idx_{table}_a_b"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table} (a INT, b INT)"))
        try:
            with engine.connect() as conn:
                if _backend() == "postgresql":
                    indexes._create_index_postgres(conn, table, ["a", "b"], index_name)
                elif _backend() == "mysql":
                    indexes._create_index_mysql(conn, table, ["a", "b"], index_name)
                else:
                    indexes._create_index_sqlite(conn, table, ["a", "b"], index_name)
            with engine.connect() as conn:
                assert indexes._index_exists(conn, table, index_name) is True
        finally:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))


class TestCreateIndexPostgres:
    @pytest.mark.backend("postgres")
    def test_concurrently(self):
        engine = get_engine()
        table = "some_table"
        index_name = f"idx_{table}_a_b"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table} (a INT, b INT)"))
        try:
            with engine.connect() as conn:
                indexes._create_index_postgres(conn, table, ["a", "b"], index_name)
            with engine.connect() as conn:
                ix = inspect(conn).get_indexes(table)
                assert any(d.get("name") == index_name for d in ix)
        finally:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))


class TestCreateIndexMySQL:
    @pytest.mark.backend("mysql")
    def test_online_or_fallback(self):
        engine = get_engine()
        table = "some_table"
        index_name = f"idx_{table}_a_b"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table} (a INT, b INT)"))
        try:
            with engine.connect() as conn:
                indexes._create_index_mysql(conn, table, ["a", "b"], index_name)
            with engine.connect() as conn:
                ix = inspect(conn).get_indexes(table)
                assert any(d.get("name") == index_name for d in ix)
        finally:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))


class TestCreateIndexSQLite:
    @pytest.mark.backend("sqlite")
    def test_if_not_exists(self):
        engine = get_engine()
        table = "some_table"
        index_name = f"idx_{table}_a_b"
        with engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE "{table}" (a INT, b INT)'))
        try:
            with engine.connect() as conn:
                indexes._create_index_sqlite(conn, table, ["a", "b"], index_name)
            with engine.connect() as conn:
                ix = inspect(conn).get_indexes(table)
                assert any(d.get("name") == index_name for d in ix)
        finally:
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}"'))


class TestInitMetadataIndexes:
    def test_creates_from_config(self):
        engine = get_engine()
        table = "some_table"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table} (a INT, b INT, dttm TIMESTAMP NULL)"))
        try:
            specs = [f"{table}(a,b)", f"{table}(dttm)"]
            with conf_vars({("database", "metadata_indexes"): "|".join(specs)}):
                res = indexes.init_metadata_indexes()
            with engine.connect() as conn:
                ix = inspect(conn).get_indexes(table)
                names = {d.get("name") for d in ix}
                assert any(name.startswith(f"idx_{table}_a_b") for name in names)
                assert any(name.startswith(f"idx_{table}_dttm") for name in names)
            assert len(res) == 2
        finally:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
