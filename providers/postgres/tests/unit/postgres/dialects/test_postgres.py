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

from unittest.mock import MagicMock

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.dialects.postgres import PostgresDialect


class TestPostgresDialect:
    def setup_method(self):
        def get_records(sql, parameters):
            assert isinstance(sql, str)
            assert "hollywood" in parameters, "Missing 'schema' in parameters"
            assert "actors" in parameters, "Missing 'table' in parameters"
            if "kcu." in sql:
                return [("id",)]
            return [
                ("id", None, "NO", None, "ALWAYS", "YES"),
                ("name", None, "YES", None, "NEVER", "NO"),
                ("firstname", None, "YES", None, "NEVER", "NO"),
                ("age", None, "YES", None, "NEVER", "NO"),
            ]

        self.test_db_hook = MagicMock(placeholder="?", spec=DbApiHook)
        self.test_db_hook.get_records.side_effect = get_records
        self.test_db_hook.insert_statement_format = "INSERT INTO {} {} VALUES ({})"
        self.test_db_hook.escape_word_format = '"{}"'
        self.test_db_hook.escape_column_names = False

    def test_placeholder(self):
        assert PostgresDialect(self.test_db_hook).placeholder == "?"

    def test_get_column_names(self):
        assert PostgresDialect(self.test_db_hook).get_column_names("hollywood.actors") == [
            "id",
            "name",
            "firstname",
            "age",
        ]

    def test_get_target_fields(self):
        assert PostgresDialect(self.test_db_hook).get_target_fields("hollywood.actors") == [
            "name",
            "firstname",
            "age",
        ]

    def test_get_primary_keys(self):
        assert PostgresDialect(self.test_db_hook).get_primary_keys("hollywood.actors") == ["id"]

    def test_generate_replace_sql(self):
        values = [
            {"id": 1, "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"id": 2, "name": "Statham", "firstname": "Jason", "age": "57"},
            {"id": 3, "name": "Li", "firstname": "Jet", "age": "61"},
            {"id": 4, "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"id": 5, "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["id", "name", "firstname", "age"]
        sql = PostgresDialect(self.test_db_hook).generate_replace_sql(
            "hollywood.actors", values, target_fields
        )
        assert (
            sql
            == """
            INSERT INTO hollywood.actors (id, name, firstname, age) VALUES (?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET name = excluded.name, firstname = excluded.firstname, age = excluded.age
        """.strip()
        )

    def test_generate_replace_sql_when_escape_column_names_is_enabled(self):
        values = [
            {"id": 1, "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"id": 2, "name": "Statham", "firstname": "Jason", "age": "57"},
            {"id": 3, "name": "Li", "firstname": "Jet", "age": "61"},
            {"id": 4, "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"id": 5, "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["id", "name", "firstname", "age"]
        self.test_db_hook.escape_column_names = True
        sql = PostgresDialect(self.test_db_hook).generate_replace_sql(
            "hollywood.actors", values, target_fields
        )
        assert (
            sql
            == """
            INSERT INTO hollywood.actors ("id", "name", "firstname", "age") VALUES (?,?,?,?,?) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name", "firstname" = excluded."firstname", "age" = excluded."age"
        """.strip()
        )
