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

from sqlalchemy.engine import Inspector

from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.sql import DbApiHook


class TestDialect:
    def setup_method(self):
        inspector = MagicMock(spc=Inspector)
        inspector.get_columns.side_effect = lambda table_name, schema: [
            {"name": "index", "identity": True},
            {"name": "name"},
            {"name": "firstname"},
            {"name": "age"},
        ]
        inspector.get_pk_constraint.side_effect = lambda table_name, schema: {"constrained_columns": ["id"]}
        self.test_db_hook = MagicMock(placeholder="?", inspector=inspector, spec=DbApiHook)
        self.test_db_hook.reserved_words = {"index", "user"}
        self.test_db_hook.insert_statement_format = "INSERT INTO {} {} VALUES ({})"
        self.test_db_hook.replace_statement_format = "REPLACE INTO {} {} VALUES ({})"
        self.test_db_hook.escape_word_format = '"{}"'
        self.test_db_hook.escape_column_names = False

    def test_insert_statement_format(self):
        assert Dialect(self.test_db_hook).insert_statement_format == "INSERT INTO {} {} VALUES ({})"

    def test_replace_statement_format(self):
        assert Dialect(self.test_db_hook).replace_statement_format == "REPLACE INTO {} {} VALUES ({})"

    def test_escape_word_format(self):
        assert Dialect(self.test_db_hook).escape_word_format == '"{}"'

    def test_unescape_word(self):
        assert Dialect(self.test_db_hook).unescape_word('"table"') == "table"

    def test_unescape_word_with_different_format(self):
        self.test_db_hook.escape_word_format = "[{}]"
        dialect = Dialect(self.test_db_hook)
        assert dialect.unescape_word("table") == "table"
        assert dialect.unescape_word("t@ble") == "t@ble"
        assert dialect.unescape_word("table_name") == "table_name"
        assert dialect.unescape_word('"table"') == '"table"'
        assert dialect.unescape_word("[table]") == "table"
        assert dialect.unescape_word("schema.[t@ble]") == "schema.t@ble"
        assert dialect.unescape_word("[schema].[t@ble]") == "schema.t@ble"
        assert dialect.unescape_word("[schema].table") == "schema.table"

    def test_escape_word(self):
        assert Dialect(self.test_db_hook).escape_word('"table"') == '"table"'

    def test_escape_word_with_different_format(self):
        self.test_db_hook.escape_word_format = "[{}]"
        dialect = Dialect(self.test_db_hook)
        assert dialect.escape_word("name") == "name"
        assert dialect.escape_word("[name]") == "[name]"
        assert dialect.escape_word("n@me") == "[n@me]"
        assert dialect.escape_word("index") == "[index]"
        assert dialect.escape_word("User") == "[User]"
        assert dialect.escape_word("attributes.id") == "[attributes.id]"

    def test_escape_word_when_all_column_names_must_be_escaped(self):
        self.test_db_hook.escape_word_format = "[{}]"
        self.test_db_hook.escape_column_names = True
        dialect = Dialect(self.test_db_hook)
        assert dialect.escape_word("name") == "[name]"
        assert dialect.escape_word("[name]") == "[name]"
        assert dialect.escape_word("n@me") == "[n@me]"
        assert dialect.escape_word("index") == "[index]"
        assert dialect.escape_word("User") == "[User]"
        assert dialect.escape_word("attributes.id") == "[attributes.id]"

    def test_placeholder(self):
        assert Dialect(self.test_db_hook).placeholder == "?"

    def test_extract_schema_from_table(self):
        assert Dialect.extract_schema_from_table("schema.table") == ("table", "schema")

    def test_get_column_names(self):
        assert Dialect(self.test_db_hook).get_column_names("table", "schema") == [
            "index",
            "name",
            "firstname",
            "age",
        ]

    def test_get_target_fields(self):
        assert Dialect(self.test_db_hook).get_target_fields("table", "schema") == [
            "name",
            "firstname",
            "age",
        ]

    def test_get_primary_keys(self):
        assert Dialect(self.test_db_hook).get_primary_keys("table", "schema") == ["id"]

    def test_generate_replace_sql(self):
        values = [
            {"index": 1, "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"index": 2, "name": "Statham", "firstname": "Jason", "age": "57"},
            {"index": 3, "name": "Li", "firstname": "Jet", "age": "61"},
            {"index": 4, "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"index": 5, "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["index", "name", "firstname", "age"]
        sql = Dialect(self.test_db_hook).generate_replace_sql("hollywood.actors", values, target_fields)
        assert (
            sql
            == """
            REPLACE INTO hollywood.actors ("index", name, firstname, age) VALUES (?,?,?,?,?)
        """.strip()
        )

    def test_generate_replace_sql_when_escape_column_names_is_enabled(self):
        values = [
            {"index": 1, "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"index": 2, "name": "Statham", "firstname": "Jason", "age": "57"},
            {"index": 3, "name": "Li", "firstname": "Jet", "age": "61"},
            {"index": 4, "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"index": 5, "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["index", "name", "firstname", "age"]
        self.test_db_hook.escape_column_names = True
        sql = Dialect(self.test_db_hook).generate_replace_sql("hollywood.actors", values, target_fields)
        assert (
            sql
            == """
            REPLACE INTO hollywood.actors ("index", "name", "firstname", "age") VALUES (?,?,?,?,?)
        """.strip()
        )
