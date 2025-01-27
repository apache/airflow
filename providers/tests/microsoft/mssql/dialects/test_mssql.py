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

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.microsoft.mssql.dialects.mssql import MsSqlDialect


class TestMsSqlDialect:
    def setup_method(self):
        inspector = MagicMock(spc=Inspector)
        inspector.get_columns.side_effect = lambda table_name, schema: [
            {"name": "index", "identity": True},
            {"name": "name"},
            {"name": "firstname"},
            {"name": "age"},
        ]
        self.test_db_hook = MagicMock(placeholder="?", inspector=inspector, spec=DbApiHook)
        self.test_db_hook.run.side_effect = lambda *args: [("index",)]
        self.test_db_hook.reserved_words = {"index", "user"}
        self.test_db_hook.escape_word_format = "[{}]"
        self.test_db_hook.escape_column_names = False

    def test_placeholder(self):
        assert MsSqlDialect(self.test_db_hook).placeholder == "?"

    def test_get_column_names(self):
        assert MsSqlDialect(self.test_db_hook).get_column_names("hollywood.actors") == [
            "index",
            "name",
            "firstname",
            "age",
        ]

    def test_get_target_fields(self):
        assert MsSqlDialect(self.test_db_hook).get_target_fields("hollywood.actors") == [
            "name",
            "firstname",
            "age",
        ]

    def test_get_primary_keys(self):
        assert MsSqlDialect(self.test_db_hook).get_primary_keys("hollywood.actors") == ["index"]

    def test_generate_replace_sql(self):
        values = [
            {"index": 1, "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"index": 2, "name": "Statham", "firstname": "Jason", "age": "57"},
            {"index": 3, "name": "Li", "firstname": "Jet", "age": "61"},
            {"index": 4, "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"index": 5, "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["index", "name", "firstname", "age"]
        sql = MsSqlDialect(self.test_db_hook).generate_replace_sql("hollywood.actors", values, target_fields)
        assert (
            sql
            == """
            MERGE INTO hollywood.actors WITH (ROWLOCK) AS target
            USING (SELECT ? AS [index], ? AS name, ? AS firstname, ? AS age) AS source
            ON target.[index] = source.[index]
            WHEN MATCHED THEN
                UPDATE SET target.name = source.name, target.firstname = source.firstname, target.age = source.age
            WHEN NOT MATCHED THEN
                INSERT ([index], name, firstname, age) VALUES (source.[index], source.name, source.firstname, source.age);
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
        sql = MsSqlDialect(self.test_db_hook).generate_replace_sql("hollywood.actors", values, target_fields)
        assert (
            sql
            == """
            MERGE INTO hollywood.actors WITH (ROWLOCK) AS target
            USING (SELECT ? AS [index], ? AS [name], ? AS [firstname], ? AS [age]) AS source
            ON target.[index] = source.[index]
            WHEN MATCHED THEN
                UPDATE SET target.[name] = source.[name], target.[firstname] = source.[firstname], target.[age] = source.[age]
            WHEN NOT MATCHED THEN
                INSERT ([index], [name], [firstname], [age]) VALUES (source.[index], source.[name], source.[firstname], source.[age]);
        """.strip()
        )
