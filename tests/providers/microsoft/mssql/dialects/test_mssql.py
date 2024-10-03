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

import pytest
from sqlalchemy.engine import Inspector

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.microsoft.mssql.dialects.mssql import MsSqlDialect
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS


class TestMsSqlDialect:
    def setup_method(self):
        inspector = MagicMock(spc=Inspector)
        inspector.get_columns.side_effect = lambda *args: [
            {"name": "id"},
            {"name": "name"},
            {"name": "firstname"},
            {"name": "age"},
        ]
        self.test_db_hook = MagicMock(placeholder="?", inspector=inspector, spec=DbApiHook)
        self.test_db_hook.run.side_effect = lambda *args: [("id",)]

    def test_placeholder(self):
        assert MsSqlDialect(self.test_db_hook).placeholder == "?"

    def test_extract_schema_from_table(self):
        assert MsSqlDialect._extract_schema_from_table("hollywood.actors") == ["actors", "hollywood"]

    def test_get_column_names(self):
        assert MsSqlDialect(self.test_db_hook).get_column_names("hollywood.actors") == [
            "id",
            "name",
            "firstname",
            "age",
        ]

    def test_get_primary_keys(self):
        assert MsSqlDialect(self.test_db_hook).get_primary_keys("hollywood.actors") == ["id"]

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="The tests should be skipped for Airflow < 3.0")
    def test_generate_replace_sql(self):
        values = [
            {"id": "id", "name": "Stallone", "firstname": "Sylvester", "age": "78"},
            {"id": "id", "name": "Statham", "firstname": "Jason", "age": "57"},
            {"id": "id", "name": "Li", "firstname": "Jet", "age": "61"},
            {"id": "id", "name": "Lundgren", "firstname": "Dolph", "age": "66"},
            {"id": "id", "name": "Norris", "firstname": "Chuck", "age": "84"},
        ]
        target_fields = ["id", "name", "firstname", "age"]
        sql = MsSqlDialect(self.test_db_hook).generate_replace_sql("hollywood.actors", values, target_fields)
        assert (
            sql
            == """
            MERGE INTO hollywood.actors WITH (ROWLOCK, UPDLOCK) AS target
            USING (SELECT ? AS id, ? AS name, ? AS firstname, ? AS age) AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET target.name = source.name, target.firstname = source.firstname, target.age = source.age
            WHEN NOT MATCHED THEN
                INSERT (id, name, firstname, age) VALUES (source.id, source.name, source.firstname, source.age);
        """.strip()
        )
