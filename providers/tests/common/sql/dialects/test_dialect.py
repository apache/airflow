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
        inspector.get_columns.side_effect = lambda *args: [
            {"name": "id"},
            {"name": "name"},
            {"name": "firstname"},
            {"name": "age"},
        ]
        inspector.get_pk_constraint.side_effect = lambda table_name, schema: {"constrained_columns": ["id"]}
        self.test_db_hook = MagicMock(placeholder="?", inspector=inspector, spec=DbApiHook)

    def test_placeholder(self):
        assert Dialect("default", self.test_db_hook).placeholder == "?"

    def test_extract_schema_from_table(self):
        assert Dialect._extract_schema_from_table("schema.table") == ("table", "schema")

    def test_get_column_names(self):
        assert Dialect("default", self.test_db_hook).get_column_names("schema.table") == [
            "id",
            "name",
            "firstname",
            "age",
        ]

    def test_get_primary_keys(self):
        assert Dialect("default", self.test_db_hook).get_primary_keys("schema.table") == ["id"]
