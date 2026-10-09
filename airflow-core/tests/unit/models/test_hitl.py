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

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

from airflow.models.hitl import HITLDetail


class TestJSONExtract:
    """The HITL responded-by filters compare against plain strings, so every dialect must yield text."""

    @pytest.mark.parametrize("key", ["id", "name"])
    @pytest.mark.parametrize(
        ("dialect", "expected_sql"),
        [
            pytest.param(
                mysql.dialect(),
                "json_unquote(json_extract(hitl_detail.responded_by, '$.{key}'))",
                id="mysql",
            ),
            pytest.param(
                sqlite.dialect(),
                "json_extract(hitl_detail.responded_by, '$.{key}')",
                id="sqlite",
            ),
            pytest.param(
                postgresql.dialect(),
                "json_extract_path_text(hitl_detail.responded_by, '{key}')",
                id="postgresql",
            ),
        ],
    )
    def test_compiles_to_text_returning_sql_per_dialect(self, dialect, expected_sql: str, key: str) -> None:
        expression = getattr(HITLDetail, f"responded_by_user_{key}")

        compiled = expression.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

        assert str(compiled) == expected_sql.format(key=key)
