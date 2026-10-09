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

import importlib.util
import json
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0089_3_2_0_change_serialized_dag_data_column_to_jsonb.py"
)
_spec = importlib.util.spec_from_file_location("migration_0089", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_TABLE = "_test_serialized_dag_jsonb_conversion"


@pytest.mark.db_test
@pytest.mark.backend("postgres")
def test_nul_is_stripped_and_literal_escape_survives_jsonb_conversion():
    nul_delimiter = "@@" + chr(0) + "@@"
    literal_escape_delimiter = "@@" + chr(92) + "u0000@@"
    drop = f"DROP TABLE IF EXISTS {_TABLE}"

    with settings.engine.begin() as conn:
        conn.execute(sa.text(drop))
        conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id int PRIMARY KEY, data JSON)"))
        conn.execute(
            sa.text(f"INSERT INTO {_TABLE} VALUES (1, CAST(:data AS JSON))"),
            {"data": json.dumps({"delimiter": nul_delimiter})},
        )
        conn.execute(
            sa.text(f"INSERT INTO {_TABLE} VALUES (2, CAST(:data AS JSON))"),
            {"data": json.dumps({"delimiter": literal_escape_delimiter})},
        )

    try:
        with settings.engine.connect() as conn:
            with pytest.raises(sa.exc.DataError):
                conn.execute(sa.text(f"SELECT data::JSONB FROM {_TABLE}")).all()
            conn.rollback()

        with settings.engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"""
                    ALTER TABLE {_TABLE}
                    ALTER COLUMN data TYPE JSONB
                    USING {_migration._SANITIZED_DATA_TO_JSONB}
                    """
                )
            )
            rows = dict(conn.execute(sa.text(f"SELECT id, data::text FROM {_TABLE}")).all())

        assert json.loads(rows[1]) == {"delimiter": "@@@@"}
        assert json.loads(rows[2]) == {"delimiter": literal_escape_delimiter}
    finally:
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
