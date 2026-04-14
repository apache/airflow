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
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = [pytest.mark.db_test, pytest.mark.backend("postgres")]

_MIGRATION_FILE = Path(AIRFLOW_CORE_SOURCES_PATH) / (
    "airflow/migrations/versions/0093_3_2_0_restructure_callback_table.py"
)


def _load_migration_module():
    spec = importlib.util.spec_from_file_location(
        "migration_0093_restructure_callback_table", _MIGRATION_FILE
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load migration module from {_MIGRATION_FILE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_downgrade_restores_callback_request_id_autoincrement(session, monkeypatch):
    migration_module = _load_migration_module()
    connection = session.connection()

    schema_name = f"test_callback_migration_{uuid.uuid4().hex[:8]}"
    quoted_schema_name = f'"{schema_name}"'

    connection.execute(sa.text(f"CREATE SCHEMA {quoted_schema_name}"))
    connection.execute(sa.text(f"SET search_path TO {quoted_schema_name}"))

    try:
        metadata = sa.MetaData()
        sa.Table("trigger", metadata, sa.Column("id", sa.Integer(), primary_key=True))
        sa.Table(
            "callback",
            metadata,
            sa.Column("id", sa.Uuid(), nullable=False),
            sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
            sa.Column("priority_weight", sa.Integer(), nullable=False),
            sa.Column("type", sa.String(length=20), nullable=False),
            sa.Column("fetch_method", sa.String(length=20), nullable=False),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("state", sa.String(length=10), nullable=True),
            sa.Column("output", sa.Text(), nullable=True),
            sa.Column("trigger_id", sa.Integer(), nullable=True),
            sa.PrimaryKeyConstraint("id", name="callback_pkey"),
            sa.ForeignKeyConstraint(["trigger_id"], ["trigger.id"], name="callback_trigger_id_fkey"),
        )
        metadata.create_all(connection)

        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration_module, "op", operations)

        migration_module.downgrade()

        id_default = connection.scalar(
            sa.text(
                """
                SELECT column_default
                FROM information_schema.columns
                WHERE table_schema = :schema_name
                  AND table_name = 'callback_request'
                  AND column_name = 'id'
                """
            ),
            {"schema_name": schema_name},
        )
        assert id_default is not None
        assert "nextval" in id_default

        callback_request = sa.Table("callback_request", sa.MetaData(), autoload_with=connection)
        connection.execute(
            callback_request.insert().values(
                created_at=datetime.now(timezone.utc),
                priority_weight=1,
                callback_data={},
                callback_type="success",
            )
        )
        inserted_id = connection.scalar(sa.text("SELECT id FROM callback_request"))
        assert inserted_id is not None
    finally:
        connection.execute(sa.text("SET search_path TO public"))
        connection.execute(sa.text(f"DROP SCHEMA {quoted_schema_name} CASCADE"))
