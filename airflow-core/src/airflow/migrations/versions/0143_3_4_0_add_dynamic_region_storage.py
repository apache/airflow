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

"""
Add dynamic region storage.

Revision ID: 54a27b6f9d01
Revises: e7c2a91bd540
Create Date: 2026-09-27 20:00:00.000000
"""

from __future__ import annotations

from textwrap import dedent
from uuid import UUID

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import raise_if_rows_exist, sqlite_rebuilds
from airflow.models.base import StringID
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime, compact_uuid_default

revision = "54a27b6f9d01"
down_revision = "e7c2a91bd540"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_SENTINEL = UUID(int=0)
_KEYS = (
    (
        "task_instance",
        "task_instance_current_key",
        ("dag_id", "task_id", "run_id", "map_index", "working_set"),
        "working_set IS NOT NULL",
    ),
    (
        "task_instance",
        "task_instance_try_key",
        ("dag_id", "task_id", "run_id", "map_index", "try_number"),
        None,
    ),
    ("task_state_store", "task_state_store_uq", ("dag_run_id", "task_id", "map_index", "key"), None),
)


def _replace_unique(table_name, constraint_name, columns):
    context = op.get_context()
    dialect = context.dialect.name
    if dialect in ("postgresql", "mysql"):
        quote = context.dialect.identifier_preparer.quote
        column_list = ", ".join(quote(name) for name in columns)
        if dialect == "postgresql":
            sql = f"""
                ALTER TABLE {table_name}
                DROP CONSTRAINT {constraint_name},
                ADD CONSTRAINT {constraint_name} UNIQUE ({column_list})
            """
        else:
            sql = f"""
                ALTER TABLE {table_name}
                DROP INDEX {constraint_name},
                ADD UNIQUE KEY {constraint_name} ({column_list})
            """
        op.execute(dedent(sql))
    else:
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.drop_constraint(constraint_name, type_="unique")
            batch_op.create_unique_constraint(constraint_name, columns)


def _build_collision_query(table_name, columns, where):
    """Select old-key groups holding more than one row, scanning only DagRuns that have regions."""
    table = sa.table(table_name, *(sa.column(name) for name in columns))
    region = sa.table("dynamic_region", sa.column("dag_id"), sa.column("run_id"))
    if table_name == "task_state_store":
        dag_run = sa.table("dag_run", sa.column("id"), sa.column("dag_id"), sa.column("run_id"))
        has_region = (
            sa.select(1)
            .select_from(
                dag_run.join(
                    region, sa.and_(region.c.dag_id == dag_run.c.dag_id, region.c.run_id == dag_run.c.run_id)
                )
            )
            .where(dag_run.c.id == table.c.dag_run_id)
            .exists()
        )
    else:
        has_region = (
            sa.select(1)
            .select_from(region)
            .where(region.c.dag_id == table.c.dag_id, region.c.run_id == table.c.run_id)
            .exists()
        )
    query = sa.select(1).select_from(table).where(has_region)
    if where:
        query = query.where(sa.text(where))
    return query.group_by(*(table.c[name] for name in columns)).having(sa.func.count() > 1).limit(1)


def _assert_downgrade_is_lossless():
    """
    Abort the downgrade if dropping ``region_id`` would merge rows, before any DDL runs.

    MySQL DDL cannot be rolled back, so every check has to happen first. The SQL guard also works
    in offline mode but does not support SQLite, which is checked by running the query directly.
    The query scans the whole ``task_instance`` table, so expect it to be slow on very large installations.
    """
    dialect = op.get_context().dialect
    for table_name, _, columns, where in _KEYS:
        query = _build_collision_query(table_name, columns, where)
        message = (
            f"Cannot downgrade: {table_name} has region rows colliding on the old key. "
            "Delete the affected DagRuns first."
        )
        if dialect.name == "sqlite":
            if op.get_bind().execute(query).scalar_one_or_none() is not None:
                raise RuntimeError(message)
        else:
            sql = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
            raise_if_rows_exist(sql, message, op)


def _configure_index_builds():
    if op.get_bind().dialect.name == "postgresql":
        op.execute("SET LOCAL statement_timeout = 0")
        op.execute(
            "SELECT set_config('maintenance_work_mem', '256MB', true) "
            "WHERE (SELECT setting::bigint FROM pg_settings WHERE name = 'maintenance_work_mem') < 262144"
        )


def upgrade():
    _configure_index_builds()
    with sqlite_rebuilds(op):
        op.create_table(
            "dynamic_region",
            sa.Column("id", CompactUUID(), nullable=False),
            sa.Column("dag_id", StringID(), nullable=False),
            sa.Column("run_id", StringID(), nullable=False),
            sa.Column("node_id", StringID(), nullable=False),
            sa.Column("parent_region_id", CompactUUID(), nullable=True),
            sa.Column("parent_region_index", sa.Integer(), nullable=True),
            sa.Column("forked_from_region_id", CompactUUID(), nullable=True),
            sa.Column("resumes_from_index", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", UtcDateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id", name="dynamic_region_pkey"),
            sa.ForeignKeyConstraint(
                ["dag_id", "run_id"],
                ["dag_run.dag_id", "dag_run.run_id"],
                name="dynamic_region_dag_run_fkey",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["parent_region_id"],
                ["dynamic_region.id"],
                name="dynamic_region_parent_region_id_fkey",
                ondelete="CASCADE",
            ),
            # Fork lineage is unbounded; a cascading self-FK would exceed MySQL's cascade depth limit.
            sa.UniqueConstraint("forked_from_region_id", name="dynamic_region_forked_from_region_id_uq"),
            sa.CheckConstraint(
                "(parent_region_id IS NULL AND parent_region_index IS NULL) OR "
                "(parent_region_id IS NOT NULL AND parent_region_index IS NOT NULL)",
                name="parent_coordinates_paired",
            ),
            sa.CheckConstraint("resumes_from_index >= 0", name="resumes_from_index_nonnegative"),
        )
        op.create_index(
            "idx_dynamic_region_slot",
            "dynamic_region",
            ["dag_id", "run_id", "node_id", "parent_region_id", "parent_region_index"],
        )
        op.create_index("idx_dynamic_region_parent_region_id", "dynamic_region", ["parent_region_id"])
        for table_name in ("task_instance", "task_state_store"):
            op.add_column(
                table_name,
                sa.Column(
                    "region_id", CompactUUID(), nullable=False, server_default=compact_uuid_default(_SENTINEL)
                ),
            )
        for table_name, constraint_name, columns, _ in _KEYS:
            new_columns = list(columns)
            new_columns.insert(new_columns.index("map_index"), "region_id")
            _replace_unique(table_name, constraint_name, new_columns)


def downgrade():
    _configure_index_builds()
    _assert_downgrade_is_lossless()
    with sqlite_rebuilds(op):
        for table_name, constraint_name, columns, _ in _KEYS:
            _replace_unique(table_name, constraint_name, list(columns))
        for table_name in ("task_instance", "task_state_store"):
            op.drop_column(table_name, "region_id")
        op.drop_table("dynamic_region")
