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

"""
Rename dataset as asset.

Revision ID: 05234396c6fc
Revises: 3a8972ecb8f9
Create Date: 2024-10-02 08:10:01.697128
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

from airflow.migrations.utils import mysql_drop_foreignkey_if_exists
from airflow.settings import json

# revision identifiers, used by Alembic.
revision = "05234396c6fc"
down_revision = "3a8972ecb8f9"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

if TYPE_CHECKING:
    from alembic.operations.base import BatchOperations
    from sqlalchemy.sql.elements import conv


def _rename_index(
    *, batch_op: BatchOperations, original_name: str, new_name: str, columns: list[str], unique: bool
) -> None:
    batch_op.drop_index(original_name)
    batch_op.create_index(new_name, columns, unique=unique)


def _rename_fk_constraint(
    *,
    batch_op: BatchOperations,
    original_name: str | conv,
    new_name: str | conv,
    referent_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    ondelete: str,
) -> None:
    batch_op.drop_constraint(original_name, type_="foreignkey")
    batch_op.create_foreign_key(
        constraint_name=new_name,
        referent_table=referent_table,
        local_cols=local_cols,
        remote_cols=remote_cols,
        ondelete=ondelete,
    )


def _rename_pk_constraint_unkown(
    *,
    batch_op: BatchOperations,
    table_name: str,
    original_name: str,
    alternative_name: str,
    new_name: str,
    columns: list[str],
) -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        op.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {original_name}")
        op.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {alternative_name}")
    elif dialect == "mysql":
        op.execute(f"ALTER TABLE {table_name} DROP PRIMARY KEY")
    batch_op.create_primary_key(constraint_name=new_name, columns=columns)


def _rename_pk_constraint(
    *, batch_op: BatchOperations, original_name: str, new_name: str, columns: list[str]
) -> None:
    if batch_op.get_bind().dialect.name in ("postgresql", "mysql"):
        batch_op.drop_constraint(original_name, type_="primary")
    batch_op.create_primary_key(constraint_name=new_name, columns=columns)


def _drop_fkey_if_exists(table, constraint_name):
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
        try:
            with op.batch_alter_table(table, schema=None) as batch_op:
                batch_op.drop_constraint(op.f(constraint_name), type_="foreignkey")
        except ValueError:
            pass
    elif dialect == "mysql":
        mysql_drop_foreignkey_if_exists(constraint_name, table, op)
    else:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint_name}")


# original table name to new table name
table_name_mappings = (
    ("dataset_alias_dataset", "asset_alias_asset"),
    ("dataset_alias_dataset_event", "asset_alias_asset_event"),
    ("dataset_alias", "asset_alias"),
    ("dataset", "asset"),
    ("dag_schedule_dataset_alias_reference", "dag_schedule_asset_alias_reference"),
    ("dag_schedule_dataset_reference", "dag_schedule_asset_reference"),
    ("task_outlet_dataset_reference", "task_outlet_asset_reference"),
    ("dataset_dag_run_queue", "asset_dag_run_queue"),
    ("dagrun_dataset_event", "dagrun_asset_event"),
    ("dataset_event", "asset_event"),
)


def upgrade():
    """Rename dataset as asset."""
    _drop_fkey_if_exists("dataset_alias_dataset", "dataset_alias_dataset_dataset_id_fkey")
    _drop_fkey_if_exists("dataset_alias_dataset", "dataset_alias_dataset_alias_id_fkey")
    _drop_fkey_if_exists("dataset_alias_dataset", "ds_dsa_alias_id")
    _drop_fkey_if_exists("dataset_alias_dataset", "ds_dsa_dataset_id")

    _drop_fkey_if_exists("dataset_alias_dataset_event", "dataset_alias_dataset_dataset_id_fkey")
    _drop_fkey_if_exists("dataset_alias_dataset_event", "dataset_alias_dataset_event_alias_id_fkey")
    _drop_fkey_if_exists("dataset_alias_dataset_event", "dataset_alias_dataset_event_event_id_fkey")

    _drop_fkey_if_exists("dataset_alias_dataset_event", "dss_de_alias_id")
    _drop_fkey_if_exists("dataset_alias_dataset_event", "dss_de_event_id")

    _drop_fkey_if_exists("dag_schedule_dataset_alias_reference", "dsdar_dag_id_fkey")
    _drop_fkey_if_exists("dag_schedule_dataset_alias_reference", "dsdar_dataset_alias_fkey")

    _drop_fkey_if_exists("dag_schedule_dataset_reference", "dsdr_dag_id_fkey")
    _drop_fkey_if_exists("dag_schedule_dataset_reference", "dsdr_dataset_fkey")

    _drop_fkey_if_exists("task_outlet_dataset_reference", "todr_dataset_fkey")

    _drop_fkey_if_exists("dataset_dag_run_queue", "ddrq_dag_fkey")
    _drop_fkey_if_exists("dataset_dag_run_queue", "ddrq_dataset_fkey")

    _drop_fkey_if_exists("dagrun_dataset_event", "dagrun_dataset_events_event_id_fkey")
    _drop_fkey_if_exists("dagrun_dataset_event", "dagrun_dataset_event_event_id_fkey")

    _drop_fkey_if_exists("dagrun_dataset_event", "dagrun_dataset_events_dag_run_id_fkey")
    _drop_fkey_if_exists("dagrun_dataset_event", "dagrun_dataset_event_dag_run_id_fkey")

    # Rename tables
    for original_name, new_name in table_name_mappings:
        op.rename_table(original_name, new_name)

    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        batch_op.drop_constraint("asset_active_asset_name_uri_fkey", type_="foreignkey")

    with op.batch_alter_table("asset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_name_uri_unique",
            new_name="idx_asset_name_uri_unique",
            columns=["name", "uri"],
            unique=True,
        )

    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="asset_active_asset_name_uri_fkey",
            referent_table="asset",
            local_cols=["name", "uri"],
            remote_cols=["name", "uri"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer(), nullable=False)
        _rename_pk_constraint_unkown(
            batch_op=batch_op,
            table_name="asset_alias_asset",
            original_name="dataset_alias_dataset_pkey",
            alternative_name="asset_alias_asset_pkey",
            new_name="asset_alias_asset_pkey",
            columns=["alias_id", "asset_id"],
        )

    with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_alias_id",
            new_name="idx_asset_alias_asset_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_alias_id_fkey",
            referent_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_alias_dataset_id",
            new_name="idx_asset_alias_asset_asset_id",
            columns=["asset_id"],
            unique=False,
        )

        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_asset_id_fkey",
            referent_table="asset",
            local_cols=["asset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("asset_alias_asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_event_alias_id",
            new_name="idx_asset_alias_asset_event_alias_id",
            columns=["alias_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("asset_alias_asset_event_alias_id_fkey"),
            referent_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_event_event_id",
            new_name="idx_asset_alias_asset_event_event_id",
            columns=["event_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("asset_alias_asset_event_event_id_fkey"),
            referent_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dag_schedule_asset_alias_reference", schema=None) as batch_op:
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="dsdar_pkey",
            new_name="dsaar_pkey",
            columns=["alias_id", "dag_id"],
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_alias_reference_dag_id",
            new_name="idx_dag_schedule_asset_alias_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )
    with op.batch_alter_table("dag_schedule_asset_alias_reference", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="dsaar_asset_alias_fkey",
            referent_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="dsaar_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dag_schedule_asset_reference", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer(), nullable=False)

    with op.batch_alter_table("dag_schedule_asset_reference", schema=None) as batch_op:
        _rename_pk_constraint_unkown(
            batch_op=batch_op,
            table_name="dag_schedule_asset_reference",
            original_name="dag_schedule_dataset_reference_pkey",
            alternative_name="dsdr_pkey",
            new_name="dsar_pkey",
            columns=["asset_id", "dag_id"],
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_reference_dag_id",
            new_name="idx_dag_schedule_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

        batch_op.create_foreign_key(
            constraint_name="dsar_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="dsar_asset_fkey",
            referent_table="asset",
            local_cols=["asset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer(), nullable=False)

        batch_op.drop_constraint("todr_dag_id_fkey", type_="foreignkey")
    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        _rename_pk_constraint_unkown(
            batch_op=batch_op,
            table_name="task_outlet_asset_reference",
            original_name="task_outlet_dataset_reference_pkey",
            alternative_name="todr_pkey",
            new_name="toar_pkey",
            columns=["asset_id", "dag_id", "task_id"],
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_task_outlet_dataset_reference_dag_id",
            new_name="idx_task_outlet_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )
    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.create_foreign_key("toar_asset_fkey", "asset", ["asset_id"], ["id"], ondelete="CASCADE")
        batch_op.create_foreign_key(
            constraint_name="toar_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer(), nullable=False)

    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        _rename_pk_constraint_unkown(
            batch_op=batch_op,
            table_name="asset_dag_run_queue",
            original_name="dataset_dag_run_queue_pkey",
            alternative_name="datasetdagrunqueue_pkey",
            new_name="assetdagrunqueue_pkey",
            columns=["asset_id", "target_dag_id"],
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_dag_run_queue_target_dag_id",
            new_name="idx_asset_dag_run_queue_target_dag_id",
            columns=["target_dag_id"],
            unique=False,
        )
    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        batch_op.create_foreign_key("adrq_asset_fkey", "asset", ["asset_id"], ["id"], ondelete="CASCADE")
        batch_op.create_foreign_key(
            constraint_name="adrq_dag_fkey",
            referent_table="dag",
            local_cols=["target_dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dagrun_asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_dag_run_id",
            new_name="idx_dagrun_asset_events_dag_run_id",
            columns=["dag_run_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name="dagrun_asset_event_dag_run_id_fkey",
            referent_table="dag_run",
            local_cols=["dag_run_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_event_id",
            new_name="idx_dagrun_asset_events_event_id",
            columns=["event_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name="dagrun_asset_event_event_id_fkey",
            referent_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        _rename_pk_constraint_unkown(
            batch_op=batch_op,
            table_name="dagrun_asset_event",
            original_name="dagrun_dataset_events_pkey",
            alternative_name="dagrun_dataset_event_pkey",
            new_name="dagrun_asset_event_pkey",
            columns=["event_id", "dag_run_id"],
        )

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer(), nullable=False)

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_id_timestamp",
            new_name="idx_asset_id_timestamp",
            columns=["asset_id", "timestamp"],
            unique=False,
        )

    with op.batch_alter_table("asset_alias", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_name_unique",
            new_name="idx_asset_alias_name_unique",
            columns=["name"],
            unique=True,
        )

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "dataset_expression",
            new_column_name="asset_expression",
            type_=sqlalchemy_jsonfield.JSONField(json=json),
        )


def downgrade():
    """Unapply Rename dataset as asset."""
    # Rename tables
    for original_name, new_name in table_name_mappings:
        op.rename_table(new_name, original_name)

    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        batch_op.drop_constraint("asset_active_asset_name_uri_fkey", type_="foreignkey")

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_name_uri_unique",
            new_name="idx_dataset_name_uri_unique",
            columns=["name", "uri"],
            unique=True,
        )

    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="asset_active_asset_name_uri_fkey",
            referent_table="dataset",
            local_cols=["name", "uri"],
            remote_cols=["name", "uri"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset_alias_dataset", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer(), nullable=False)

    with op.batch_alter_table("dataset_alias_dataset", schema=None) as batch_op:
        batch_op.drop_constraint(op.f("asset_alias_asset_alias_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_alias_id",
            new_name="idx_dataset_alias_dataset_alias_id",
            columns=["alias_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("dataset_alias_dataset_alias_id_fkey"),
            referent_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.drop_constraint(op.f("asset_alias_asset_asset_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_asset_id",
            new_name="idx_dataset_alias_dataset_alias_dataset_id",
            columns=["dataset_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("dataset_alias_dataset_dataset_id_fkey"),
            referent_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    _drop_fkey_if_exists("dataset_alias_dataset_event", "dataset_alias_dataset_event_event_id_fkey")
    with op.batch_alter_table("dataset_alias_dataset_event", schema=None) as batch_op:
        batch_op.drop_constraint(op.f("asset_alias_asset_event_alias_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_event_alias_id",
            new_name="idx_dataset_alias_dataset_event_alias_id",
            columns=["alias_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("dataset_alias_dataset_event_alias_id_fkey"),
            referent_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.drop_constraint(op.f("asset_alias_asset_event_event_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_event_event_id",
            new_name="idx_dataset_alias_dataset_event_event_id",
            columns=["event_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name=op.f("dataset_alias_dataset_event_event_id_fkey"),
            referent_table="dataset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dag_schedule_dataset_alias_reference", schema=None) as batch_op:
        batch_op.drop_constraint("dsaar_asset_alias_fkey", type_="foreignkey")
        batch_op.drop_constraint("dsaar_dag_id_fkey", type_="foreignkey")

        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="dsaar_pkey",
            new_name="dsdar_pkey",
            columns=["alias_id", "dag_id"],
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_asset_alias_reference_dag_id",
            new_name="idx_dag_schedule_dataset_alias_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

        batch_op.create_foreign_key(
            constraint_name="dsdar_dataset_alias_fkey",
            referent_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="dsdar_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dag_schedule_dataset_reference", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer(), nullable=False)

        batch_op.drop_constraint("dsar_dag_id_fkey", type_="foreignkey")
        batch_op.drop_constraint("dsar_asset_fkey", type_="foreignkey")

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_asset_reference_dag_id",
            new_name="idx_dag_schedule_dataset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="dsar_pkey",
            new_name="dsdr_pkey",
            columns=["dataset_id", "dag_id"],
        )

        batch_op.create_foreign_key(
            constraint_name="dsdr_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="dsdr_dataset_fkey",
            referent_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("task_outlet_dataset_reference", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer(), nullable=False)
        batch_op.drop_constraint("toar_asset_fkey", type_="foreignkey")
        batch_op.drop_constraint("toar_dag_id_fkey", type_="foreignkey")

        _rename_index(
            batch_op=batch_op,
            original_name="idx_task_outlet_asset_reference_dag_id",
            new_name="idx_task_outlet_dataset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="toar_pkey",
            new_name="todr_pkey",
            columns=["dataset_id", "dag_id", "task_id"],
        )

        batch_op.create_foreign_key(
            constraint_name="todr_dataset_fkey",
            referent_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="todr_dag_id_fkey",
            referent_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset_dag_run_queue", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer(), nullable=False)

        batch_op.drop_constraint("adrq_asset_fkey", type_="foreignkey")
        batch_op.drop_constraint("adrq_dag_fkey", type_="foreignkey")

        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="assetdagrunqueue_pkey",
            new_name="datasetdagrunqueue_pkey",
            columns=["dataset_id", "target_dag_id"],
        )
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_dag_run_queue_target_dag_id",
            new_name="idx_dataset_dag_run_queue_target_dag_id",
            columns=["target_dag_id"],
            unique=False,
        )

        batch_op.create_foreign_key(
            constraint_name="ddrq_dataset_fkey",
            referent_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            constraint_name="ddrq_dag_fkey",
            referent_table="dag",
            local_cols=["target_dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dagrun_dataset_event", schema=None) as batch_op:
        batch_op.drop_constraint(op.f("dagrun_asset_event_event_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_asset_events_event_id",
            new_name="idx_dagrun_dataset_events_event_id",
            columns=["event_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name="dagrun_dataset_event_event_id_fkey",
            referent_table="dataset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.drop_constraint(op.f("dagrun_asset_event_dag_run_id_fkey"), type_="foreignkey")
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_asset_events_dag_run_id",
            new_name="idx_dagrun_dataset_events_dag_run_id",
            columns=["dag_run_id"],
            unique=False,
        )
        batch_op.create_foreign_key(
            constraint_name="dagrun_dataset_event_dag_run_id_fkey",
            referent_table="dag_run",
            local_cols=["dag_run_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="dagrun_asset_event_pkey",
            new_name="dagrun_dataset_event_pkey",
            columns=["event_id", "dag_run_id"],
        )

    with op.batch_alter_table("dataset_event", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer(), nullable=False)

    with op.batch_alter_table("dataset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_id_timestamp",
            new_name="idx_dataset_id_timestamp",
            columns=["dataset_id", "timestamp"],
            unique=False,
        )

    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_name_unique",
            new_name="idx_dataset_alias_name_unique",
            columns=["name"],
            unique=True,
        )

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "asset_expression",
            new_column_name="dataset_expression",
            type_=sqlalchemy_jsonfield.JSONField(json=json),
        )
