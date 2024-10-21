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
    reference_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    ondelete: str,
) -> None:
    batch_op.drop_constraint(original_name)
    batch_op.create_foreign_key(
        constraint_name=new_name,
        referent_table=reference_table,
        local_cols=local_cols,
        remote_cols=remote_cols,
        ondelete=ondelete,
    )


def _rename_pk_constraint(
    *, batch_op: BatchOperations, original_name: str, new_name: str, columns: list[str]
) -> None:
    batch_op.drop_constraint(original_name)
    batch_op.create_primary_key(constraint_name=new_name, columns=columns)


def upgrade():
    """Rename dataset as asset."""
    op.rename_table("dataset_alias_dataset", "asset_alias_asset")
    op.rename_table("dataset_alias_dataset_event", "asset_alias_asset_event")
    op.rename_table("dataset_alias", "asset_alias")
    op.rename_table("dataset", "asset")
    op.rename_table("dag_schedule_dataset_alias_reference", "dag_schedule_asset_alias_reference")
    op.rename_table("dag_schedule_dataset_reference", "dag_schedule_asset_reference")
    op.rename_table("task_outlet_dataset_reference", "task_outlet_asset_reference")
    op.rename_table("dataset_dag_run_queue", "asset_dag_run_queue")
    op.rename_table("dagrun_dataset_event", "dagrun_asset_event")
    op.rename_table("dataset_event", "asset_event")

    with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_alias_id_fk_key",
            referent_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer())
        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_asset_id_fk_key",
            referent_table="asset",
            local_cols=["asset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_alias_id",
            new_name="idx_asset_alias_asset_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_alias_dataset_id",
            new_name="idx_asset_alias_asset_asset_id",
            columns=["asset_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dataset_alias_dataset_alias_id_fkey"),
            new_name=op.f("asset_alias_asset_alias_id_fkey"),
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dataset_alias_dataset_dataset_id_fkey"),
            new_name=op.f("asset_alias_asset_asset_id_fk_key"),
            reference_table="asset",
            local_cols=["asset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("asset_alias_asset_event", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_event_asset_id_fk_key",
            referent_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.create_foreign_key(
            constraint_name="asset_alias_asset_event_event_id_fk_key",
            referent_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_event_alias_id",
            new_name="idx_asset_alias_asset_event_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_event_event_id",
            new_name="idx_asset_alias_asset_event_event_id",
            columns=["event_id"],
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

    with op.batch_alter_table("asset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_name_uri_unique",
            new_name="idx_asset_name_uri_unique",
            columns=["name", "uri"],
            unique=True,
        )

    with op.batch_alter_table("dag_schedule_asset_alias_reference", schema=None) as batch_op:
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="dsdar_pkey",
            new_name="asaar_pkey",
            columns=["alias_id", "dag_id"],
        )
        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdar_dataset_fkey",
            new_name="dsaar_asset_alias_fkey",
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdar_dag_id_fkey",
            new_name="dsaar_dag_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_alias_reference_dag_id",
            new_name="idx_dag_schedule_asset_alias_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("dag_schedule_asset_reference", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer())

        # _rename_pk_constraint(
        #     batch_op=batch_op,
        #     original_name="dsdr_pkey",
        #     new_name="dsar_pkey",
        #     columns=["asset_id", "dag_id"],
        # )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdr_dag_id_fkey",
            new_name="dsar_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        # _rename_fk_constraint(
        #     batch_op=batch_op,
        #     original_name="dsdr_dataset_fkey",
        #     new_name="dsar_asset_fkey",
        #     reference_table="asset",
        #     local_cols=["asset_id"],
        #     remote_cols=["id"],
        #     ondelete="CASCADE",
        # )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_reference_dag_id",
            new_name="idx_dag_schedule_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer())

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="todr_dataset_fkey",
            new_name="toar_asset_fkey",
            reference_table="asset",
            local_cols=["asset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="todr_pkey",
            new_name="toar_pkey",
            columns=["asset_id", "dag_id", "task_id"],
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="todr_dag_id_fkey",
            new_name="toar_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_task_outlet_dataset_reference_dag_id",
            new_name="idx_task_outlet_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer())

        # _rename_pk_constraint(
        #     batch_op=batch_op,
        #     original_name="datasetdagrunqueue_pkey",
        #     new_name="assetdagrunqueue_pkey",
        #     columns=["asset_id", "target_dag_id"],
        # )

        # _rename_fk_constraint(
        #     batch_op=batch_op,
        #     original_name="ddrq_dataset_fkey",
        #     new_name="adrq_asset_fkey",
        #     reference_table="asset",
        #     local_cols=["asset_id"],
        #     remote_cols=["id"],
        #     ondelete="CASCADE",
        # )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="ddrq_dag_fkey",
            new_name="adrq_dag_fkey",
            reference_table="dag",
            local_cols=["target_dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_dag_run_queue_target_dag_id",
            new_name="idx_asset_dag_run_queue_target_dag_id",
            columns=["target_dag_id"],
            unique=False,
        )

    with op.batch_alter_table("dagrun_asset_event", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="dagrun_asset_event_event_id_fk_key",
            referent_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_dag_run_id",
            new_name="idx_dagrun_asset_events_dag_run_id",
            columns=["dag_run_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_event_id",
            new_name="idx_dagrun_asset_events_event_id",
            columns=["event_id"],
            unique=False,
        )

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        batch_op.alter_column("dataset_id", new_column_name="asset_id", type_=sa.Integer())

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_id_timestamp",
            new_name="idx_asset_id_timestamp",
            columns=["asset_id", "timestamp"],
            unique=False,
        )

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "dataset_expression",
            new_column_name="asset_expression",
            type_=sqlalchemy_jsonfield.JSONField(json=json),
        )

    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="asset_active_asset_name_uri_fkey",
            new_name="asset_active_asset_name_uri_fkey",
            reference_table="asset",
            local_cols=["name", "uri"],
            remote_cols=["name", "uri"],
            ondelete="CASCADE",
        )


def downgrade():
    """Unapply Rename dataset as asset."""
    op.rename_table("asset_alias_asset", "dataset_alias_dataset")
    op.rename_table("asset_alias_asset_event", "dataset_alias_dataset_event")
    op.rename_table("asset_alias", "dataset_alias")
    op.rename_table("asset", "dataset")
    op.rename_table("dag_schedule_asset_alias_reference", "dag_schedule_dataset_alias_reference")
    op.rename_table("dag_schedule_asset_reference", "dag_schedule_dataset_reference")
    op.rename_table("task_outlet_asset_reference", "task_outlet_dataset_reference")
    op.rename_table("asset_dag_run_queue", "dataset_dag_run_queue")
    op.rename_table("dagrun_asset_event", "dagrun_dataset_event")
    op.rename_table("asset_event", "dataset_event")

    with op.batch_alter_table("dataset_alias_dataset", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="dataset_alias_dataset_alias_id_fkey",
            referent_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer())
        batch_op.create_foreign_key(
            constraint_name=op.f("dataset_alias_dataset_dataset_id_fkey"),
            referent_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset_alias_dataset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_alias_id",
            new_name="idx_dataset_alias_dataset_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_asset_id",
            new_name="idx_dataset_alias_dataset_alias_dataset_id",
            columns=["dataset_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="a_aa_alias_id",
            new_name="ds_dsa_alias_id",
            reference_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="a_aa_asset_id",
            new_name="ds_dsa_dataset_id",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset_alias_dataset_event", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="dataset_alias_dataset_event_alias_id_fk_key",
            referent_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        batch_op.create_foreign_key(
            constraint_name="dataset_alias_dataset_event_event_id_fk_key",
            referent_table="dataset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_event_alias_id",
            new_name="idx_dataset_alias_dataset_event_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_asset_event_event_id",
            new_name="idx_dataset_alias_dataset_event_event_id",
            columns=["event_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="aa_ae_alias_id",
            new_name="dss_de_alias_id",
            reference_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="aa_ae_event_id",
            new_name="dss_de_event_id",
            reference_table="dataset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset_alias", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_alias_name_unique",
            new_name="idx_dataset_alias_name_unique",
            columns=["name"],
            unique=True,
        )
    with op.batch_alter_table("asset_active", schema=None) as batch_op:
        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="asset_active_asset_name_uri_fkey",
            new_name="asset_active_asset_name_uri_fkey",
            reference_table="dataset",
            local_cols=["name", "uri"],
            remote_cols=["name", "uri"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_name_uri_unique",
            new_name="idx_dataset_name_uri_unique",
            columns=["name", "uri"],
            unique=True,
        )

    with op.batch_alter_table("dag_schedule_dataset_alias_reference", schema=None) as batch_op:
        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="asaar_pkey",
            new_name="dsdar_pkey",
            columns=["alias_id", "dag_id"],
        )
        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsaar_asset_alias_fkey",
            new_name="dsdar_dataset_fkey",
            reference_table="dataset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsaar_dag_fkey",
            new_name="dsdar_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_asset_alias_reference_dag_id",
            new_name="idx_dag_schedule_dataset_alias_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("dag_schedule_dataset_reference", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer())

        # _rename_pk_constraint(
        #     batch_op=batch_op,
        #     original_name="dsar_pkey",
        #     new_name="dsdr_pkey",
        #     columns=["dataset_id", "dag_id"],
        # )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsar_dag_id_fkey",
            new_name="dsdr_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        # _rename_fk_constraint(
        #     batch_op=batch_op,
        #     original_name="dsar_asset_fkey",
        #     new_name="dsdr_dataset_fkey",
        #     reference_table="dataset",
        #     local_cols=["dataset_id"],
        #     remote_cols=["id"],
        #     ondelete="CASCADE",
        # )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_asset_reference_dag_id",
            new_name="idx_dag_schedule_dataset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("task_outlet_dataset_reference", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer())

    with op.batch_alter_table("task_outlet_dataset_reference", schema=None) as batch_op:
        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="toar_asset_fkey",
            new_name="todr_dataset_fkey",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_pk_constraint(
            batch_op=batch_op,
            original_name="toar_pkey",
            new_name="todr_pkey",
            columns=["dataset_id", "dag_id", "task_id"],
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="toar_dag_id_fkey",
            new_name="todr_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_task_outlet_asset_reference_dag_id",
            new_name="idx_task_outlet_dataset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

    with op.batch_alter_table("dataset_dag_run_queue", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer())

        # _rename_pk_constraint(
        #     batch_op=batch_op,
        #     original_name="assetdagrunqueue_pkey",
        #     new_name="datasetdagrunqueue_pkey",
        #     columns=["dataset_id", "target_dag_id"],
        # )

        # _rename_fk_constraint(
        #     batch_op=batch_op,
        #     original_name="adrq_asset_fkey",
        #     new_name="ddrq_dataset_fkey",
        #     reference_table="dataset",
        #     local_cols=["dataset_id"],
        #     remote_cols=["id"],
        #     ondelete="CASCADE",
        # )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="adrq_dag_fkey",
            new_name="ddrq_dag_fkey",
            reference_table="dag",
            local_cols=["target_dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_dag_run_queue_target_dag_id",
            new_name="idx_dataset_dag_run_queue_target_dag_id",
            columns=["target_dag_id"],
            unique=False,
        )

    with op.batch_alter_table("dagrun_dataset_event", schema=None) as batch_op:
        batch_op.create_foreign_key(
            constraint_name="dagrun_asset_event_event_id_fk_key",
            referent_table="dataset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_asset_events_dag_run_id",
            new_name="idx_dagrun_dataset_events_dag_run_id",
            columns=["dag_run_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_asset_events_event_id",
            new_name="idx_dagrun_dataset_events_event_id",
            columns=["event_id"],
            unique=False,
        )

    with op.batch_alter_table("dataset_event", schema=None) as batch_op:
        batch_op.alter_column("asset_id", new_column_name="dataset_id", type_=sa.Integer())

    with op.batch_alter_table("dataset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_asset_id_timestamp",
            new_name="idx_dataset_id_timestamp",
            columns=["dataset_id", "timestamp"],
            unique=False,
        )

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "asset_expression",
            new_column_name="dataset_expression",
            type_=sqlalchemy_jsonfield.JSONField(json=json),
        )
