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
Rename dag_schedule_dataset_alias_reference constraint names.

Revision ID: 5f2621c13b39
Revises: 22ed7efa9da2
Create Date: 2024-10-25 04:03:33.002701

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "5f2621c13b39"
down_revision = "22ed7efa9da2"
branch_labels = None
depends_on = None
airflow_version = "2.10.3"

if TYPE_CHECKING:
    from alembic.operations.base import BatchOperations
    from sqlalchemy.sql.elements import conv


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


def upgrade():
    """Rename dag_schedule_dataset_alias_reference constraint."""
    with op.batch_alter_table(
        "dag_schedule_dataset_alias_reference", schema=None
    ) as batch_op:
        bind = op.get_context().bind
        insp = inspect(bind)
        fk_constraints = [
            fk["name"]
            for fk in insp.get_foreign_keys("dag_schedule_dataset_alias_reference")
        ]

        # "dsdar_dataset_alias_fkey" was the constraint name defined in the model while "dsdar_dataset_fkey" is the one
        # defined in the previous migration.
        # Rename this constraint name if user is using the name "dsdar_dataset_fkey".
        if "dsdar_dataset_fkey" in fk_constraints:
            _rename_fk_constraint(
                batch_op=batch_op,
                original_name="dsdar_dataset_fkey",
                new_name="dsdar_dataset_alias_fkey",
                referent_table="dataset_alias",
                local_cols=["alias_id"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )

        # "dsdar_dag_fkey" was the constraint name defined in the model while "dsdar_dag_id_fkey" is the one
        # defined in the previous migration.
        # Rename this constraint name if user is using the name "dsdar_dag_fkey".
        if "dsdar_dag_fkey" in fk_constraints:
            _rename_fk_constraint(
                batch_op=batch_op,
                original_name="dsdar_dag_fkey",
                new_name="dsdar_dag_id_fkey",
                referent_table="dataset_alias",
                local_cols=["alias_id"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )


def downgrade():
    """Undo dag_schedule_dataset_alias_reference constraint rename."""
    with op.batch_alter_table(
        "dag_schedule_dataset_alias_reference", schema=None
    ) as batch_op:
        bind = op.get_context().bind
        insp = inspect(bind)
        fk_constraints = [
            fk["name"]
            for fk in insp.get_foreign_keys("dag_schedule_dataset_alias_reference")
        ]
        if "dsdar_dataset_alias_fkey" in fk_constraints:
            _rename_fk_constraint(
                batch_op=batch_op,
                original_name="dsdar_dataset_alias_fkey",
                new_name="dsdar_dataset_fkey",
                referent_table="dataset_alias",
                local_cols=["alias_id"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )

        if "dsdar_dag_id_fkey" in fk_constraints:
            _rename_fk_constraint(
                batch_op=batch_op,
                original_name="dsdar_dag_id_fkey",
                new_name="dsdar_dag_fkey",
                referent_table="dataset_alias",
                local_cols=["alias_id"],
                remote_cols=["id"],
                ondelete="CASCADE",
            )
