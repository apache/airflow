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
Use TI.id as primary key to TaskInstanceNote.

Revision ID: cf87489a35df
Revises: 7645189f3479
Create Date: 2025-03-04 20:29:19.935428

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "cf87489a35df"
down_revision = "7645189f3479"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Use TI.id as primary key to TaskInstanceNote."""
    dialect_name = op.get_bind().dialect.name
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("ti_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"))
        )
    if dialect_name == "postgresql":
        op.execute("""
            UPDATE task_instance_note SET ti_id = task_instance.id
            FROM task_instance
            WHERE task_instance_note.task_id = task_instance.task_id
            AND task_instance_note.dag_id = task_instance.dag_id
            AND task_instance_note.run_id = task_instance.run_id
            AND task_instance_note.map_index = task_instance.map_index
            """)
    elif dialect_name == "mysql":
        op.execute("""
            UPDATE task_instance_note tin
            JOIN task_instance ti ON
                tin.task_id = ti.task_id
                AND tin.dag_id = ti.dag_id
                AND tin.run_id = ti.run_id
                AND tin.map_index = ti.map_index
            SET tin.ti_id = ti.id
            """)
    else:
        op.execute("""
            UPDATE task_instance_note
            SET ti_id = (SELECT id FROM task_instance WHERE task_instance_note.task_id = task_instance.task_id
            AND task_instance_note.dag_id = task_instance.dag_id
            AND task_instance_note.run_id = task_instance.run_id
            AND task_instance_note.map_index = task_instance.map_index)
            """)
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.alter_column(
            "ti_id",
            existing_type=sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            nullable=False,
        )
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("task_instance_note_pkey", type_="primary")
        batch_op.drop_column("map_index")
        batch_op.drop_column("dag_id")
        batch_op.drop_column("task_id")
        batch_op.drop_column("run_id")
        batch_op.create_primary_key("task_instance_note_pkey", ["ti_id"])
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )


def downgrade():
    """Unapply Use TI.id as primary key to TaskInstanceNote."""
    dialect_name = op.get_bind().dialect.name
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_id", StringID(), nullable=True))
        batch_op.add_column(sa.Column("task_id", StringID(), nullable=True))
        batch_op.add_column(sa.Column("run_id", StringID(), nullable=True))
        batch_op.add_column(sa.Column("map_index", sa.Integer(), nullable=True, server_default=sa.text("-1")))

    if dialect_name == "postgresql":
        op.execute("""
            UPDATE task_instance_note
            SET dag_id = task_instance.dag_id,
                task_id = task_instance.task_id,
                run_id = task_instance.run_id,
                map_index = task_instance.map_index
            FROM task_instance
            WHERE task_instance_note.ti_id = task_instance.id
            """)
    elif dialect_name == "mysql":
        op.execute("""
            UPDATE task_instance_note tin
            JOIN task_instance ti ON
                tin.ti_id = ti.id
            SET tin.dag_id = ti.dag_id,
                tin.task_id = ti.task_id,
                tin.run_id = ti.run_id,
                tin.map_index = ti.map_index
            """)
    else:
        op.execute("""
            UPDATE task_instance_note
            SET dag_id = (SELECT dag_id FROM task_instance WHERE task_instance_note.ti_id = task_instance.id),
                task_id = (SELECT task_id FROM task_instance WHERE task_instance_note.ti_id = task_instance.id),
                run_id = (SELECT run_id FROM task_instance WHERE task_instance_note.ti_id = task_instance.id),
                map_index = (SELECT map_index FROM task_instance WHERE task_instance_note.ti_id = task_instance.id)
            """)
    with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("task_instance_note_pkey", type_="primary")
        batch_op.drop_column("ti_id")
        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )
        batch_op.alter_column(
            "map_index", existing_type=sa.INTEGER(), existing_server_default=sa.text("-1"), nullable=False
        )
        batch_op.alter_column("run_id", existing_type=StringID(), nullable=False)
        batch_op.alter_column("task_id", existing_type=StringID(), nullable=False)
        batch_op.alter_column("dag_id", existing_type=StringID(), nullable=False)

        batch_op.create_primary_key("task_instance_note_pkey", ["dag_id", "task_id", "run_id", "map_index"])
