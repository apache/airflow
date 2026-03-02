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
Add try_id to TI and TIH.

Revision ID: 7645189f3479
Revises: e00344393f31
Create Date: 2025-02-24 18:18:12.063106

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from airflow.models.taskinstance import uuid7

# revision identifiers, used by Alembic.
revision = "7645189f3479"
down_revision = "e00344393f31"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add try_id to TaskInstanceHistory."""
    dialect_name = op.get_bind().dialect.name

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "task_instance_id",
                sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            )
        )
        batch_op.add_column(
            sa.Column(
                "try_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"), nullable=True
            )
        )
    # Update try_id column
    stmt = sa.text("SELECT id FROM task_instance_history WHERE try_id IS NULL")
    conn = op.get_bind()
    null_rows = conn.execute(stmt)
    if null_rows:
        null_rows = null_rows.fetchall()
    else:
        null_rows = []

    stmt = sa.text("""
        UPDATE task_instance_history
        SET try_id = :uuid
        WHERE id = :row_id AND try_id IS NULL
    """)

    # Update each row with a unique UUID
    for row in null_rows:
        uuid_value = uuid7()
        conn.execute(stmt.bindparams(uuid=uuid_value, row_id=row.id))
    # Update task_instance_id
    if dialect_name == "postgresql":
        op.execute("""
            UPDATE task_instance_history SET task_instance_id = task_instance.id
            FROM task_instance
            WHERE task_instance_history.task_id = task_instance.task_id
            AND task_instance_history.dag_id = task_instance.dag_id
            AND task_instance_history.run_id = task_instance.run_id
            AND task_instance_history.map_index = task_instance.map_index
            """)
    elif dialect_name == "mysql":
        op.execute("""
            UPDATE task_instance_history tih
            JOIN task_instance ti ON
                tih.task_id = ti.task_id
                AND tih.dag_id = ti.dag_id
                AND tih.run_id = ti.run_id
                AND tih.map_index = ti.map_index
            SET tih.task_instance_id = ti.id
            """)
    else:
        op.execute("""
            UPDATE task_instance_history
            SET task_instance_id = (SELECT id FROM task_instance WHERE task_instance_history.task_id = task_instance.task_id
            AND task_instance_history.dag_id = task_instance.dag_id
            AND task_instance_history.run_id = task_instance.run_id
            AND task_instance_history.map_index = task_instance.map_index)
            """)
    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.alter_column(
            "try_id",
            existing_type=sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            nullable=False,
        )
        batch_op.drop_column("id")
        batch_op.alter_column(
            "task_instance_id",
            nullable=False,
            existing_type=sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
        )

        batch_op.create_primary_key(
            "task_instance_history_pkey",
            [
                "try_id",
            ],
        )


def downgrade():
    """Unapply Add try_id to TI and TIH."""
    dialect_name = op.get_bind().dialect.name
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_history_pkey"), type_="primary")
        batch_op.add_column(sa.Column("id", sa.INTEGER, nullable=True))
        batch_op.drop_column("task_instance_id")
    if dialect_name == "postgresql":
        op.execute(
            """
            ALTER TABLE task_instance_history ADD COLUMN row_num SERIAL;
            UPDATE task_instance_history SET id = row_num;
            ALTER TABLE task_instance_history DROP COLUMN row_num;
        """
        )
    elif dialect_name == "mysql":
        op.execute(
            """
            SET @row_number = 0;
            UPDATE task_instance_history
            SET id = (@row_number := @row_number + 1)
            ORDER BY try_id;
            """
        )
    else:
        op.execute("""
            UPDATE task_instance_history
            SET id = (SELECT COUNT(*) FROM task_instance_history t2 WHERE t2.id <= task_instance_history.id);
        """)
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.alter_column("id", nullable=False, existing_type=sa.INTEGER)
        batch_op.drop_column("try_id")
        batch_op.create_primary_key("task_instance_history_pkey", ["id"])
