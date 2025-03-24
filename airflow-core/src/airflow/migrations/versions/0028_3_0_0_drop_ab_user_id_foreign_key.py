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
Drop ab_user.id foreign key.

Revision ID: 044f740568ec
Revises: 5f2621c13b39
Create Date: 2024-08-02 07:18:29.830521

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP
from airflow.migrations.utils import mysql_drop_foreignkey_if_exists
from airflow.models.base import metadata

# revision identifiers, used by Alembic.
revision = "044f740568ec"
down_revision = "5f2621c13b39"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Drop ab_user.id foreign key."""
    conn = op.get_bind()
    dialect = conn.dialect.name
    if dialect == "mysql":
        mysql_drop_foreignkey_if_exists("dag_run_note", "dag_run_note_user_fkey", op)
        mysql_drop_foreignkey_if_exists("task_instance_note", "task_instance_note_user_fkey", op)

    elif dialect == "postgresql":
        conn.execute(sa.text("ALTER TABLE dag_run_note DROP CONSTRAINT IF EXISTS dag_run_note_user_fkey"))
        conn.execute(
            sa.text("ALTER TABLE task_instance_note DROP CONSTRAINT IF EXISTS task_instance_note_user_fkey")
        )
    else:
        # SQLite does not support DROP CONSTRAINT
        # We have to recreate the table without the constraint
        conn.execute(sa.text("PRAGMA foreign_keys=off"))
        dag_run_note_new = sa.Table(
            "dag_run_note_new",
            metadata,
            sa.Column("user_id", sa.Integer),
            sa.Column("dag_run_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("content", sa.String(1000)),
            sa.Column("created_at", TIMESTAMP(), nullable=False),
            sa.Column("updated_at", TIMESTAMP(), nullable=False),
            sa.ForeignKeyConstraint(
                ("dag_run_id",), ["dag_run.id"], name="dag_run_note_dr_fkey", ondelete="CASCADE"
            ),
        )
        dag_run_note_new.create(bind=op.get_bind())
        conn.execute(
            sa.text("""
            INSERT INTO dag_run_note_new (user_id, dag_run_id, content, created_at, updated_at)
            SELECT user_id, dag_run_id, content, created_at, updated_at FROM dag_run_note
            """)
        )
        conn.execute(sa.text("DROP TABLE dag_run_note"))
        conn.execute(sa.text("ALTER TABLE dag_run_note_new RENAME TO dag_run_note"))

        # task_instance_note
        task_instance_note_new = sa.Table(
            "task_instance_note_new",
            metadata,
            sa.Column("user_id", sa.Integer),
            sa.Column("task_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("dag_id", sa.String(250), primary_key=True, nullable=False),
            sa.Column("run_id", sa.String(250), primary_key=True, nullable=False),
            sa.Column("map_index", sa.Integer, primary_key=True, nullable=False),
            sa.Column("content", sa.String(1000)),
            sa.Column("created_at", TIMESTAMP(), nullable=False),
            sa.Column("updated_at", TIMESTAMP(), nullable=False),
            sa.PrimaryKeyConstraint(
                "task_id", "dag_id", "run_id", "map_index", name="task_instance_note_pkey"
            ),
            sa.ForeignKeyConstraint(
                (
                    "task_id",
                    "dag_id",
                    "run_id",
                    "map_index",
                ),
                [
                    "task_instance.task_id",
                    "task_instance.dag_id",
                    "task_instance.run_id",
                    "task_instance.map_index",
                ],
                name="task_instance_note_ti_fkey",
                ondelete="CASCADE",
            ),
        )
        task_instance_note_new.create(bind=op.get_bind())
        conn.execute(
            sa.text("""
            INSERT INTO task_instance_note_new (user_id, task_id, dag_id, run_id, map_index, content, created_at, updated_at)
            SELECT user_id, task_id, dag_id, run_id, map_index, content, created_at, updated_at FROM task_instance_note
            """)
        )
        conn.execute(sa.text("DROP TABLE task_instance_note"))
        conn.execute(sa.text("ALTER TABLE task_instance_note_new RENAME TO task_instance_note"))
        conn.execute(sa.text("PRAGMA foreign_keys=on"))


def downgrade():
    """Unapply Drop ab_user.id foreign key."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == "sqlite":
        table_exists = conn.execute(
            sa.text("SELECT name FROM sqlite_master WHERE type='table' AND name='ab_user'")
        ).fetchone()
    else:
        table_exists = conn.execute(
            sa.text("SELECT 1 FROM information_schema.tables WHERE table_name = 'ab_user'")
        ).scalar()
    if table_exists:
        with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
            batch_op.create_foreign_key("task_instance_note_user_fkey", "ab_user", ["user_id"], ["id"])

        with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
            batch_op.create_foreign_key("dag_run_note_user_fkey", "ab_user", ["user_id"], ["id"])

        if op.get_bind().dialect.name == "mysql":
            with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
                batch_op.create_index("task_instance_note_user_fkey", ["user_id"], unique=False)

            with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
                batch_op.create_index("dag_run_note_user_fkey", ["user_id"], unique=False)
