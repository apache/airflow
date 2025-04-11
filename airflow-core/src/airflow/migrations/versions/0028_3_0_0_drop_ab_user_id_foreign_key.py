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

from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "044f740568ec"
down_revision = "5f2621c13b39"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def table_exists(table_name):
    """Check if a table exists in the database."""
    inspector = inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def constraint_exists(table_name, constraint_name):
    """Check if a foreign key constraint exists on a table."""
    inspector = inspect(op.get_bind())
    foreign_keys = inspector.get_foreign_keys(table_name)
    return any(fk["name"] == constraint_name for fk in foreign_keys)


def index_exists(table_name, index_name):
    """Check if an index exists on a table."""
    inspector = inspect(op.get_bind())
    indexes = inspector.get_indexes(table_name)
    return any(idx["name"] == index_name for idx in indexes)


def upgrade():
    """Apply Drop ab_user.id foreign key."""
    if constraint_exists("dag_run_note", "dag_run_note_user_fkey"):
        with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
            batch_op.drop_constraint("dag_run_note_user_fkey", type_="foreignkey")

    if constraint_exists("task_instance_note", "task_instance_note_user_fkey"):
        with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
            batch_op.drop_constraint("task_instance_note_user_fkey", type_="foreignkey")

    if op.get_bind().dialect.name == "mysql":
        if index_exists("dag_run_note", "dag_run_note_user_fkey"):
            with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
                batch_op.drop_index("dag_run_note_user_fkey")

        if index_exists("task_instance_note", "task_instance_note_user_fkey"):
            with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
                batch_op.drop_index("task_instance_note_user_fkey")


def downgrade():
    """Unapply Drop ab_user.id foreign key."""
    if table_exists("ab_user"):
        with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
            batch_op.create_foreign_key("task_instance_note_user_fkey", "ab_user", ["user_id"], ["id"])

        with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
            batch_op.create_foreign_key("dag_run_note_user_fkey", "ab_user", ["user_id"], ["id"])

    if op.get_bind().dialect.name == "mysql":
        with op.batch_alter_table("task_instance_note", schema=None) as batch_op:
            batch_op.create_index("task_instance_note_user_fkey", ["user_id"], unique=False)

        with op.batch_alter_table("dag_run_note", schema=None) as batch_op:
            batch_op.create_index("dag_run_note_user_fkey", ["user_id"], unique=False)
