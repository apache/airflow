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
Update dag_run_note.user_id and task_instance_note.user_id columns to String.

Revision ID: 44eabb1904b4
Revises: 16cbcb1c8c36
Create Date: 2024-09-27 09:57:29.830521

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "44eabb1904b4"
down_revision = "16cbcb1c8c36"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("dag_run_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.String(length=128))
    with op.batch_alter_table("task_instance_note") as batch_op:
        batch_op.alter_column("user_id", type_=sa.String(length=128))


def downgrade():
    """Unapply Update dag_run_note.user_id and task_instance_note.user_id columns to String."""
    # Handle the case where user_id contains string values that cannot be converted to integer
    # This is necessary because the migration from 2.11.0 -> 3.0.0 changed user_id from Integer to String
    # and when downgrading, we need to safely handle cases where string user_ids like "admin" exist

    dialect_name = op.get_bind().dialect.name

    # Clean up non-numeric user_id values before type conversion to prevent errors
    if dialect_name == "postgresql":
        # For PostgreSQL, use regex to identify non-integer values and set them to NULL
        op.execute(
            text("""
                UPDATE dag_run_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND user_id !~ '^[0-9]+$'
            """)
        )
        op.execute(
            text("""
                UPDATE task_instance_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND user_id !~ '^[0-9]+$'
            """)
        )
    elif dialect_name == "mysql":
        # For MySQL, use REGEXP to identify non-integer values
        op.execute(
            text("""
                UPDATE dag_run_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND user_id NOT REGEXP '^[0-9]+$'
            """)
        )
        op.execute(
            text("""
                UPDATE task_instance_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND user_id NOT REGEXP '^[0-9]+$'
            """)
        )
    elif dialect_name == "sqlite":
        # SQLite doesn't have regex, so use GLOB pattern matching
        op.execute(
            text("""
                UPDATE dag_run_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND (user_id = '' OR user_id GLOB '*[^0-9]*')
            """)
        )
        op.execute(
            text("""
                UPDATE task_instance_note
                SET user_id = NULL
                WHERE user_id IS NOT NULL
                AND (user_id = '' OR user_id GLOB '*[^0-9]*')
            """)
        )

    # Now alter the column types back to Integer
    with op.batch_alter_table("dag_run_note") as batch_op:
        if dialect_name == "postgresql":
            batch_op.alter_column("user_id", type_=sa.Integer(), postgresql_using="user_id::integer")
        else:
            batch_op.alter_column("user_id", type_=sa.Integer())
    with op.batch_alter_table("task_instance_note") as batch_op:
        if dialect_name == "postgresql":
            batch_op.alter_column("user_id", type_=sa.Integer(), postgresql_using="user_id::integer")
        else:
            batch_op.alter_column("user_id", type_=sa.Integer())
