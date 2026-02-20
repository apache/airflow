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
Fix migration file ORM inconsistencies.

Revision ID: 888b59e02a5b
Revises: e42d9fcd10d9
Create Date: 2026-02-20 16:13:02.623981

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = "888b59e02a5b"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Fix migration file inconsistencies with ORM."""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=False)
        batch_op.alter_column("is_extra_encrypted", existing_type=sa.BOOLEAN(), nullable=False)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_paused", existing_type=sa.BOOLEAN(), nullable=False)
        batch_op.alter_column(
            "has_import_errors",
            existing_type=sa.BOOLEAN(),
            nullable=False,
            existing_server_default=sa.text("(false)"),
        )

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("state", existing_type=StringID(length=50), nullable=False)
        batch_op.alter_column("log_template_id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("updated_at", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column("dttm", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.alter_column("pool", existing_type=StringID(length=256), nullable=False)
        batch_op.alter_column("slots", existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("try_number", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column(
            "max_tries", existing_type=sa.INTEGER(), nullable=False, existing_server_default=sa.text("'-1'")
        )
        batch_op.alter_column("hostname", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("unixname", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("queue", existing_type=StringID(length=256), nullable=False)
        batch_op.alter_column("priority_weight", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("custom_operator_name", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("executor_config", existing_type=sa.BLOB(), nullable=False)

    with op.batch_alter_table("variable", schema=None) as batch_op:
        batch_op.alter_column("key", existing_type=StringID(length=250), nullable=False)
        batch_op.alter_column("val", existing_type=sa.TEXT(), nullable=False)
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=False)


def downgrade():
    """Unapply Fix migration file inconsistencies with ORM."""
    with op.batch_alter_table("variable", schema=None) as batch_op:
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
        batch_op.alter_column("val", existing_type=sa.TEXT(), nullable=True)
        batch_op.alter_column("key", existing_type=StringID(length=250), nullable=True)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("executor_config", existing_type=sa.BLOB(), nullable=True)
        batch_op.alter_column("custom_operator_name", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column("priority_weight", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("queue", existing_type=StringID(length=256), nullable=True)
        batch_op.alter_column("unixname", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column("hostname", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column(
            "max_tries", existing_type=sa.INTEGER(), nullable=True, existing_server_default=sa.text("'-1'")
        )
        batch_op.alter_column("try_number", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.alter_column("slots", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("pool", existing_type=StringID(length=256), nullable=True)

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column("dttm", existing_type=TIMESTAMP(), nullable=True)

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("updated_at", existing_type=TIMESTAMP(), nullable=True)
        batch_op.alter_column("log_template_id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("state", existing_type=StringID(length=50), nullable=True)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "has_import_errors",
            existing_type=sa.BOOLEAN(),
            nullable=True,
            existing_server_default=sa.text("(false)"),
        )
        batch_op.alter_column("is_paused", existing_type=sa.BOOLEAN(), nullable=True)

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("is_extra_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
