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
Change the on-delete behaviour of dag_run.created_dag_version_id foreign key constraint to RESTRICT.

Revision ID: b6d94f271ae3
Revises: 7a98f1b7dbd3
Create Date: 2026-07-20 12:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "b6d94f271ae3"
down_revision = "7a98f1b7dbd3"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Alter dag_run.created_dag_version_id foreign key to use ON DELETE RESTRICT."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("created_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("created_dag_version_id_fkey"),
            "dag_version",
            ["created_dag_version_id"],
            ["id"],
            ondelete="RESTRICT",
        )


def downgrade():
    """Revert dag_run.created_dag_version_id foreign key to ON DELETE SET NULL."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("created_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("created_dag_version_id_fkey"),
            "dag_version",
            ["created_dag_version_id"],
            ["id"],
            ondelete="SET NULL",
        )
