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
Add new otel span fields.

Revision ID: ec62e120484d
Revises: be2cc2f742cf
Create Date: 2025-04-02 15:54:39.926913

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "ec62e120484d"
down_revision = "be2cc2f742cf"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add new otel span fields."""
    op.add_column("dag_run", sa.Column("scheduled_by_job_id", sa.Integer, nullable=True))
    op.add_column("dag_run", sa.Column("context_carrier", ExtendedJSON, nullable=True))
    op.add_column(
        "dag_run", sa.Column("span_status", sa.String(250), nullable=False, server_default="not_started")
    )

    op.add_column("task_instance", sa.Column("context_carrier", ExtendedJSON, nullable=True))
    op.add_column(
        "task_instance",
        sa.Column("span_status", sa.String(250), nullable=False, server_default="not_started"),
    )
    op.add_column("task_instance_history", sa.Column("context_carrier", ExtendedJSON, nullable=True))
    op.add_column(
        "task_instance_history",
        sa.Column("span_status", sa.String(250), nullable=False, server_default="not_started"),
    )


def downgrade():
    """Unapply add new otel span fields."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_column("scheduled_by_job_id")
        batch_op.drop_column("context_carrier")
        batch_op.drop_column("span_status")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("context_carrier")
        batch_op.drop_column("span_status")

    with op.batch_alter_table("task_instance_history") as batch_op:
        batch_op.drop_column("context_carrier")
        batch_op.drop_column("span_status")
