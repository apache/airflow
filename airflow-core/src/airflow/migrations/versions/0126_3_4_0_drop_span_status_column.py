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
Drop span_status column.

Revision ID: 436dc127462c
Revises: 5a5d3253e946
Create Date: 2026-07-02 15:54:02.509006

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "436dc127462c"
down_revision = "5a5d3253e946"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

TABLES = ["dag_run", "task_instance", "task_instance_history"]


def upgrade():
    """Apply drop span_status column."""
    with disable_sqlite_fkeys(op):
        for table_name in TABLES:
            with op.batch_alter_table(table_name) as batch_op:
                batch_op.drop_column("span_status")


def downgrade():
    """Unapply drop span_status column."""
    with disable_sqlite_fkeys(op):
        for table_name in TABLES:
            with op.batch_alter_table(table_name) as batch_op:
                batch_op.add_column(
                    sa.Column(
                        "span_status",
                        sa.VARCHAR(length=250),
                        server_default=sa.text("'not_started'"),
                        nullable=False,
                    )
                )
