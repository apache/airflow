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
Allow bundle priority parsing requests.

Revision ID: a4f3c8d19e72
Revises: 90e4d18ccadf
Create Date: 2026-09-08 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

# revision identifiers, used by Alembic.
revision = "a4f3c8d19e72"
down_revision = "90e4d18ccadf"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Allow priority parsing requests to target an entire Dag bundle."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
            batch_op.alter_column(
                "relative_fileloc",
                existing_type=sa.String(length=2000),
                nullable=True,
            )


def downgrade():
    """Require priority parsing requests to target a Dag file."""
    with disable_sqlite_fkeys(op):
        op.execute("DELETE FROM dag_priority_parsing_request WHERE relative_fileloc IS NULL")
        with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
            batch_op.alter_column(
                "relative_fileloc",
                existing_type=sa.String(length=2000),
                nullable=False,
            )
