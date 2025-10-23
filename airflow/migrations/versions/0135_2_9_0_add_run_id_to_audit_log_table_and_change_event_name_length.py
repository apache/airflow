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
Add run_id to (Audit) log table and increase event name length.

Revision ID: d75389605139
Revises: 1fd565369930
Create Date: 2024-02-29 17:50:03.759967

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "d75389605139"
down_revision = "1fd565369930"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def upgrade():
    """Apply Add run_id to Log and increase Log event name length."""
    # Note: we could repopulate the run_id of old runs via a join with DagRun on date + dag_id,
    # But this would incur a potentially heavy migration for non-essential changes.
    # Instead, we've chosen to only populate this column from 2.9.0 onwards.
    with op.batch_alter_table("log") as batch_op:
        batch_op.add_column(sa.Column("run_id", StringID(), nullable=True))
        batch_op.alter_column("event", type_=sa.String(60))


def downgrade():
    """Unapply Add run_id to Log and increase Log event name length."""
    with op.batch_alter_table("log") as batch_op:
        batch_op.drop_column("run_id")

    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table("log") as batch_op:
            batch_op.drop_index("idx_log_event")
            batch_op.alter_column("event", type_=sa.String(30))
            batch_op.create_index("idx_log_event", ["event"])
    else:
        with op.batch_alter_table("log") as batch_op:
            batch_op.alter_column("event", type_=sa.String(30))
