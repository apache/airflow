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

"""Add index on last_scheduling_decision NULLS FIRST, execution_date, state for queued dagrun

Revision ID: 14db5484317e
Revises: 937cbd173ca1
Create Date: 2023-05-14 21:16:42.399167

"""
from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "14db5484317e"
down_revision = "937cbd173ca1"
branch_labels = None
depends_on = None
airflow_version = "2.7.0"


def upgrade():
    """Apply Add index on last_scheduling_decision NULLS FIRST, execution_date, state for queued dagrun"""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.create_index(
                "idx_last_scheduling_decision_queued",
                [text("last_scheduling_decision NULLS FIRST"), "execution_date", "state"],
                postgresql_where=text("state='queued'"),
            )


def downgrade():
    """Unapply Add index on last_scheduling_decision NULLS FIRST, execution_date, state for queued dagrun"""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.drop_index("idx_last_scheduling_decision_queued")
