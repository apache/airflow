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
Add durable Dag processor work claims.

Revision ID: d8c6f3a129b0
Revises: 687eb253de24
Create Date: 2026-10-15 09:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "d8c6f3a129b0"
down_revision = "687eb253de24"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Retain callback and priority requests until their processor acknowledges delivery."""
    with disable_sqlite_fkeys(op):
        for table in ("callback", "dag_priority_parsing_request"):
            with op.batch_alter_table(table) as batch:
                batch.add_column(sa.Column("processor_job_id", sa.Integer(), nullable=True))
                batch.add_column(sa.Column("processor_claim_id", sa.Uuid(), nullable=True))
                batch.create_index(f"idx_{table}_processor_job_id", ["processor_job_id"])
                batch.create_foreign_key(
                    f"{table}_processor_job_id_fkey", "job", ["processor_job_id"], ["id"], ondelete="SET NULL"
                )


def downgrade():
    """Remove processor ownership from callback and priority requests."""
    with disable_sqlite_fkeys(op):
        for table in ("dag_priority_parsing_request", "callback"):
            with op.batch_alter_table(table) as batch:
                batch.drop_constraint(f"{table}_processor_job_id_fkey", type_="foreignkey")
                batch.drop_index(f"idx_{table}_processor_job_id")
                batch.drop_column("processor_claim_id")
                batch.drop_column("processor_job_id")
