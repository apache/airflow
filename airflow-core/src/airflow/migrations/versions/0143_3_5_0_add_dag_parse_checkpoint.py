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
Add checkpoints for Dag result publication.

Revision ID: e941ab8243b7
Revises: c71065a1b14f
Create Date: 2026-10-05 13:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "e941ab8243b7"
down_revision = "c71065a1b14f"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Store the latest accepted publication for each Job and source."""
    op.create_table(
        "dag_parse_checkpoint",
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("source_key", sa.String(64), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_sequence", sa.BigInteger(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("accepted_at", UtcDateTime(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("job_id", "source_key"),
    )


def downgrade():
    """Remove publication checkpoints."""
    op.drop_table("dag_parse_checkpoint")
