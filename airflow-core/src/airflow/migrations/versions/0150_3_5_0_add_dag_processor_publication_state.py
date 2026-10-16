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
Add Dag processor publication receipts and source revisions.

Revision ID: 687eb253de24
Revises: c71065a1b14f
Create Date: 2026-10-05 13:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import UtcDateTime

revision = "687eb253de24"
down_revision = "c71065a1b14f"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Store publication receipts and fence publications against accepted discovery snapshots."""
    # SQLite rebuilds these tables, and dropping the old copy would cascade to deadline and team rows.
    with disable_sqlite_fkeys(op):
        op.create_table(
            "dag_parse_checkpoint",
            sa.Column("job_id", sa.Integer(), nullable=False),
            sa.Column("source_key", sa.String(64), nullable=False),
            sa.Column("attempt_id", sa.Uuid(), nullable=False),
            sa.Column("dispatch_sequence", sa.BigInteger(), nullable=False),
            sa.Column("payload_hash", sa.String(64), nullable=False),
            sa.Column("accepted_at", UtcDateTime(), nullable=False),
            sa.Column("bundle_revision", sa.Uuid(), nullable=True),
            sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("job_id", "source_key"),
        )
        op.create_index("idx_dag_bundle_name_is_stale", "dag", ["bundle_name", "is_stale"])
        with op.batch_alter_table("dag_bundle") as batch:
            batch.add_column(sa.Column("parse_revision", sa.Uuid(), nullable=True))
            batch.add_column(sa.Column("inventory_hash", sa.String(64), nullable=True))


def downgrade():
    """Remove publication receipts and discovery revision metadata."""
    with disable_sqlite_fkeys(op):
        if op.get_bind().dialect.name == "mysql":
            # MySQL lets idx_dag_bundle_name_is_stale back the bundle_name foreign key and refuses to drop
            # it while the key exists; recreating the key restores the index MySQL keeps for it.
            op.drop_constraint("dag_bundle_name_fkey", "dag", type_="foreignkey")
            op.drop_index("idx_dag_bundle_name_is_stale", table_name="dag")
            op.create_foreign_key("dag_bundle_name_fkey", "dag", "dag_bundle", ["bundle_name"], ["name"])
        else:
            op.drop_index("idx_dag_bundle_name_is_stale", table_name="dag")
        with op.batch_alter_table("dag_bundle") as batch:
            batch.drop_column("inventory_hash")
            batch.drop_column("parse_revision")
    op.drop_table("dag_parse_checkpoint")
