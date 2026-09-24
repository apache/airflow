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
Replace the ``job.team_name`` column with a ``job_team`` association table.

A single job can serve more than one team -- a Dag processor parsing bundles that belong to
several teams is relevant to each of them -- so the association is many-to-many.

Revision ID: c9f4b3e7a218
Revises: 3b7a91c5df20
Create Date: 2026-09-22 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c9f4b3e7a218"
down_revision = "3b7a91c5df20"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Replace ``job.team_name`` with the ``job_team`` association table."""
    # The column goes before the table is created: on SQLite, dropping a column rebuilds the
    # whole ``job`` table, which a ``job_team`` foreign key pointing at it would not survive.
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("job_team_name_fkey"), type_="foreignkey")
        batch_op.drop_column("team_name")

    op.create_table(
        "job_team",
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("team_name", sa.String(length=50), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "team_name", name=op.f("job_team_pkey")),
        sa.ForeignKeyConstraint(
            columns=("job_id",),
            refcolumns=["job.id"],
            name="job_team_job_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            columns=("team_name",),
            refcolumns=["team.name"],
            name="job_team_team_name_fkey",
            ondelete="CASCADE",
        ),
        sa.Index("idx_job_team_team_name", "team_name", unique=False),
    )


def downgrade():
    """Restore the ``job.team_name`` column."""
    op.drop_table("job_team")

    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.add_column(sa.Column("team_name", sa.String(length=50), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("job_team_name_fkey"), "team", ["team_name"], ["name"], ondelete="SET NULL"
        )
