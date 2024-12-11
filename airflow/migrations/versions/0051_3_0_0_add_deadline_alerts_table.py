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
Add deadline alerts table.

Revision ID: 237cef8dfea1
Revises: e229247a6cb1
Create Date: 2024-12-05 22:08:38.997054

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

# revision identifiers, used by Alembic.
revision = "237cef8dfea1"
down_revision = "e229247a6cb1"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    op.create_table(
        "deadline",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=True),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("deadline", sa.DateTime(), nullable=False),
        sa.Column("callback", sa.String(), nullable=False),
        sa.Column("callback_kwargs", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_pkey")),
        sa.ForeignKeyConstraint(
            columns=("run_id",),
            refcolumns=["dag_run.id"],
            name=op.f("dag_run_id_fkey"),
        ),
        sa.ForeignKeyConstraint(
            columns=("dag_id",),
            refcolumns=["dag.dag_id"],
            name=op.f("dag_id_fkey"),
        ),
    )
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.create_index("deadline_idx", ["deadline"], unique=False)


def downgrade():
    op.drop_table("deadline")
