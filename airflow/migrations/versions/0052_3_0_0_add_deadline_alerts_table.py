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
Revises: 038dc8bc6284
Create Date: 2024-12-05 22:08:38.997054

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.migrations.db_types import StringID
from airflow.models import ID_LEN

# revision identifiers, used by Alembic.
revision = "237cef8dfea1"
down_revision = "038dc8bc6284"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    op.create_table(
        "deadline",
        sa.Column("id", UUIDType(binary=False), nullable=False),
        sa.Column("dag_id", StringID(length=ID_LEN), nullable=True),
        sa.Column("dagrun_id", sa.Integer(), nullable=True),
        sa.Column("deadline", sa.DateTime(), nullable=False),
        sa.Column("callback", sa.String(length=500), nullable=False),
        sa.Column("callback_kwargs", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("deadline_pkey")),
        sa.ForeignKeyConstraint(columns=("dagrun_id",), refcolumns=["dag_run.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(columns=("dag_id",), refcolumns=["dag.dag_id"], ondelete="CASCADE"),
        sa.Index("deadline_idx", "deadline", unique=False),
    )


def downgrade():
    op.drop_table("deadline")
