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
Add DagBundleModel.

Revision ID: e229247a6cb1
Revises: eed27faa34e3
Create Date: 2024-12-02 22:06:40.587330
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime

revision = "e229247a6cb1"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    op.create_table(
        "dag_bundle",
        sa.Column("id", UUIDType(binary=False), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("classpath", sa.String(length=1000), nullable=False),
        sa.Column("kwargs", ExtendedJSON(), nullable=True),
        sa.Column("refresh_interval", sa.Integer(), nullable=True),
        sa.Column("latest_version", sa.String(length=200), nullable=True),
        sa.Column("last_refreshed", UtcDateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("dag_bundle_pkey")),
        sa.UniqueConstraint("name", name=op.f("dag_bundle_name_uq")),
    )
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_id", UUIDType(binary=False), nullable=True))
        batch_op.add_column(sa.Column("latest_bundle_version", sa.String(length=200), nullable=True))
        batch_op.create_foreign_key(batch_op.f("dag_bundle_id_fkey"), "dag_bundle", ["bundle_id"], ["id"])


def downgrade():
    """Unapply Add DagBundleModel."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_bundle_id_fkey"), type_="foreignkey")
        batch_op.drop_column("latest_bundle_version")
        batch_op.drop_column("bundle_id")

    op.drop_table("dag_bundle")
