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

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "e229247a6cb1"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    op.create_table(
        "dag_bundle",
        sa.Column("name", sa.String(length=250), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("version", sa.String(length=200), nullable=True),
        sa.Column("last_refreshed", UtcDateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("name", name=op.f("dag_bundle_pkey")),
    )
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", sa.String(length=250), nullable=True))
        batch_op.add_column(sa.Column("bundle_version", sa.String(length=200), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("dag_bundle_name_fkey"), "dag_bundle", ["bundle_name"], ["name"]
        )
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_version", sa.String(length=250), nullable=True))

    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", sa.String(length=250), nullable=True))

    with op.batch_alter_table("dag_version", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", StringID()))
        batch_op.add_column(sa.Column("bundle_version", StringID()))


def downgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_bundle_name_fkey"), type_="foreignkey")
        batch_op.drop_column("bundle_version")
        batch_op.drop_column("bundle_name")
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("bundle_version")

    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.drop_column("bundle_name")

    with op.batch_alter_table("dag_version", schema=None) as batch_op:
        batch_op.drop_column("bundle_version")
        batch_op.drop_column("bundle_name")

    op.drop_table("dag_bundle")
