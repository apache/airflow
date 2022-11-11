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
"""Drop ``KubeResourceVersion`` and ``KubeWorkerId``

Revision ID: bef4f3d11e8b
Revises: e1a11ece99cc
Create Date: 2020-09-22 18:45:28.011654

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "bef4f3d11e8b"
down_revision = "e1a11ece99cc"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"


WORKER_UUID_TABLE = "kube_worker_uuid"
WORKER_RESOURCEVERSION_TABLE = "kube_resource_version"


def upgrade():
    """Apply Drop ``KubeResourceVersion`` and ``KubeWorkerId``entifier tables"""
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()

    if WORKER_UUID_TABLE in tables:
        op.drop_table(WORKER_UUID_TABLE)
    if WORKER_RESOURCEVERSION_TABLE in tables:
        op.drop_table(WORKER_RESOURCEVERSION_TABLE)


def downgrade():
    """Unapply Drop ``KubeResourceVersion`` and ``KubeWorkerId``entifier tables"""
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()

    if WORKER_UUID_TABLE not in tables:
        _add_worker_uuid_table()
    if WORKER_RESOURCEVERSION_TABLE not in tables:
        _add_resource_table()


def _add_worker_uuid_table():
    columns_and_constraints = [
        sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
        sa.Column("worker_uuid", sa.String(255)),
    ]

    conn = op.get_bind()

    # alembic creates an invalid SQL for mssql and mysql dialects
    if conn.dialect.name in {"mysql"}:
        columns_and_constraints.append(sa.CheckConstraint("one_row_id<>0", name="kube_worker_one_row_id"))
    elif conn.dialect.name not in {"mssql"}:
        columns_and_constraints.append(sa.CheckConstraint("one_row_id", name="kube_worker_one_row_id"))

    table = op.create_table(WORKER_UUID_TABLE, *columns_and_constraints)

    op.bulk_insert(table, [{"worker_uuid": ""}])


def _add_resource_table():
    columns_and_constraints = [
        sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
        sa.Column("resource_version", sa.String(255)),
    ]

    conn = op.get_bind()

    # alembic creates an invalid SQL for mssql and mysql dialects
    if conn.dialect.name in {"mysql"}:
        columns_and_constraints.append(
            sa.CheckConstraint("one_row_id<>0", name="kube_resource_version_one_row_id")
        )
    elif conn.dialect.name not in {"mssql"}:
        columns_and_constraints.append(
            sa.CheckConstraint("one_row_id", name="kube_resource_version_one_row_id")
        )

    table = op.create_table(WORKER_RESOURCEVERSION_TABLE, *columns_and_constraints)

    op.bulk_insert(table, [{"resource_version": ""}])
