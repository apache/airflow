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
"""Add kubernetes scheduler uniqueness

Revision ID: 86770d1215c0
Revises: 27c6a30d7c24
Create Date: 2018-04-03 15:31:20.814328

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "86770d1215c0"
down_revision = "27c6a30d7c24"
branch_labels = None
depends_on = None
airflow_version = "1.10.0"

RESOURCE_TABLE = "kube_worker_uuid"


def upgrade():

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

    table = op.create_table(RESOURCE_TABLE, *columns_and_constraints)

    op.bulk_insert(table, [{"worker_uuid": ""}])


def downgrade():
    op.drop_table(RESOURCE_TABLE)
