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
Add length to dag_bundle_team.dag_bundle_name.

Revision ID: ab6dc0c82d0eg
Revises: ab6dc0c82d0e
Create Date: 2025-09-22 12:34:56.000000

"""

from __future__ import annotations

from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "1b2c3d4e5f6g"
down_revision = "ab6dc0c82d0e"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.alter_column(
            "dag_bundle_name",
            type_=StringID(length=250),
            existing_nullable=False,
        )


def downgrade():
    with op.batch_alter_table("dag_bundle_team") as batch_op:
        # revert to plain StringID() without length (works on Postgres, fails on MySQL)
        batch_op.alter_column(
            "dag_bundle_name",
            type_=StringID(),
            existing_nullable=False,
        )
