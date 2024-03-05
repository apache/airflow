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

"""Add state table

Revision ID: 41729c6bd933
Revises: 1fd565369930
Create Date: 2024-03-05 00:01:45.086985

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = '41729c6bd933'
down_revision = '1fd565369930'
branch_labels = None
depends_on = None
airflow_version = '2.9.0'


def upgrade():
    """Apply Add state table"""
    op.create_table(
        "dagrun_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("type", StringID(), nullable=False),
        sa.Column("name", sa.String(20), nullable=False),
        sa.Column("message", StringID(), nullable=True),
        sa.Column("timestamp", sa.DateTime()),
        sa.Column("task_instance_id", sa.Integer(), nullable=True),
        sa.Column("dag_run_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(['dag_run_id'], ["dag_run.id"]),
    )


def downgrade():
    """Unapply Add state table"""
    op.drop_table("base_state")
