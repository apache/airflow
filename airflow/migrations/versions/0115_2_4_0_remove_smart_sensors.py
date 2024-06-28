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
Remove smart sensors.

Revision ID: f4ff391becb5
Revises: 0038cd0c28b4
Create Date: 2022-08-03 11:33:44.777945

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import func
from sqlalchemy.sql import column, table

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = "f4ff391becb5"
down_revision = "0038cd0c28b4"
branch_labels = None
depends_on = None
airflow_version = "2.4.0"


def upgrade():
    """Apply Remove smart sensors."""
    op.drop_table("sensor_instance")

    """Minimal model definition for migrations"""
    task_instance = table("task_instance", column("state", sa.String))
    op.execute(task_instance.update().where(task_instance.c.state == "sensing").values({"state": "failed"}))


def downgrade():
    """Unapply Remove smart sensors."""
    op.create_table(
        "sensor_instance",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("execution_date", TIMESTAMP, nullable=False),
        sa.Column("state", sa.String(length=20), nullable=True),
        sa.Column("try_number", sa.Integer(), nullable=True),
        sa.Column("start_date", TIMESTAMP, nullable=True),
        sa.Column("operator", sa.String(length=1000), nullable=False),
        sa.Column("op_classpath", sa.String(length=1000), nullable=False),
        sa.Column("hashcode", sa.BigInteger(), nullable=False),
        sa.Column("shardcode", sa.Integer(), nullable=False),
        sa.Column("poke_context", sa.Text(), nullable=False),
        sa.Column("execution_context", sa.Text(), nullable=True),
        sa.Column("created_at", TIMESTAMP, default=func.now, nullable=False),
        sa.Column("updated_at", TIMESTAMP, default=func.now, nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ti_primary_key", "sensor_instance", ["dag_id", "task_id", "execution_date"], unique=True)
    op.create_index("si_hashcode", "sensor_instance", ["hashcode"], unique=False)
    op.create_index("si_shardcode", "sensor_instance", ["shardcode"], unique=False)
    op.create_index("si_state_shard", "sensor_instance", ["state", "shardcode"], unique=False)
    op.create_index("si_updated_at", "sensor_instance", ["updated_at"], unique=False)
