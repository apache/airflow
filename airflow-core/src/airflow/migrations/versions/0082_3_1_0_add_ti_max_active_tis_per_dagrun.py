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
Add max_active_tis_per_dag and max_active_tis_per_dagrun to task_instance table.

Revision ID: 2f49f2dae90c
Revises: 808787349f22
Create Date: 2025-08-06 12:44:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "2f49f2dae90c"
down_revision = "808787349f22"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add max_active_tis_per_dag and max_active_tis_per_dagrun to task_instance table."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("max_active_tis_per_dag", sa.Integer, nullable=True))
        batch_op.add_column(sa.Column("max_active_tis_per_dagrun", sa.Integer, nullable=True))
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("max_active_tis_per_dag", sa.Integer, nullable=True))
        batch_op.add_column(sa.Column("max_active_tis_per_dagrun", sa.Integer, nullable=True))


def downgrade():
    """Remove callback_state from deadline."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_column("max_active_tis_per_dag")
        batch_op.drop_column("max_active_tis_per_dagrun")
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("max_active_tis_per_dag")
        batch_op.drop_column("max_active_tis_per_dagrun")
