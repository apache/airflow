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
Add try_number to audit log.

Revision ID: 41b3bc7c0272
Revises: ec3471c1e067
Create Date: 2024-07-11 14:48:58.998259

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "41b3bc7c0272"
down_revision = "ec3471c1e067"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply add try_number to audit log."""
    with op.batch_alter_table("log") as batch_op:
        batch_op.add_column(sa.Column("try_number", sa.Integer(), nullable=True))
        batch_op.create_index(
            "idx_log_task_instance", ["dag_id", "task_id", "run_id", "map_index", "try_number"], unique=False
        )


def downgrade():
    """Unapply add try_number to audit log."""
    with op.batch_alter_table("log") as batch_op:
        batch_op.drop_index("idx_log_task_instance")
        batch_op.drop_column("try_number")
