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
Add is_stub to task_instance.

Revision ID: b4f1a7d2c903
Revises: ee86eed19e24
Create Date: 2026-10-27 09:12:44.317602

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b4f1a7d2c903"
down_revision = "ee86eed19e24"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

TABLE_NAME = "task_instance"


def upgrade():
    """Add is_stub to task_instance."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("is_stub", sa.Boolean(), nullable=True))


def downgrade():
    """Remove is_stub from task_instance."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column("is_stub")
