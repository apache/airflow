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
Add trigger_id to deadline.

Revision ID: 09fa89ba1710
Revises: 40f7c30a228b
Create Date: 2025-07-11 22:37:17.706269

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "09fa89ba1710"
down_revision = "40f7c30a228b"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add trigger_id to deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("trigger_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])


def downgrade():
    """Remove trigger_id from deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("deadline_trigger_id_fkey"), type_="foreignkey")
        batch_op.drop_column("trigger_id")
