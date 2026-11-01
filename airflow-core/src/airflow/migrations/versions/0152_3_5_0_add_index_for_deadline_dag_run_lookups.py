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
Add index for deadline Dag run lookups.

Revision ID: 8a64f0369d2c
Revises: 38c8df3fcb9c
Create Date: 2026-10-30 00:57:37.288181

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8a64f0369d2c"
down_revision = "38c8df3fcb9c"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Index deadline lookups by Dag run."""
    if op.get_bind().dialect.name == "mysql":
        op.execute("ALTER TABLE deadline RENAME INDEX deadline_dagrun_id_fkey TO deadline_dagrun_id_idx")
        return

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.create_index(
            "deadline_dagrun_id_idx", ["dagrun_id"], unique=False, sqlite_where=sa.text("missed = 0")
        )


def downgrade():
    """Restore the previous deadline indexes."""
    if op.get_bind().dialect.name == "mysql":
        op.execute("ALTER TABLE deadline RENAME INDEX deadline_dagrun_id_idx TO deadline_dagrun_id_fkey")
        return

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_index("deadline_dagrun_id_idx")
