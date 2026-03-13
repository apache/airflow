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
Add allowed_run_types to dag.

Revision ID: e42d9fcd10d9
Revises: f8c9d7e6b5a4
Create Date: 2026-02-12 11:49:40.753440

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e42d9fcd10d9"
down_revision = "f8c9d7e6b5a4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add allowed_run_types column to dag table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("allowed_run_types", sa.JSON(), nullable=True))


def downgrade():
    """Remove allowed_run_types column from dag table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("allowed_run_types")
