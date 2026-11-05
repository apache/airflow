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
Add health_check_result and owner_job_id to Job.

Revision ID: feb8b1e719ee
Revises: 16d7301f124b
Create Date: 2026-11-05 09:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "feb8b1e719ee"
down_revision = "16d7301f124b"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the nullable health_check_result and owner_job_id columns to Job."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("job", schema=None) as batch_op:
            batch_op.add_column(sa.Column("health_check_result", sa.String(length=20), nullable=True))
            batch_op.add_column(sa.Column("owner_job_id", sa.Integer(), nullable=True))


def downgrade():
    """Remove health_check_result and owner_job_id from Job."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("job", schema=None) as batch_op:
            batch_op.drop_column("owner_job_id")
            batch_op.drop_column("health_check_result")
