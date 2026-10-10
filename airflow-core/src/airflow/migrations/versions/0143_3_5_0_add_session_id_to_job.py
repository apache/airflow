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
Add session_id and registration_id to Job.

Revision ID: c71065a1b14f
Revises: e7c2a91bd540
Create Date: 2026-10-05 08:40:20.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "c71065a1b14f"
down_revision = "e7c2a91bd540"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the nullable, unique session_id and registration_id columns to Job."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("job", schema=None) as batch_op:
            batch_op.add_column(sa.Column("session_id", sa.Uuid(), nullable=True))
            batch_op.add_column(sa.Column("registration_id", sa.Uuid(), nullable=True))
            batch_op.create_unique_constraint("job_session_id_uq", ["session_id"])
            batch_op.create_unique_constraint("job_registration_id_uq", ["registration_id"])


def downgrade():
    """Remove session_id and registration_id from Job."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("job", schema=None) as batch_op:
            batch_op.drop_constraint("job_registration_id_uq", type_="unique")
            batch_op.drop_constraint("job_session_id_uq", type_="unique")
            batch_op.drop_column("registration_id")
            batch_op.drop_column("session_id")
