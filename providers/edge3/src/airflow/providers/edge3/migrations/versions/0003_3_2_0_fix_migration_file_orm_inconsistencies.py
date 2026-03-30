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
Fix migration file/ORM inconsistencies.

Revision ID: 8c275b6fbaa8
Revises: b3c4d5e6f7a8
Create Date: 2026-02-25 20:07:12.723427

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8c275b6fbaa8"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None
edge3_version = "3.2.0"


def upgrade() -> None:
    from airflow.migrations.db_types import TIMESTAMP

    with op.batch_alter_table("edge_job", schema=None) as batch_op:
        batch_op.alter_column(
            "queued_dttm", existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            "last_update", existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table("edge_logs", schema=None) as batch_op:
        batch_op.alter_column(
            "log_chunk_time", existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=False
        )

    with op.batch_alter_table("edge_worker", schema=None) as batch_op:
        batch_op.alter_column(
            "first_online", existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            "last_update", existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=True
        )


def downgrade() -> None:
    from airflow.migrations.db_types import TIMESTAMP

    with op.batch_alter_table("edge_worker", schema=None) as batch_op:
        batch_op.alter_column(
            "last_update", existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=True
        )
        batch_op.alter_column(
            "first_online", existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=True
        )

    with op.batch_alter_table("edge_logs", schema=None) as batch_op:
        batch_op.alter_column(
            "log_chunk_time", existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=False
        )

    with op.batch_alter_table("edge_job", schema=None) as batch_op:
        batch_op.alter_column(
            "last_update", existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=True
        )
        batch_op.alter_column(
            "queued_dttm", existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=True
        )
