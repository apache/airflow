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
Remove dag_id from Deadline.

Revision ID: a169942745c2
Revises: 808787349f22
Create Date: 2025-08-07 22:26:13.053501

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.models import ID_LEN

# revision identifiers, used by Alembic.
revision = "a169942745c2"
down_revision = "808787349f22"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Remove dag_id from Deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("deadline_dag_id_fkey"), type_="foreignkey")
        batch_op.drop_column("dag_id")


def downgrade():
    """Add dag_id to Deadline."""
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_id", StringID(length=ID_LEN), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("deadline_dag_id_fkey"), "dag", ["dag_id"], ["dag_id"], ondelete="CASCADE"
        )
