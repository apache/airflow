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
Enforce the new ``NOT NULL`` expectations for ``log.event`` and ``dag.is_stale``.

Revision ID: edc4f85a4619
Revises: b12d4f98a91e
Create Date: 2025-12-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "edc4f85a4619"
down_revision = "b12d4f98a91e"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Bring existing deployments in line with 0010 and 0067."""
    # Ensure `log.event` can safely transition to NOT NULL.
    op.execute("UPDATE log SET event = '' WHERE event IS NULL")
    with op.batch_alter_table("log") as batch_op:
        batch_op.alter_column("event", existing_type=sa.String(60), nullable=False)

    # Make sure DAG rows that survived the old 0067 path are not NULL.
    op.execute("UPDATE dag SET is_stale = false WHERE is_stale IS NULL")
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_stale", existing_type=sa.Boolean, nullable=False)


def downgrade():
    """Allow the columns to accept NULL again for older state reversions."""
    with op.batch_alter_table("log") as batch_op:
        batch_op.alter_column("event", existing_type=sa.String(60), nullable=True)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_stale", existing_type=sa.Boolean, nullable=True)
