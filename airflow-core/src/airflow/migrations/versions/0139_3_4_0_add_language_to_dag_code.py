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
Add language column to dag_code.

Revision ID: e5a91c7f42b3
Revises: ca8499dc1004
Create Date: 2026-09-10 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e5a91c7f42b3"
down_revision = "ca8499dc1004"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply add language column to dag_code."""
    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("language", sa.String(length=64), nullable=False, server_default="python")
        )


def downgrade():
    """Unapply add language column to dag_code."""
    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.drop_column("language")
