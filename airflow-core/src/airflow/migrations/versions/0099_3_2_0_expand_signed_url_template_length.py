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
Expand signed_url_template to TEXT to avoid truncation.

Revision ID: 2d4c2f3c3b8a
Revises: e79fc784f145
Create Date: 2026-01-22 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "2d4c2f3c3b8a"
down_revision = "e79fc784f145"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Expand signed_url_template to TEXT to avoid truncation."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.alter_column(
            "signed_url_template",
            existing_type=sa.String(length=200),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade():
    """Unapply Expand signed_url_template to TEXT to avoid truncation."""
    with op.batch_alter_table("dag_bundle", schema=None) as batch_op:
        batch_op.alter_column(
            "signed_url_template",
            existing_type=sa.Text(),
            type_=sa.String(length=200),
            existing_nullable=True,
        )
