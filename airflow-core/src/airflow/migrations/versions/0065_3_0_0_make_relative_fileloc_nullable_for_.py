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
make relative_fileloc nullable for reserialized all bundles.

Revision ID: 62e9fc3d00a4
Revises: be2cc2f742cf
Create Date: 2025-04-01 08:30:42.009221

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "62e9fc3d00a4"
down_revision = "be2cc2f742cf"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply make relative_fileloc nullable for reserialized all bundles."""
    with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
        batch_op.alter_column("relative_fileloc", existing_type=sa.VARCHAR(length=2000), nullable=True)


def downgrade():
    """Unapply make relative_fileloc nullable for reserialized all bundles."""
    op.execute("DELETE FROM dag_priority_parsing_request WHERE relative_fileloc IS NULL")

    with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
        batch_op.alter_column("relative_fileloc", existing_type=sa.VARCHAR(length=2000), nullable=False)
