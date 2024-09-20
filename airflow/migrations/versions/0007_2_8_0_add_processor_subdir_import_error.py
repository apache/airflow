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
Add processor_subdir to ImportError.

Revision ID: 10b52ebd31f7
Revises: bd5dfbe21f88
Create Date: 2023-11-29 16:54:48.101834

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "10b52ebd31f7"
down_revision = "bd5dfbe21f88"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"


def upgrade():
    """Apply Add processor_subdir to ImportError."""
    conn = op.get_bind()

    with op.batch_alter_table("import_error") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.add_column(sa.Column("processor_subdir", sa.Text(length=2000), nullable=True))
        else:
            batch_op.add_column(sa.Column("processor_subdir", sa.String(length=2000), nullable=True))


def downgrade():
    """Unapply Add processor_subdir to ImportError."""
    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")
