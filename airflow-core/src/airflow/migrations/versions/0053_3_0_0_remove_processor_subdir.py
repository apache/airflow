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
Remove processor_subdir.

Revision ID: 5c9c0231baa2
Revises: 237cef8dfea1
Create Date: 2024-12-18 19:10:26.962464
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "5c9c0231baa2"
down_revision = "237cef8dfea1"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Remove processor_subdir."""
    with op.batch_alter_table("callback_request", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")

    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")

    with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
        batch_op.drop_column("processor_subdir")


def downgrade():
    """Unapply Remove processor_subdir."""
    with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("processor_subdir", sa.VARCHAR(length=2000), autoincrement=False, nullable=True)
        )

    with op.batch_alter_table("import_error", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("processor_subdir", sa.VARCHAR(length=2000), autoincrement=False, nullable=True)
        )

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("processor_subdir", sa.VARCHAR(length=2000), autoincrement=False, nullable=True)
        )

    with op.batch_alter_table("callback_request", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("processor_subdir", sa.VARCHAR(length=2000), autoincrement=False, nullable=True)
        )
