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
remove external_trigger field.

Revision ID: e00344393f31
Revises: 6a9e7a527a88
Create Date: 2025-02-04 16:45:49.068781

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "e00344393f31"
down_revision = "6a9e7a527a88"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove external_trigger field."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("external_trigger")


def downgrade():
    """Unapply remove external_trigger field."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("external_trigger", sa.BOOLEAN(), autoincrement=False, nullable=True))
    # restore external_trigger field based on run_type
    dag_run_table = sa.table(
        "dag_run", sa.column("external_trigger", sa.BOOLEAN()), sa.column("run_type", sa.String())
    )
    op.execute(
        dag_run_table.update().values(
            external_trigger=sa.case((dag_run_table.c.run_type == "manual", True), else_=False)
        )
    )
