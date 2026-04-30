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
Support bundles in DagPriorityParsingRequest.

Revision ID: be2cc2f742cf
Revises: d469d27e2a64
Create Date: 2025-03-13 00:16:31.011374
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "be2cc2f742cf"
down_revision = "d469d27e2a64"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    # It's just easier to delete all the rows from the table
    # No need to keep any records that may be in there during a major upgrade
    op.execute("DELETE FROM dag_priority_parsing_request")

    with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bundle_name", sa.String(length=250), nullable=False))
        batch_op.add_column(sa.Column("relative_fileloc", sa.String(length=2000), nullable=False))
        batch_op.drop_column("fileloc")


def downgrade():
    # It's just easier to delete all the rows from the table
    # No need to keep any records that may be in there during a major upgrade
    op.execute("DELETE FROM dag_priority_parsing_request")

    with op.batch_alter_table("dag_priority_parsing_request", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("fileloc", sa.VARCHAR(length=2000), autoincrement=False, nullable=False)
        )
        batch_op.drop_column("relative_fileloc")
        batch_op.drop_column("bundle_name")
