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
Remove dag.default_view column.

Revision ID: 16f7f5ee874e
Revises: cf87489a35df
Create Date: 2025-03-11 10:39:25.449265

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "16f7f5ee874e"
down_revision = "cf87489a35df"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("default_view")


def downgrade():
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("default_view", sa.VARCHAR(length=25), nullable=True))
