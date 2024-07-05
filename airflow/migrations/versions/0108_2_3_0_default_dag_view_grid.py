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
Update dag.default_view to grid.

Revision ID: b1b348e02d07
Revises: 75d5ed6c2b43
Create Date: 2022-04-19 17:25:00.872220

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import String
from sqlalchemy.sql import column, table

# revision identifiers, used by Alembic.
revision = "b1b348e02d07"
down_revision = "75d5ed6c2b43"
branch_labels = None
depends_on = "75d5ed6c2b43"
airflow_version = "2.3.0"


dag = table("dag", column("default_view", String))


def upgrade():
    op.execute(
        dag.update()
        .where(dag.c.default_view == op.inline_literal("tree"))
        .values({"default_view": op.inline_literal("grid")})
    )


def downgrade():
    op.execute(
        dag.update()
        .where(dag.c.default_view == op.inline_literal("grid"))
        .values({"default_view": op.inline_literal("tree")})
    )
