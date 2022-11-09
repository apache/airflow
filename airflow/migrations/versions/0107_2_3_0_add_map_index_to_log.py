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
"""Add map_index to Log.

Revision ID: 75d5ed6c2b43
Revises: 909884dea523
Create Date: 2022-03-15 16:35:54.816863
"""
from __future__ import annotations

from alembic import op
from sqlalchemy import Column, Integer

# Revision identifiers, used by Alembic.
revision = "75d5ed6c2b43"
down_revision = "909884dea523"
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def upgrade():
    """Add map_index to Log."""
    op.add_column("log", Column("map_index", Integer))


def downgrade():
    """Remove map_index from Log."""
    with op.batch_alter_table("log") as batch_op:
        batch_op.drop_column("map_index")
