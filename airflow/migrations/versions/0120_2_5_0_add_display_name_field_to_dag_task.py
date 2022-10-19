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

"""Add display_name field to dag/task

Revision ID: 9f20933f6e31
Revises: ee8d93fcc81e
Create Date: 2022-10-19 13:20:02.405370

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '9f20933f6e31'
down_revision = 'ee8d93fcc81e'
branch_labels = None
depends_on = None
airflow_version = '2.5.0'


def upgrade():
    """Apply Add display_name field to dag/task"""
    op.add_column('dag', sa.Column('display_name', sa.Text(), nullable=True))


def downgrade():
    """Unapply Add display_name field to dag/task"""
    op.drop_column('dag', 'display_name')
