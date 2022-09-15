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
"""add callback request table

Revision ID: c97c2ab6aa23
Revises: c306b5b5ae4a
Create Date: 2022-01-28 21:11:11.857010

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import func

from airflow.migrations.db_types import TIMESTAMP
from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = 'c97c2ab6aa23'
down_revision = 'c306b5b5ae4a'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'

TABLE_NAME = 'callback_request'


def upgrade():
    op.create_table(
        TABLE_NAME,
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('created_at', TIMESTAMP, default=func.now, nullable=False),
        sa.Column('priority_weight', sa.Integer(), default=1, nullable=False),
        sa.Column('callback_data', ExtendedJSON, nullable=False),
        sa.Column('callback_type', sa.String(20), nullable=False),
        sa.Column('dag_directory', sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table(TABLE_NAME)
