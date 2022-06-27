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

"""Add Dataset model

Revision ID: 0038cd0c28b4
Revises: 44b7034f6bdc
Create Date: 2022-06-22 14:37:20.880672

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Integer, String

from airflow.migrations.db_types import TIMESTAMP
from airflow.utils.sqlalchemy import ExtendedJSON

revision = '0038cd0c28b4'
down_revision = '44b7034f6bdc'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def upgrade():
    """Apply Add Dataset model"""
    op.create_table(
        'dataset',
        sa.Column('id', Integer, primary_key=True, autoincrement=True),
        sa.Column(
            'uri',
            String(length=3000).with_variant(
                String(
                    length=3000,
                    # latin1 allows for more indexed length in mysql
                    # and this field should only be ascii chars
                    collation='latin1_general_cs',
                ),
                'mysql',
            ),
            nullable=False,
        ),
        sa.Column('extra', ExtendedJSON, nullable=True),
        sa.Column('created_at', TIMESTAMP, nullable=False),
        sa.Column('updated_at', TIMESTAMP, nullable=False),
        sqlite_autoincrement=True,  # ensures PK values not reused
    )
    op.create_index('idx_uri_unique', 'dataset', ['uri'], unique=True)


def downgrade():
    """Unapply Add Dataset model"""
    op.drop_table('dataset')
