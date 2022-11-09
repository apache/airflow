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
"""add data_compressed to serialized_dag

Revision ID: a3bcd0914482
Revises: e655c0453f75
Create Date: 2022-02-03 22:40:59.841119

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a3bcd0914482'
down_revision = 'e655c0453f75'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def upgrade():
    with op.batch_alter_table('serialized_dag') as batch_op:
        batch_op.alter_column('data', existing_type=sa.JSON, nullable=True)
        batch_op.add_column(sa.Column('data_compressed', sa.LargeBinary, nullable=True))


def downgrade():
    with op.batch_alter_table('serialized_dag') as batch_op:
        batch_op.alter_column('data', existing_type=sa.JSON, nullable=False)
        batch_op.drop_column('data_compressed')
