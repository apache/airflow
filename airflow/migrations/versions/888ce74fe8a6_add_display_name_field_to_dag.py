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

"""add display_name field to dag

Revision ID: 888ce74fe8a6
Revises: c97c2ab6aa23
Create Date: 2022-03-09 16:11:26.604661

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '888ce74fe8a6'
down_revision = 'c97c2ab6aa23'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def upgrade():
    op.add_column('dag', sa.Column('display_name', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('dag', 'display_name')
