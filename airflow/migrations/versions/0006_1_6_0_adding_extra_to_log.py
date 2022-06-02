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

"""Adding ``extra`` column to ``Log`` table

Revision ID: 502898887f84
Revises: 52d714495f0
Create Date: 2015-11-03 22:50:49.794097

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '502898887f84'
down_revision = '52d714495f0'
branch_labels = None
depends_on = None
airflow_version = '1.6.0'


def upgrade():
    op.add_column('log', sa.Column('extra', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('log', 'extra')
