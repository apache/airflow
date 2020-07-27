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

"""add tighetning_controller table

Revision ID: 648a51dcdaae
Revises: 710064c03cba
Create Date: 2020-07-27 10:30:59.517118

"""

# revision identifiers, used by Alembic.
revision = '648a51dcdaae'
down_revision = '710064c03cba'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'tightening_controller',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('controller_name', sa.String(length=1000), nullable=False, unique=True),
        sa.Column('work_center_code', sa.String(length=1000), nullable=False),
        sa.Column('line_code', sa.String(length=1000), nullable=False),
        sa.Column('work_center_name', sa.String(length=1000), nullable=True),
    )


def downgrade():
    op.drop_table('tightening_controller')
