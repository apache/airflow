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

"""add bolt_no & craft_type to task_instance

Revision ID: 710064c03cba
Revises: 78505e57c49e
Create Date: 2020-07-13 14:20:54.563503

"""

# revision identifiers, used by Alembic.
revision = '710064c03cba'
down_revision = '78505e57c49e'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('bolt_number', sa.String(1000)))
    op.add_column('task_instance', sa.Column('craft_type', sa.Integer()))


def downgrade():
    op.drop_column('task_instance', 'bolt_number')
    op.drop_column('task_instance', 'craft_type')
