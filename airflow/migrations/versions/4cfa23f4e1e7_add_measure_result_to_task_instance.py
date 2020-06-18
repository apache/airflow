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

"""add measure_result to task instance

Revision ID: 4cfa23f4e1e7
Revises: 311615751c5b
Create Date: 2020-06-18 16:33:29.714872

"""

# revision identifiers, used by Alembic.
revision = '4cfa23f4e1e7'
down_revision = '311615751c5b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('measure_result', sa.String(20)))


def downgrade():
    op.drop_column('task_instance', 'measure_result')
