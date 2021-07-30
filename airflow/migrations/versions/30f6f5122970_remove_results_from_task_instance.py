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

"""remove results from task_instance

Revision ID: 30f6f5122970
Revises: daa054bdb091
Create Date: 2021-07-30 16:57:28.842178

"""

# revision identifiers, used by Alembic.
revision = '30f6f5122970'
down_revision = 'daa054bdb091'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_column('task_instance', 'bolt_number')
    op.drop_column('task_instance', 'craft_type')
    op.drop_column('task_instance', 'entity_id')
    op.drop_column('task_instance', 'error_tag')
    op.drop_column('task_instance', 'measure_result')
    op.drop_column('task_instance', 'result')
    op.drop_column('task_instance', 'error_code')
    op.drop_column('task_instance', 'verify_error')
    op.drop_column('task_instance', 'final_state')
    op.drop_column('task_instance', 'line_code')
    op.drop_column('task_instance', 'factory_code')
    op.drop_column('task_instance', 'controller_name')
    op.drop_column('task_instance', 'car_code')
    op.drop_column('task_instance', 'type')
    op.drop_column('task_instance', 'should_analyze')
    op.drop_column('task_instance', 'device_type')


def downgrade():
    op.add_column('task_instance', sa.Column('bolt_number', sa.String(1000)))
    op.add_column('task_instance', sa.Column('craft_type', sa.Integer()))
    op.add_column('task_instance', sa.Column('entity_id', sa.String(128)))
    op.add_column('task_instance', sa.Column('error_tag', sa.String(1000)))
    op.add_column('task_instance', sa.Column('measure_result', sa.String(20)))
    op.add_column('task_instance', sa.Column('result', sa.String(20)))
    op.add_column('task_instance', sa.Column('error_code', sa.Integer))
    op.add_column('task_instance', sa.Column('verify_error', sa.Integer))
    op.add_column('task_instance', sa.Column('final_state', sa.String(20)))
    op.add_column('task_instance', sa.Column('line_code', sa.String(100)))
    op.add_column('task_instance', sa.Column('factory_code', sa.String(100)))
    op.add_column('task_instance', sa.Column('controller_name', sa.String(100)))
    op.add_column('task_instance', sa.Column('car_code', sa.String(1000)))
    op.add_column('task_instance', sa.Column('type', sa.String(100), server_default='normal'))
    op.add_column('task_instance', sa.Column('should_analyze', sa.Boolean(), default=True))
    op.add_column('task_instance', sa.Column('device_type', sa.String(100)))
