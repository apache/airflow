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

"""add device type id to tightening controller

Revision ID: daa054bdb091
Revises: 65763ea18e7e
Create Date: 2021-06-21 23:27:37.231405

"""

# revision identifiers, used by Alembic.
revision = 'daa054bdb091'
down_revision = '65763ea18e7e'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('tightening_controller', sa.Column('device_type_id', sa.Integer))
    with op.batch_alter_table('tightening_controller') as tightening_controller_batch_op:

        tightening_controller_batch_op.create_foreign_key('tightening_controller_device_type_id_fkey', 'device_type',
                                                          ['device_type_id'], ['id'])


def downgrade():
    op.drop_column('tightening_controller', 'device_type_id')
    # with op.batch_alter_table('tightening_controller') as tightening_controller_batch_op:
    #     tightening_controller_batch_op.drop_constraint('tightening_controller_device_type_id_fkey', type_='foreignkey')

