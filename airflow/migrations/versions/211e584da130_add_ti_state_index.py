# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""add TI state index

Revision ID: 211e584da130
Revises: 2e82aab8ef20
Create Date: 2016-06-30 10:54:24.323588

"""

# revision identifiers, used by Alembic.
revision = '211e584da130'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_state', 'task_instance', ['state'], unique=False)


def downgrade():
    op.drop_index('ti_state', table_name='task_instance')
