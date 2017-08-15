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

"""add ti job_id index

Revision ID: 7171349d4c73
Revises: cc1e65623dc7
Create Date: 2017-08-14 18:08:50.196042

"""

# revision identifiers, used by Alembic.
revision = '7171349d4c73'
down_revision = 'cc1e65623dc7'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_job_id', 'task_instance', ['job_id'], unique=False)


def downgrade():
    op.drop_index('ti_job_id', table_name='task_instance')
