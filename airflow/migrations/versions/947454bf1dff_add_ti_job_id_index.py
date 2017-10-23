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

Revision ID: 947454bf1dff
Revises: bdaa763e6c56
Create Date: 2017-08-15 15:12:13.845074

"""

# revision identifiers, used by Alembic.
revision = '947454bf1dff'
down_revision = 'bdaa763e6c56'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_job_id', 'task_instance', ['job_id'], unique=False)


def downgrade():
    op.drop_index('ti_job_id', table_name='task_instance')
