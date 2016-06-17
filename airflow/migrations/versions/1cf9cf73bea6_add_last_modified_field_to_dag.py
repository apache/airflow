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
#

"""
Add last_modified field to dag

Revision ID: 1cf9cf73bea6
Revises: 2e82aab8ef20
Create Date: 2016-06-17 13:32:09.035886

"""

# revision identifiers, used by Alembic.
revision = '1cf9cf73bea6'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('dag', sa.Column('last_modified', sa.DateTime,
                                   server_default=sa.func.current_timestamp(),
                                   nullable=False))


def downgrade():
    op.drop_column('dag', 'last_modified')
