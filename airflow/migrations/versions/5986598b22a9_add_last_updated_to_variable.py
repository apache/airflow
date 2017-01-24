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

"""Add Last Updated to Variable

Revision ID: 5986598b22a9
Revises: 5e7d17757c7a
Create Date: 2017-01-17 23:22:10.142592

"""

# revision identifiers, used by Alembic.
revision = '5986598b22a9'
down_revision = '5e7d17757c7a'
branch_labels = None
depends_on = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    op.add_column('variable', sa.Column('last_updated',
                                        sa.DateTime(),
                                        default=sa.func.now(),
                                        onupdate=sa.func.now()))


def downgrade():
    op.drop_column('variable', 'last_updated')
