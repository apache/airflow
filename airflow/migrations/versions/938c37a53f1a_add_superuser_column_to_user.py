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

"""add superuser column to user

Revision ID: 938c37a53f1a
Revises: f2ca10b85618
Create Date: 2016-11-07 11:19:10.388577

"""

# revision identifiers, used by Alembic.
revision = '938c37a53f1a'
down_revision = 'f2ca10b85618'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('users', sa.Column('superuser', sa.Boolean,
                                     nullable=False, default=False))


def downgrade():
    op.drop_column('users', 'superuser')
