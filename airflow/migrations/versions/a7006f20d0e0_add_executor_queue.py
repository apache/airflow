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

"""add executor queue

Revision ID: a7006f20d0e0
Revises: 947454bf1dff
Create Date: 2017-08-22 10:54:07.425417

"""

# revision identifiers, used by Alembic.
revision = 'a7006f20d0e0'
down_revision = '947454bf1dff'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('executor_queue',
                    sa.Column('key', sa.String(length=250), nullable=False),
                    sa.Column('id', sa.String(length=250), nullable=False),
                    sa.Column('updated', sa.DateTime(), nullable=True),
                    sa.PrimaryKeyConstraint('key')
                    )


def downgrade():
    op.drop_table('executor_queue')
