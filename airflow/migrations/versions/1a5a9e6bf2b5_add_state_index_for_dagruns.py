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

"""Add state index for dagruns to allow the quick lookup of active dagruns

Revision ID: 1a5a9e6bf2b5
Revises: 5e7d17757c7a
Create Date: 2017-01-17 10:22:53.193711

"""

# revision identifiers, used by Alembic.
revision = '1a5a9e6bf2b5'
down_revision = '5e7d17757c7a'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('dr_state', 'dag_run', ['state'], unique=False)


def downgrade():
    op.drop_index('state', table_name='dag_run')
