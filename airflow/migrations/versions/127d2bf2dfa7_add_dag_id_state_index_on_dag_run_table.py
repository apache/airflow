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

"""Add dag_id/state index on dag_run table

Revision ID: 127d2bf2dfa7
Revises: 1a5a9e6bf2b5
Create Date: 2017-01-25 11:43:51.635667

"""

# revision identifiers, used by Alembic.
revision = '127d2bf2dfa7'
down_revision = '1a5a9e6bf2b5'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa

def upgrade():
    op.create_index('dag_id_state', 'dag_run', ['dag_id', 'state'], unique=False)


def downgrade():
    op.drop_index('dag_id_state', table_name='dag_run')

