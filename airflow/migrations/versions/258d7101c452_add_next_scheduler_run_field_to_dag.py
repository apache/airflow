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

"""[AIRFLOW-1424] add next_scheduler_run field to dag

Revision ID: 258d7101c452
Revises: cc1e65623dc7
Create Date: 2017-07-18 11:37:53.890462

"""

# revision identifiers, used by Alembic.
revision = '258d7101c452'
down_revision = 'cc1e65623dc7'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('dag', sa.Column('next_scheduler_run', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('dag', 'next_scheduler_run')
