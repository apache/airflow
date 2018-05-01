# flake8: noqa
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

"""add kubernetes scheduler uniqueness

Revision ID: 86770d1215c0
Revises: 27c6a30d7c24
Create Date: 2018-04-03 15:31:20.814328

"""

# revision identifiers, used by Alembic.
revision = '86770d1215c0'
down_revision = '27c6a30d7c24'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


RESOURCE_TABLE = "kube_worker_uuid"


def upgrade():
    table = op.create_table(
        RESOURCE_TABLE,
        sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
        sa.Column("worker_uuid", sa.String(255)),
        sa.CheckConstraint("one_row_id", name="kube_worker_one_row_id")
    )
    op.bulk_insert(table, [
        {"worker_uuid": ""}
    ])


def downgrade():
    op.drop_table(RESOURCE_TABLE)
