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

"""task_duration

Revision ID: 2e541a1dcfed
Revises: 1b38cef5b76e
Create Date: 2015-10-28 20:38:41.266143

"""

# revision identifiers, used by Alembic.
revision = '2e541a1dcfed'
down_revision = '1b38cef5b76e'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column('duration',
                              existing_type=mysql.INTEGER(display_width=11),
                              type_=sa.Float(),
                              existing_nullable=True)


def downgrade():
    pass
