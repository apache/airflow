#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""add pid field to TaskInstance

Revision ID: 5e7d17757c7a
Revises: 8504051e801b
Create Date: 2016-12-07 15:51:37.119478

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '5e7d17757c7a'
down_revision = '8504051e801b'
branch_labels = None
depends_on = None


def upgrade():
    """Add pid column to task_instance table."""
    op.add_column('task_instance', sa.Column('pid', sa.Integer))


def downgrade():
    """Drop pid column from task_instance table."""
    op.drop_column('task_instance', 'pid')
