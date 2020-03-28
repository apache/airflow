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

"""add pkg_name to task instance

Revision ID: 19d2506c1da1
Revises: 3f257cf25d77
Create Date: 2020-03-28 19:43:47.731411

"""

# revision identifiers, used by Alembic.
revision = '19d2506c1da1'
down_revision = '3f257cf25d77'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('pkg_name', sa.String(128)))


def downgrade():
    op.drop_column('task_instance', 'pkg_name')
