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

"""adding is_pinned column to connections

Revision ID: f2386f13728c
Revises: c306b5b5ae4a
Create Date: 2022-02-23 00:51:54.124717

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'f2386f13728c'
down_revision = 'c306b5b5ae4a'
branch_labels = None
depends_on = None


def upgrade():
    """Apply adding is_pinned column to connections"""
    op.add_column('connection', sa.Column('is_pinned', sa.Boolean, default=False))


def downgrade():
    op.drop_column('connection', 'is_pinned')

