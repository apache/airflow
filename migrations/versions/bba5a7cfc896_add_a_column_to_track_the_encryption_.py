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

"""Add a column to track the encryption state of the 'Extra' field in connection

Revision ID: bba5a7cfc896
Revises: bbc73705a13e
Create Date: 2016-01-29 15:10:32.656425

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'bba5a7cfc896'
down_revision = 'bbc73705a13e'
branch_labels = None
depends_on = None


def upgrade():   # noqa: D103
    op.add_column('connection',
                  sa.Column('is_extra_encrypted', sa.Boolean, default=False))


def downgrade():   # noqa: D103
    op.drop_column('connection', 'is_extra_encrypted')
