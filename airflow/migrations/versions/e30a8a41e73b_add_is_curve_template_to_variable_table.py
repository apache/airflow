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

"""add is_curve_template to variable table

Revision ID: e30a8a41e73b
Revises: 598ae0a6cee3
Create Date: 2020-03-24 13:01:38.547807

"""

# revision identifiers, used by Alembic.
revision = 'e30a8a41e73b'
down_revision = '598ae0a6cee3'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('variable', sa.Column('is_curve_template', sa.Boolean(), default=True))


def downgrade():
    op.drop_column('variable', 'is_curve_template')
