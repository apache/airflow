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

"""add line_name into tightening_controller table

Revision ID: cddd4a00802b
Revises: 648a51dcdaae
Create Date: 2020-07-31 09:39:32.447103

"""

# revision identifiers, used by Alembic.
revision = 'cddd4a00802b'
down_revision = '648a51dcdaae'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('tightening_controller', sa.Column('line_name', sa.String(1000), nullable=True))


def downgrade():
    op.drop_column('tightening_controller', 'line_name')
