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

"""add car_code to task_instance

Revision ID: b09b88512259
Revises: cddd4a00802b
Create Date: 2020-08-07 16:15:20.590866

"""

# revision identifiers, used by Alembic.
revision = 'b09b88512259'
down_revision = 'cddd4a00802b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade():
    op.add_column('task_instance', sa.Column('car_code', sa.String(1000)))


def downgrade():
    op.add_column('task_instance', sa.Column('car_code', sa.String(1000)))
