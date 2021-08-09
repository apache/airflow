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

"""remove curve template args from variable

Revision ID: 7e41719a474d
Revises: 30f6f5122970
Create Date: 2021-08-09 10:52:57.372307

"""

# revision identifiers, used by Alembic.
revision = '7e41719a474d'
down_revision = '30f6f5122970'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_column('variable', 'is_curve_template')
    op.drop_column('variable', 'active')


def downgrade():
    op.add_column('variable', sa.Column('is_curve_template', sa.Boolean(), default=True))
    op.add_column('variable', sa.Column('active', sa.Boolean(), default=True))
