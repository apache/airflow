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

"""add KMS fields to connection

Revision ID: d7a2586b258a
Revises: 9635ae0956e7
Create Date: 2018-07-19 09:13:46.044044

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd7a2586b258a'
down_revision = '9635ae0956e7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('connection', sa.Column('conn_key', sa.String(length=200),
                                          nullable=True))
    op.add_column('connection', sa.Column('_kms_conn_id', sa.String(length=250),
                                          nullable=True))
    op.add_column('connection', sa.Column('_kms_extra', sa.String(length=5000),
                                          nullable=True))


def downgrade():
    op.drop_column('connection', 'conn_key')
    op.drop_column('connection', '_kms_conn_id')
    op.drop_column('connection', '_kms_extra')
