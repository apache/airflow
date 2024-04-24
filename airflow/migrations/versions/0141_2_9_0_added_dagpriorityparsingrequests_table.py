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

"""Added DagPriorityParsingRequests table

Revision ID: c4602ba06b4b
Revises: 88344c1d9134
Create Date: 2024-04-17 17:12:05.473889

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = 'c4602ba06b4b'
down_revision = '1949afb29106'
branch_labels = None
depends_on = None
airflow_version = '2.9.0'


def upgrade():
    """Apply Added DagPriorityParsingRequests table"""
    op.create_table('dag_priority_parsing_requests',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('fileloc', sa.String(length=2000), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('dag_priority_parsing_requests_pkey'))
    )


def downgrade():
    """Unapply Added DagPriorityParsingRequests table"""
    op.drop_table('dag_priority_parsing_requests')
