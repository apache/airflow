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

"""Add SLA miss type.

Revision ID: 19e9c503bc4e
Revises: 0e2a74e0fc9f
Create Date: 2018-07-05 12:19:18.718960

"""

# revision identifiers, used by Alembic.
revision = '19e9c503bc4e'
down_revision = '0e2a74e0fc9f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


SLA_MISS_TABLE = "sla_miss"
NEW_COLUMN = "sla_type"
OLD_SLA_MISS_TYPE = "task_late_finish"


def upgrade():
    op.add_column(SLA_MISS_TABLE,
                  sa.Column(
                      NEW_COLUMN,
                      sa.String(50),
                      primary_key=True,
                      nullable=False,
                      server_default=OLD_SLA_MISS_TYPE
                  ))


def downgrade():
    op.drop_column(SLA_MISS_TABLE, NEW_COLUMN)
