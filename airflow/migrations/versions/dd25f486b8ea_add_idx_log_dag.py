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
"""add idx_log_dag

Revision ID: dd25f486b8ea
Revises: 9635ae0956e7
Create Date: 2018-08-07 06:41:41.028249

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'dd25f486b8ea'
down_revision = '9635ae0956e7'
branch_labels = None
depends_on = None


def upgrade():   # noqa: D103
    op.create_index('idx_log_dag', 'log', ['dag_id'], unique=False)


def downgrade():   # noqa: D103
    op.drop_index('idx_log_dag', table_name='log')
