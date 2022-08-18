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

"""Add index for ``event`` column in ``log`` table.

Revision ID: 1de7bc13c950
Revises: b1b348e02d07
Create Date: 2022-05-10 18:18:43.484829

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '1de7bc13c950'
down_revision = 'b1b348e02d07'
branch_labels = None
depends_on = None
airflow_version = '2.3.1'


def upgrade():
    """Apply Add index for ``event`` column in ``log`` table."""
    op.create_index('idx_log_event', 'log', ['event'], unique=False)


def downgrade():
    """Unapply Add index for ``event`` column in ``log`` table."""
    op.drop_index('idx_log_event', table_name='log')
