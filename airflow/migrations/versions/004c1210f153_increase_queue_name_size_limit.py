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

"""increase queue name size limit

Revision ID: 004c1210f153
Revises: 939bb1e647c8
Create Date: 2019-06-07 07:46:04.262275

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '004c1210f153'
down_revision = '939bb1e647c8'
branch_labels = None
depends_on = None


def upgrade():
    """
    Increase column size from 50 to 256 characters, closing AIRFLOW-4737 caused
    by broker backends that might use unusually large queue names.
    """
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.alter_column('queue', type_=sa.String(256))


def downgrade():
    """Revert column size from 256 to 50 characters, might result in data loss."""
    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.alter_column('queue', type_=sa.String(50))
