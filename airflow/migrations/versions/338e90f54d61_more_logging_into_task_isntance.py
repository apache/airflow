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

"""More logging into task_instance

Revision ID: 338e90f54d61
Revises: 13eb55f81627
Create Date: 2015-08-25 06:09:20.460147

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '338e90f54d61'
down_revision = '13eb55f81627'
branch_labels = None
depends_on = None


def upgrade():   # noqa: D103
    op.add_column('task_instance', sa.Column('operator', sa.String(length=1000), nullable=True))
    op.add_column('task_instance', sa.Column('queued_dttm', sa.DateTime(), nullable=True))


def downgrade():   # noqa: D103
    op.drop_column('task_instance', 'queued_dttm')
    op.drop_column('task_instance', 'operator')
