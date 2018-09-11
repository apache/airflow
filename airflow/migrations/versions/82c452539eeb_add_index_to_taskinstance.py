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

"""add index to taskinstance

Revision ID: 82c452539eeb
Revises: 9635ae0956e7
Create Date: 2018-09-10 17:04:32.058103

"""

# revision identifiers, used by Alembic.
revision = '82c452539eeb'
down_revision = '9635ae0956e7'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_dag_date', 'task_instance', ['dag_id', 'execution_date'], unique=False)


def downgrade():
    op.drop_index('ti_dag_date', table_name='task_instance')
