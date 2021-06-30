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

"""index-faskfail

Revision ID: 9635ae0956e7
Revises: 856955da8476
Create Date: 2018-06-17 21:40:01.963540

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9635ae0956e7'
down_revision = '856955da8476'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        'idx_task_fail_dag_task_date', 'task_fail', ['dag_id', 'task_id', 'execution_date'], unique=False
    )


def downgrade():
    op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')
