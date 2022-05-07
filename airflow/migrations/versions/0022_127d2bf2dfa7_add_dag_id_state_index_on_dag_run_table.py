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

"""Add ``dag_id``/``state`` index on ``dag_run`` table

Revision ID: 127d2bf2dfa7
Revises: 5e7d17757c7a
Create Date: 2017-01-25 11:43:51.635667

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '127d2bf2dfa7'
down_revision = '5e7d17757c7a'
branch_labels = None
depends_on = None
airflow_version = '1.7.1.3'


def upgrade():
    op.create_index('dag_id_state', 'dag_run', ['dag_id', 'state'], unique=False)


def downgrade():
    op.drop_index('dag_id_state', table_name='dag_run')
