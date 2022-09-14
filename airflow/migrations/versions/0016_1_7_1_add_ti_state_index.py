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
"""Add TI state index

Revision ID: 211e584da130
Revises: 2e82aab8ef20
Create Date: 2016-06-30 10:54:24.323588

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = '211e584da130'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None
airflow_version = '1.7.1.3'


def upgrade():
    op.create_index('ti_state', 'task_instance', ['state'], unique=False)


def downgrade():
    op.drop_index('ti_state', table_name='task_instance')
