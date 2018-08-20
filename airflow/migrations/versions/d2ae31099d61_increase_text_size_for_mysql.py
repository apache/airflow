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

"""Increase text size for MySQL (not relevant for other DBs' text types)

Revision ID: d2ae31099d61
Revises: 947454bf1dff
Create Date: 2017-08-18 17:07:16.686130

"""
from alembic import op
from sqlalchemy.dialects import mysql
from alembic import context

# revision identifiers, used by Alembic.
revision = 'd2ae31099d61'
down_revision = '947454bf1dff'
branch_labels = None
depends_on = None


def upgrade():
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='variable', column_name='val', type_=mysql.MEDIUMTEXT)


def downgrade():
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='variable', column_name='val', type_=mysql.TEXT)
