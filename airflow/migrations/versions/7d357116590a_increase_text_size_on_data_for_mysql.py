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

"""Increase text size on data column for MySQL

Revision ID: 7d357116590a
Revises: a4c2fd67d16b
Create Date: 2020-02-13 19:06:54.302785

"""

from alembic import context, op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '7d357116590a'
down_revision = 'a4c2fd67d16b'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Increase text size on data column for MySQL"""
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='serialized_dag', column_name='data', type_=mysql.MEDIUMTEXT)


def downgrade():
    """Unapply Increase text size on data column for MySQL"""
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='serialized_dag', column_name='data', type_=mysql.TEXT)
