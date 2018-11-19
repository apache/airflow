# flake8: noqa
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

"""fix mysql not null constraint

Revision ID: f23433877c24
Revises: 05f30312d566
Create Date: 2018-06-17 10:16:31.412131

"""
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'f23433877c24'
down_revision = '05f30312d566'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        conn.execute("SET time_zone = '+00:00'")
        op.alter_column('task_fail', 'execution_date', existing_type=mysql.TIMESTAMP(fsp=6), nullable=False)
        op.alter_column('xcom', 'execution_date', existing_type=mysql.TIMESTAMP(fsp=6), nullable=False)
        op.alter_column('xcom', 'timestamp', existing_type=mysql.TIMESTAMP(fsp=6), nullable=False)


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        conn.execute("SET time_zone = '+00:00'")
        op.alter_column('xcom', 'timestamp', existing_type=mysql.TIMESTAMP(fsp=6), nullable=True)
        op.alter_column('xcom', 'execution_date', existing_type=mysql.TIMESTAMP(fsp=6), nullable=True)
        op.alter_column('task_fail', 'execution_date', existing_type=mysql.TIMESTAMP(fsp=6), nullable=True)
