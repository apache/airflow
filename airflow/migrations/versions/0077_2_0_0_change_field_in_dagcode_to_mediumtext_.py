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
"""Change field in ``DagCode`` to ``MEDIUMTEXT`` for MySql

Revision ID: e959f08ac86c
Revises: 64a7d6477aae
Create Date: 2020-12-07 16:31:43.982353

"""
from __future__ import annotations

from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "e959f08ac86c"
down_revision = "64a7d6477aae"
branch_labels = None
depends_on = None
airflow_version = "2.0.0"


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        op.alter_column(
            table_name="dag_code", column_name="source_code", type_=mysql.MEDIUMTEXT, nullable=False
        )


def downgrade():
    # Do not downgrade to TEXT as it will break data
    pass
