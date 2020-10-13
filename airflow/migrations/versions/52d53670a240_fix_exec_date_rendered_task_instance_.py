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

"""fix_exec_date_rendered_task_instance_fields_for_MSSQL

Revision ID: 52d53670a240
Revises: 98271e7606e2
Create Date: 2020-10-13 15:13:24.911486

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '52d53670a240'
down_revision = '98271e7606e2'
branch_labels = None
depends_on = None


TABLE_NAME = 'rendered_task_instance_fields'


def upgrade():
    """
        Change datetime to datetime2(6) when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        result = conn.execute(
            """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion""").fetchone()
        mssql_version = result[0]
        if mssql_version in ("2000", "2005"):
            return

        with op.batch_alter_table(TABLE_NAME) as rendered_task_instance_fields:
            rendered_task_instance_fields.drop_constraint('rendered_task_instance_fields_pkey', type_='primary')
            rendered_task_instance_fields.alter_column(column_name="execution_date",
                                                       type_=mssql.DATETIME2(precision=6), nullable=False, )


def downgrade():
    """Drop RenderedTaskInstanceFields table"""
    op.drop_table(TABLE_NAME)  # pylint: disable=no-member


def drop_constraint(operator, constraint_dict):
    """
    Drop a primary key or unique constraint
    :param operator: batch_alter_table for the table
    :param constraint_dict: a dictionary of ((constraint name, constraint type), column name) of table
    """
    for constraint, columns in constraint_dict.items():
        if 'execution_date' in columns:
            if constraint[1].lower().startswith("primary"):
                operator.drop_constraint(
                    constraint[0],
                    type_='primary'
                )
            elif constraint[1].lower().startswith("unique"):
                operator.drop_constraint(
                    constraint[0],
                    type_='unique'
                )

