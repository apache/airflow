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

"""
Drop ``execution_date`` unique constraint on DagRun.

The column has also been renamed to logical_date, although the Python model is
not changed. This allows us to not need to fix all the Python code at once, but
still do the two changes in one migration instead of two.

Revision ID: 1cdc775ca98f
Revises: a2c32e6c7729
Create Date: 2024-08-28 08:35:26.634475

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1cdc775ca98f"
down_revision = "a2c32e6c7729"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("execution_date", new_column_name="logical_date", existing_type=sa.TIMESTAMP)
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint("dag_run_dag_id_execution_date_key", type_="unique")


def downgrade():
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("logical_date", new_column_name="execution_date", existing_type=sa.TIMESTAMP)
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_unique_constraint(
            "dag_run_dag_id_execution_date_key",
            columns=["dag_id", "execution_date"],
        )
