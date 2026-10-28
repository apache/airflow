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
Extend dag_warning for parse-time failures (file scope, no dag FK).

Revision ID: b8f2c1d04e91
Revises: 38c8df3fcb9c
Create Date: 2026-10-26 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID

revision = "b8f2c1d04e91"
down_revision = "38c8df3fcb9c"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    with op.batch_alter_table("dag_warning") as batch_op:
        batch_op.drop_constraint("dcw_dag_id_fkey", type_="foreignkey")
        batch_op.add_column(sa.Column("bundle_name", StringID(), nullable=True))
        batch_op.add_column(sa.Column("filename", sa.String(length=1024), nullable=True))


def downgrade():
    op.execute("DELETE FROM dag_warning WHERE dag_id NOT IN (SELECT dag_id FROM dag)")
    with op.batch_alter_table("dag_warning") as batch_op:
        batch_op.drop_column("filename")
        batch_op.drop_column("bundle_name")
        batch_op.create_foreign_key("dcw_dag_id_fkey", "dag", ["dag_id"], ["dag_id"], ondelete="CASCADE")
