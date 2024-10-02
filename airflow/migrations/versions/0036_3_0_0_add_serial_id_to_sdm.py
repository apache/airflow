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
Add serial ID to SDM.

Revision ID: e1ff90d3efe9
Revises: 0d9e73a75ee4
Create Date: 2024-09-27 09:32:46.514067

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import naming_convention

# revision identifiers, used by Alembic.
revision = "e1ff90d3efe9"
down_revision = "0d9e73a75ee4"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add serial pkey to SerializedDag."""
    with op.batch_alter_table(
        "serialized_dag", recreate="always", naming_convention=naming_convention
    ) as batch_op:
        batch_op.drop_constraint("serialized_dag_pkey", type_="primary")
        # hack. The primary_key here sets autoincrement
        batch_op.add_column(sa.Column("id", sa.Integer(), primary_key=True), insert_before="dag_id")
        batch_op.create_primary_key("serialized_dag_pkey", ["id"])
        batch_op.add_column(
            sa.Column("version_number", sa.Integer(), nullable=False, default=1), insert_before="dag_id"
        )
        batch_op.create_unique_constraint(
            batch_op.f("dag_hash_version_number_unique"), ["dag_hash", "version_number"]
        )


def downgrade():
    """Unapply add serial pkey to SerializedDag."""
    with op.batch_alter_table("serialized_dag", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_hash_version_number_unique"), type_="unique")
        batch_op.drop_column("id")
        batch_op.create_primary_key("serialized_dag_pkey", ["dag_id"])
        batch_op.drop_column("version_number")
