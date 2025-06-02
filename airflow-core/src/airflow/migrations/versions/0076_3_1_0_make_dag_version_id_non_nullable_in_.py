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
Make dag_version_id non-nullable in TaskInstance.

Revision ID: 5d3072c51bac
Revises: ffdb0566c7c0
Create Date: 2025-05-20 10:38:25.635779

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

# revision identifiers, used by Alembic.
revision = "5d3072c51bac"
down_revision = "ffdb0566c7c0"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply make dag_version_id non-nullable in TaskInstance."""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        update_query = sa.text("""
            UPDATE task_instance
            SET dag_version_id = latest_versions.id
            FROM (
                SELECT DISTINCT ON (dag_id) dag_id, id
                FROM dag_version
                ORDER BY dag_id, created_at DESC
            ) latest_versions
            WHERE task_instance.dag_id = latest_versions.dag_id
            AND task_instance.dag_version_id IS NULL
        """)
    else:
        update_query = sa.text("""
            UPDATE task_instance
            SET dag_version_id = (
                SELECT id FROM (
                    SELECT id, dag_id,
                    ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY created_at DESC) as rn
                    FROM dag_version
                ) ranked_versions
                WHERE ranked_versions.dag_id = task_instance.dag_id
                AND ranked_versions.rn = 1
            )
            WHERE task_instance.dag_version_id IS NULL
        """)

    op.execute(update_query)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("dag_version_id", existing_type=UUIDType(binary=False), nullable=False)


def downgrade():
    """Unapply make dag_version_id non-nullable in TaskInstance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("dag_version_id", existing_type=UUIDType(binary=False), nullable=True)
