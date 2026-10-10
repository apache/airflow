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
Add index on dag_run (dag_id, run_after).

Revision ID: 38c8df3fcb9c
Revises: e7c2a91bd540
Create Date: 2026-10-08 22:19:14.965383
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "38c8df3fcb9c"
down_revision = "e7c2a91bd540"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add index on dag_run (dag_id, run_after)."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_index("idx_dag_run_dag_id_run_after", ["dag_id", "run_after"], unique=False)


def downgrade():
    """Remove index on dag_run (dag_id, run_after)."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_run_dag_id_run_after")
