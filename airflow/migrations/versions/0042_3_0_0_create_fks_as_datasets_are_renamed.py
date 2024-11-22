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
create foreign key constraints for assets.

Revision ID: c4a1639f0f67
Revises: 05234396c6fc
Create Date: 2024-11-22 09:49:41.813016

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "c4a1639f0f67"
down_revision = "05234396c6fc"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply create foreign key constraints for assets."""
    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        batch_op.create_foreign_key("adrq_asset_fkey", "asset", ["asset_id"], ["id"], ondelete="CASCADE")

    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.create_foreign_key("toar_asset_fkey", "asset", ["asset_id"], ["id"], ondelete="CASCADE")


def downgrade():
    """Unapply create foreign key constraints for assets."""
    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.drop_constraint("toar_asset_fkey", type_="foreignkey")

    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        batch_op.drop_constraint("adrq_asset_fkey", type_="foreignkey")
