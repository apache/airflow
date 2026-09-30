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
Add last_probed_at index to lang_sdk_task_handler_artifact.

Revision ID: 37d645374a9c
Revises: f7ed13533d23
Create Date: 2026-09-30 19:00:42.253263

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "37d645374a9c"
down_revision = "f7ed13533d23"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add last_probed_at index to lang_sdk_task_handler_artifact."""
    with op.batch_alter_table("lang_sdk_task_handler_artifact", schema=None) as batch_op:
        batch_op.create_index(
            "idx_lang_sdk_task_handler_artifact_last_probed_at", ["last_probed_at"], unique=False
        )


def downgrade():
    """Remove last_probed_at index from lang_sdk_task_handler_artifact."""
    with op.batch_alter_table("lang_sdk_task_handler_artifact", schema=None) as batch_op:
        batch_op.drop_index("idx_lang_sdk_task_handler_artifact_last_probed_at")
