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
Add try_id to TI and TIH.

Revision ID: 7645189f3479
Revises: e00344393f31
Create Date: 2025-02-24 18:18:12.063106

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy_utils import UUIDType

# revision identifiers, used by Alembic.
revision = "7645189f3479"
down_revision = "e00344393f31"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add try_id to TI and TIH."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("try_id", UUIDType(binary=False), nullable=True))
        batch_op.create_unique_constraint(batch_op.f("task_instance_try_id_uq"), ["try_id"])

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "task_instance_id",
                sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("try_id", UUIDType(binary=False), nullable=False))
        batch_op.create_unique_constraint(batch_op.f("task_instance_history_try_id_uq"), ["try_id"])


def downgrade():
    """Unapply Add try_id to TI and TIH."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_history_try_id_uq"), type_="unique")
        batch_op.drop_column("try_id")
        batch_op.drop_column("task_instance_id")

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_try_id_uq"), type_="unique")
        batch_op.drop_column("try_id")
