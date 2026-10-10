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
Add task instance identity to Edge jobs.

Revision ID: f2a4b6c8d0e1
Revises: c6b3c3d093fd
Create Date: 2026-09-29 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "f2a4b6c8d0e1"
down_revision = "c6b3c3d093fd"
branch_labels = None
depends_on = None
edge3_version = "5.0.0"


_COORDINATES = ["dag_id", "task_id", "run_id", "map_index", "try_number"]
_PK_NAMING = {"pk": "%(table_name)s_pkey"}


def upgrade() -> None:
    identity_type = sa.String(36).with_variant(
        mysql.VARCHAR(36, charset="ascii", collation="ascii_bin"), "mysql"
    )
    with op.batch_alter_table("edge_job", naming_convention=_PK_NAMING) as batch_op:
        batch_op.add_column(sa.Column("task_instance_id", identity_type, nullable=False, server_default=""))
        batch_op.drop_constraint("edge_job_pkey", type_="primary")
        batch_op.create_primary_key("edge_job_pkey", [*_COORDINATES, "task_instance_id"])


def downgrade() -> None:
    if op.get_context().as_sql:
        raise RuntimeError(
            "Edge job identity downgrade requires an online check for duplicate task coordinates"
        )
    jobs = sa.table("edge_job", *(sa.column(name) for name in _COORDINATES))
    duplicate = (
        op.get_bind()
        .execute(sa.select(*jobs.c).group_by(*jobs.c).having(sa.func.count() > 1).limit(1))
        .first()
    )
    if duplicate is not None:
        raise RuntimeError(
            "Cannot downgrade Edge jobs: multiple task instances share the same coordinates. "
            "Remove duplicate attempt jobs before downgrading."
        )
    with op.batch_alter_table("edge_job", naming_convention=_PK_NAMING) as batch_op:
        batch_op.drop_constraint("edge_job_pkey", type_="primary")
        batch_op.create_primary_key("edge_job_pkey", _COORDINATES)
        batch_op.drop_column("task_instance_id")
