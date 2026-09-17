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
Allocate attempt numbers for pending retries and cleared tasks.

Revision ID: a61f0c9d2b47
Revises: c9f4b3e7a218
Create Date: 2026-09-15 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a61f0c9d2b47"
down_revision = "c9f4b3e7a218"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_TASK_INSTANCE = sa.table("task_instance", sa.column("state", sa.String), sa.column("try_number", sa.Integer))


def upgrade():
    """Allocate the attempt numbers previously assigned when these tasks were scheduled."""
    op.execute(
        _TASK_INSTANCE.update()
        .where(
            sa.or_(
                _TASK_INSTANCE.c.state == "up_for_retry",
                sa.and_(_TASK_INSTANCE.c.state.is_(None), _TASK_INSTANCE.c.try_number > 0),
            )
        )
        .values(try_number=_TASK_INSTANCE.c.try_number + 1)
    )


def downgrade():
    """Leave pending attempt allocation to the old scheduler."""
    op.execute(
        _TASK_INSTANCE.update()
        .where(
            sa.or_(_TASK_INSTANCE.c.state == "up_for_retry", _TASK_INSTANCE.c.state.is_(None)),
            _TASK_INSTANCE.c.try_number > 0,
        )
        .values(try_number=_TASK_INSTANCE.c.try_number - 1)
    )
