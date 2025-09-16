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
Add stored procedures for task selection.

Revision ID: 28abf6d28d2f
Revises: 2f49f2dae90c
Create Date: 2025-09-12 23:20:39.502229

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from airflow.migrations.selector_procs import (
    mysql_ti_selector_proc,
    mysql_ti_selector_proc_drop,
    pgsql_hstore_extension,
    pgsql_hstore_extension_drop,
    pgsql_ti_selector_proc,
    pgsql_ti_selector_proc_drop,
)

# revision identifiers, used by Alembic.
revision = "28abf6d28d2f"
down_revision = "2f49f2dae90c"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def _mysql_upgrade():
    conn = op.get_bind()
    conn.execute(text(mysql_ti_selector_proc_drop))
    conn.execute(text(mysql_ti_selector_proc))


def _mysql_downgrade():
    conn = op.get_bind()
    conn.execute(text(mysql_ti_selector_proc_drop))


def _pgsql_upgrade():
    conn = op.get_bind()
    conn.execute(text(pgsql_hstore_extension))
    conn.execute(text(pgsql_ti_selector_proc_drop))
    conn.execute(text(pgsql_ti_selector_proc))


def _pgsql_downgrade():
    conn = op.get_bind()
    conn.execute(text(pgsql_ti_selector_proc_drop))
    conn.execute(text(pgsql_hstore_extension_drop))


_UPGRADE_BY_DIALECT = {
    "mysql": _mysql_upgrade,
    "postgresql": _pgsql_upgrade,
}

_DOWNGRADE_BY_DIALECT = {
    "mysql": _mysql_downgrade,
    "postgresql": _pgsql_downgrade,
}


def upgrade():
    """Apply Add stored procedures for task selection."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    upgrade_fn = _UPGRADE_BY_DIALECT.get(dialect_name)
    if upgrade_fn:
        upgrade_fn()


def downgrade():
    """Unapply Add stored procedures for task selection."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    downgrade_fn = _DOWNGRADE_BY_DIALECT.get(dialect_name)
    if downgrade_fn:
        downgrade_fn()
