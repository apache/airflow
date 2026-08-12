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
Lower case team names.

Revision ID: c7f0a5d2e9b4
Revises: 76c46545c91e
Create Date: 2026-08-12 09:14:22.518233

"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement

# revision identifiers, used by Alembic.
revision = "c7f0a5d2e9b4"
down_revision = "76c46545c91e"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

# Tables carrying a foreign key to ``team.name``.
_REFERRING_TABLES = ("dag_bundle_team", "connection", "variable", "slot_pool", "trigger")

_TEAM = sa.table("team", sa.column("name", sa.String))


def build_lower_casing_statements() -> list[ClauseElement]:
    """
    Build the statements that fold every stored team name to lower case.

    Kept free of anything read back from a row so that ``db migrate --show-sql-only`` can emit
    it: an offline migration has no rows to read.

    Their order carries both collations a database may have, without having to tell them apart:

    * Case sensitive, where ``name <> lower(name)`` selects the names to fold. ``team.name`` is
      the primary key and the foreign keys do not cascade on update, so the lower case row has to
      exist before anything can point at it, and the old one can only go once nothing does. Two
      stored names folding onto one collide on the primary key at the insert, deliberately --
      merging two teams is not a migration's decision to make, and ``db migrate`` refuses earlier
      with an explanation.
    * Case insensitive, where the two spellings are a single value: the insert and the delete
      match nothing, the referring rows keep matching their team while they are rewritten, and
      the closing unconditional update renames the row in place.
    """
    lowered = sa.func.lower(_TEAM.c.name)
    statements: list[ClauseElement] = [
        _TEAM.insert().from_select(["name"], sa.select(lowered).where(_TEAM.c.name != lowered))
    ]
    for table_name in _REFERRING_TABLES:
        referring = sa.table(table_name, sa.column("team_name", sa.String))
        # ``connection`` and ``trigger`` carry the rows of team-less deployments as well; only
        # the team scoped ones are rewritten.
        statements.append(
            referring.update()
            .where(referring.c.team_name.is_not(None))
            .values(team_name=sa.func.lower(referring.c.team_name))
        )
    statements.append(_TEAM.delete().where(_TEAM.c.name != lowered))
    statements.append(_TEAM.update().values(name=lowered))
    return statements


def upgrade():
    """Lower case team names."""
    for statement in build_lower_casing_statements():
        op.execute(statement)


def downgrade():
    """Unapply Lower case team names."""
    # The original casing is recorded nowhere, so it cannot be restored. Lower case names are
    # valid under every rule that preceded this one, so leaving them as they are is safe.
