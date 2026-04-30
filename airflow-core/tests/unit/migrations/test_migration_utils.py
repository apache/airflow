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
Stairway test for Airflow DB migrations.

Walks every migration step since the 3.0.0 release forward (upgrade) then
backward (downgrade) to verify that all migrations are fully reversible.
This catches problems like:
- a migration that creates a constraint the downgrade forgets to drop
- a downgrade that references a column that was never added by the upgrade

The test runs as a single (non-parametrized) function so that execution order
and DB state are guaranteed without relying on xdist grouping or cross-test
state.  Failures include the specific revision ID so the broken migration is
immediately identifiable in CI logs.
"""

from __future__ import annotations

import pytest
from alembic.script import ScriptDirectory

from airflow.utils.db import (
    _REVISION_HEADS_MAP,
    _get_alembic_config,
    downgrade,
    upgradedb,
)

pytestmark = pytest.mark.db_test

# Stairway starts from the 3.0.0 head revision.  Starting here (rather than
# the 2.6.2 squashed baseline) avoids triggering FAB-provider downgrade
# handling that airflow.utils.db.downgrade() applies when the target is below
# 2.10.3, and keeps the number of steps manageable on slower backends.
_STAIRWAY_START_REVISION = _REVISION_HEADS_MAP["3.0.0"]


def _get_revisions_in_order() -> list[str]:
    """Return revision IDs in upgrade order, starting after the stairway base."""
    config = _get_alembic_config()
    script = ScriptDirectory.from_config(config)

    # walk_revisions() yields from head → base; reverse for upgrade order.
    all_revisions = list(script.walk_revisions())
    all_revisions.reverse()

    revision_ids = [rev.revision for rev in all_revisions]

    if _STAIRWAY_START_REVISION not in revision_ids:
        raise ValueError(
            f"Stairway base revision {_STAIRWAY_START_REVISION!r} not found in migration scripts. "
            "Update _STAIRWAY_START_REVISION to a valid revision from _REVISION_HEADS_MAP."
        )

    base_index = revision_ids.index(_STAIRWAY_START_REVISION)
    return revision_ids[base_index + 1 :]


@pytest.fixture(scope="module")
def stairway_db():
    """
    Bring the DB to the stairway base revision before the test runs, and
    restore it to the current heads afterwards so other tests are unaffected.

    Uses Airflow's downgrade()/upgradedb() wrappers so that backend-specific
    handling (MySQL metadata-lock, global migration lock, single-connection
    pool) is applied consistently.
    """
    downgrade(to_revision=_STAIRWAY_START_REVISION)

    yield

    # Restore to latest heads even if the test failed mid-way.
    upgradedb()


def test_migration_stairway(stairway_db) -> None:
    """
    Walk every incremental migration step since 3.0.0: upgrade → downgrade → re-upgrade.

    Uses Airflow's upgradedb()/downgrade() wrappers for every step so that
    the global migration lock, MySQL metadata-lock handling, and
    single-connection pool are active — matching real-world upgrade/downgrade
    behaviour and preventing backend-specific hangs.

    The relative ``-1`` specifier is used for downgrade so that merge revisions
    are handled correctly: it always rolls back exactly one step from the
    current head regardless of how many parents a merge revision has.
    """
    revisions = _get_revisions_in_order()

    for revision_id in revisions:
        try:
            # Step 1: upgrade to this revision.
            upgradedb(to_revision=revision_id)

            # Step 2: downgrade exactly one step back.
            downgrade(to_revision="-1")

            # Step 3: re-apply so the next iteration starts from the right state.
            upgradedb(to_revision=revision_id)
        except Exception as e:
            raise AssertionError(f"Stairway test failed at revision {revision_id!r}") from e
