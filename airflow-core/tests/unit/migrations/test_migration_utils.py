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

Walks every individual migration step forward (upgrade) then backward (downgrade)
to verify that all migrations are fully reversible.  This catches problems like:
- a migration that creates a constraint the downgrade forgets to drop
- a downgrade that references a column that was never added by the upgrade

The test starts from the squashed baseline (the 2.6.2 snapshot, looked up via
``_REVISION_HEADS_MAP["2.6.2"]``) and iterates through each subsequent revision
in topological order.
"""

from __future__ import annotations

import pytest
from alembic import command
from alembic.script import ScriptDirectory

from airflow.utils.db import _REVISION_HEADS_MAP, _get_alembic_config, upgradedb

pytestmark = pytest.mark.db_test

# The squashed "start-of-history" revision – stairway starts here.
_SQUASHED_BASE_REVISION = _REVISION_HEADS_MAP["2.6.2"]


def _get_revisions_in_order() -> list[str]:
    """Return revision IDs in upgrade order, starting after the squashed base."""
    config = _get_alembic_config()
    script = ScriptDirectory.from_config(config)

    # walk_revisions() yields from head → base; reverse for upgrade order
    all_revisions = list(script.walk_revisions())
    all_revisions.reverse()

    revision_ids = [rev.revision for rev in all_revisions]

    try:
        base_index = revision_ids.index(_SQUASHED_BASE_REVISION)
    except ValueError:
        return revision_ids  # squashed revision not present, return all

    return revision_ids[base_index + 1:]


@pytest.fixture(scope="module")
def stairway_db():
    """
    Bring the DB to the squashed baseline before the stairway test runs, and
    restore it to the current heads afterwards so other tests are unaffected.
    """
    config = _get_alembic_config()

    # Downgrade to the squashed base so the stairway starts from a known state.
    command.downgrade(config, revision=_SQUASHED_BASE_REVISION)

    yield

    # Always restore to the latest heads, even if the test failed mid-way.
    upgradedb()


def test_migration_stairway(stairway_db) -> None:
    """
    Walk every incremental migration step: upgrade → downgrade → re-upgrade.

    Runs as a single test (not parametrized) so execution order and DB state
    are guaranteed without relying on xdist grouping or cross-test ordering.
    Each step uses Alembic's relative ``-1`` target for downgrade so that merge
    revisions are handled correctly — it always rolls back exactly one step from
    the current head regardless of how many parents a merge revision has.
    """
    config = _get_alembic_config()
    revisions = _get_revisions_in_order()

    for revision_id in revisions:
        # Step 1: upgrade to this revision
        command.upgrade(config, revision=revision_id)

        # Step 2: downgrade exactly one step back.
        # Using the relative specifier "-1" from the current head is correct for
        # both normal and merge revisions — it never picks an arbitrary parent.
        command.downgrade(config, revision="-1")

        # Step 3: re-apply so the next iteration starts from the right state.
        command.upgrade(config, revision=revision_id)
