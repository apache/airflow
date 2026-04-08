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

The test starts from the squashed baseline (revision 4bc4d934e2bc, the 2.6.2
snapshot) and iterates through each revision in topological order.
"""

from __future__ import annotations

import pytest
from alembic import command
from alembic.script import ScriptDirectory

from airflow.utils.db import _get_alembic_config

pytestmark = pytest.mark.db_test

# The squashed "start-of-history" revision – stairway starts here.
_SQUASHED_BASE_REVISION = "4bc4d934e2bc"


def _get_revisions_in_order() -> list[str]:
    """Return revision IDs in upgrade order, starting after the squashed base."""
    config = _get_alembic_config()
    script = ScriptDirectory.from_config(config)

    # walk_revisions() yields from head → base; reverse for upgrade order
    all_revisions = list(script.walk_revisions())
    all_revisions.reverse()

    revision_ids = [rev.revision for rev in all_revisions]

    # Find the squashed base and return everything *after* it (the incremental steps)
    try:
        base_index = revision_ids.index(_SQUASHED_BASE_REVISION)
    except ValueError:
        return revision_ids  # squashed revision not present, return all

    return revision_ids[base_index + 1 :]


# Force these tests to run sequentially on the same worker to prevent DB state 
# conflicts during parallel testing (e.g., when using pytest-xdist).
@pytest.mark.xdist_group(name="migration_stairway")
@pytest.mark.parametrize(
    "revision_id",
    [pytest.param(rev, id=rev) for rev in _get_revisions_in_order()],
)
def test_migration_stairway(revision_id: str) -> None:
    """
    For each migration step verify that upgrade followed by downgrade works.

    The test applies the migration under test (upgrade to *revision_id*), then
    immediately rolls it back (downgrade to the previous revision), and finally
    re-applies it so that subsequent parametrized steps start from the correct
    state.
    """
    config = _get_alembic_config()
    script = ScriptDirectory.from_config(config)

    revision_obj = script.get_revision(revision_id)
    if revision_obj is None:
        pytest.skip(f"Revision {revision_id!r} not found in migration scripts")

    prev_revision = revision_obj.down_revision
    if isinstance(prev_revision, tuple):
        # Merge point – pick the first parent.
        prev_revision = prev_revision[0]

    # Step 1: upgrade to this revision
    command.upgrade(config, revision=revision_id)

    # Determine the target revision for downgrade
    target = prev_revision if prev_revision else "base"
    
    try:
        # Step 2: downgrade back to the previous revision
        command.downgrade(config, revision=target)
    finally:
        # Step 3: re-apply so subsequent steps see a consistent DB state.
        command.upgrade(config, revision=revision_id)
