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
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.utils.state import TaskInstanceState
from airflow.webhooks.wait_resumption import lock_current_waits
from airflow.webhooks.waits import compute_key_digest, compute_match_digest

from tests_common.test_utils.db import clear_db_runs

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    yield
    clear_db_runs()


def add_wait(
    session: Session, ti_id: UUID, *, number: int, is_open: bool = True, **overrides: Any
) -> InboundWait:
    match = {"number": number}
    wait = InboundWait(
        ti_id=ti_id,
        webhook="fake",
        events=None,
        match=match,
        key_digest=compute_key_digest(match),
        match_digest=compute_match_digest(None, match),
        deadline=NOW + timedelta(hours=1),
        **overrides,
    )
    session.add(wait)
    session.flush()
    # Set after the insert: the ORM inserts a column's default in place of None.
    wait.open = True if is_open else None
    session.commit()
    return wait


class TestLockCurrentWaits:
    def test_returns_the_latest_open_wait_of_each_task_instance(self, create_task_instance, session):
        with_two_open = create_task_instance(dag_id="two_open", state=TaskInstanceState.AWAITING_INPUT)
        with_closed = create_task_instance(dag_id="closed", state=TaskInstanceState.AWAITING_INPUT)
        without_wait = create_task_instance(dag_id="no_wait", state=TaskInstanceState.AWAITING_INPUT)
        session.commit()
        add_wait(session, with_two_open.id, number=1, created_at=NOW - timedelta(minutes=2))
        latest = add_wait(session, with_two_open.id, number=2, created_at=NOW - timedelta(minutes=1))
        add_wait(session, with_closed.id, number=3, is_open=False, status=WaitStatus.ENDED)

        current = lock_current_waits([with_two_open.id, with_closed.id, without_wait.id], session=session)

        assert {ti_id: wait.id for ti_id, wait in current.items()} == {with_two_open.id: latest.id}
