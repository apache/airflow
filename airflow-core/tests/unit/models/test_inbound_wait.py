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

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.taskinstance import TaskInstance

from tests_common.test_utils.db import clear_db_runs

DEADLINE = datetime(2026, 10, 21, tzinfo=UTC)


def build_wait(ti_id, **overrides: Any) -> InboundWait:
    fields: dict[str, Any] = {
        "ti_id": ti_id,
        "webhook": "github",
        "events": ["pull_request"],
        "match": {"number": ["42"]},
        "key_digest": "k" * 64,
        "match_digest": "m" * 64,
        "deadline": DEADLINE,
    }
    return InboundWait(**(fields | overrides))


class TestWaitStatus:
    @pytest.mark.parametrize("status", list(WaitStatus))
    def test_formats_as_its_value(self, status):
        assert str(status) == f"{status}" == status.value


@pytest.mark.db_test
class TestInboundWait:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_a_new_wait_is_open_and_registered(self, create_task_instance, session):
        wait = build_wait(create_task_instance().id)
        session.add(wait)
        session.flush()

        assert (wait.open, wait.status) == (True, WaitStatus.REGISTERED)

    def test_a_second_open_wait_with_the_same_key_is_rejected(self, create_task_instance, session):
        ti = create_task_instance()
        session.add(build_wait(ti.id))
        session.flush()

        session.add(build_wait(ti.id))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_closed_waits_do_not_hold_their_key(self, create_task_instance, session):
        ti = create_task_instance()
        for _ in range(2):
            wait = build_wait(ti.id)
            session.add(wait)
            session.flush()
            wait.open = None
            session.flush()
        session.add(build_wait(ti.id))
        session.flush()

        assert session.scalars(select(InboundWait.open).order_by(InboundWait.id)).all() == [None, None, True]

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param({"webhook": "github"}, {"webhook": "gitlab"}, id="two-webhooks"),
            pytest.param({"match_digest": "m" * 64}, {"match_digest": "n" * 64}, id="two-keys"),
        ],
    )
    def test_waits_with_distinct_keys_are_both_recorded(self, create_task_instance, session, first, second):
        ti = create_task_instance()
        session.add_all([build_wait(ti.id, **first), build_wait(ti.id, **second)])
        session.flush()

        assert session.scalar(select(func.count()).select_from(InboundWait)) == 2

    def test_explicit_none_events_are_stored_as_sql_null(self, create_task_instance, session):
        session.add(build_wait(create_task_instance().id, events=None))
        session.flush()

        query = select(func.count()).select_from(InboundWait).where(InboundWait.events.is_(None))
        assert session.scalar(query) == 1

    def test_task_instance_loads_its_waits(self, create_task_instance, session):
        ti = create_task_instance()
        wait = build_wait(ti.id)
        session.add(wait)
        session.commit()
        session.expunge_all()

        loaded = session.scalar(
            select(TaskInstance)
            .where(TaskInstance.id == ti.id)
            .options(selectinload(TaskInstance.inbound_waits))
        )
        assert [w.id for w in loaded.inbound_waits] == [wait.id]

    def test_deleting_a_task_instance_deletes_its_waits(self, create_task_instance, session):
        ti = create_task_instance()
        session.add(build_wait(ti.id))
        session.commit()

        session.delete(session.get(TaskInstance, ti.id))
        session.flush()

        assert session.scalar(select(func.count()).select_from(InboundWait)) == 0
