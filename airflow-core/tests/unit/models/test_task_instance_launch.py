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

import pytest

from airflow.models.task_instance_launch import TaskInstanceLaunch, TaskInstanceLaunchState

from tests_common.test_utils.db import clear_db_task_instance_launches

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_task_instance_launches()
    yield
    clear_db_task_instance_launches()


def _launch(token: str = "token", task_instance_id: str = "ti-id") -> TaskInstanceLaunch:
    return TaskInstanceLaunch(
        token=token,
        task_instance_id=task_instance_id,
        dag_id="dag",
        task_id="task",
        run_id="run",
        map_index=-1,
        try_number=1,
        executor="executor",
    )


def test_launch_defaults_to_active(session):
    launch = _launch()
    session.add(launch)
    session.flush()

    assert launch.state == TaskInstanceLaunchState.ACTIVE
    assert launch.created_at is not None
    assert launch.updated_at is not None
    assert launch.consumed_at is None
    assert launch.superseded_at is None


@pytest.mark.parametrize(
    ("method", "state", "timestamp_attribute"),
    [
        pytest.param("mark_consumed", TaskInstanceLaunchState.CONSUMED, "consumed_at"),
        pytest.param("mark_superseded", TaskInstanceLaunchState.SUPERSEDED, "superseded_at"),
    ],
)
def test_terminal_transition_is_guarded(session, method, state, timestamp_attribute):
    launch = _launch()
    session.add(launch)
    session.flush()

    transition = getattr(TaskInstanceLaunch, method)
    assert transition(launch.token, session) is True
    assert transition(launch.token, session) is False
    session.refresh(launch)

    assert launch.state == state
    assert getattr(launch, timestamp_attribute) is not None


def test_launch_lookup_helpers(session):
    active = _launch("active", "ti-id")
    consumed = _launch("consumed", "ti-id")
    other = _launch("other", "other-ti")
    session.add_all([active, consumed, other])
    session.flush()
    TaskInstanceLaunch.mark_consumed(consumed.token, session)

    assert TaskInstanceLaunch.get_active_by_token(active.token, session) is active
    assert TaskInstanceLaunch.get_active_by_token(consumed.token, session) is None
    assert TaskInstanceLaunch.get_by_token(consumed.token, session) is consumed
    assert TaskInstanceLaunch.get_task_instance_active_launches("ti-id", session) == [active]
