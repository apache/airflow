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

from datetime import datetime, timezone as dt_timezone
from typing import TYPE_CHECKING

import pytest
import time_machine
from sqlalchemy import delete, select

from airflow.providers.common.compat.sdk import TaskInstanceKey
from airflow.providers.edge3.models.edge_job import EdgeJobModel, build_job_key
from airflow.providers.edge3.models.types import EXECUTE_CALLBACK_TAG
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _make_job(
    *,
    map_index: int = -1,
    try_number: int = 1,
    queued_dttm: datetime | None = None,
    edge_worker: str | None = None,
    last_update: datetime | None = None,
    team_name: str | None = None,
) -> EdgeJobModel:
    return EdgeJobModel(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=map_index,
        try_number=try_number,
        state=TaskInstanceState.QUEUED,
        queue="default",
        concurrency_slots=1,
        command="{}",
        queued_dttm=queued_dttm,
        edge_worker=edge_worker,
        last_update=last_update,
        team_name=team_name,
    )


def test_key_builds_task_instance_key():
    job = _make_job(map_index=3, try_number=2)

    assert job.key == TaskInstanceKey("test_dag", "test_task", "test_run", 2, 3)


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Callback workloads need Airflow 3.3+")
def test_build_job_key_maps_callback_row_to_callback_key():
    from airflow.models.callback import CallbackKey

    key = build_job_key(EXECUTE_CALLBACK_TAG, "abc", f"{EXECUTE_CALLBACK_TAG}-abc", 0, -1)

    assert key == CallbackKey(id="abc")


@pytest.mark.parametrize(
    ("dag_id", "run_id", "try_number", "map_index"),
    [
        pytest.param("test_dag", f"{EXECUTE_CALLBACK_TAG}-abc", 0, -1, id="other_dag"),
        pytest.param(EXECUTE_CALLBACK_TAG, "manual__2026-01-01T00:00:00+00:00", 0, -1, id="task_run_id"),
        pytest.param(EXECUTE_CALLBACK_TAG, f"{EXECUTE_CALLBACK_TAG}-abc", 1, -1, id="try_number"),
        pytest.param(EXECUTE_CALLBACK_TAG, f"{EXECUTE_CALLBACK_TAG}-abc", 0, 2, id="map_index"),
    ],
)
def test_build_job_key_keeps_task_key_unless_full_callback_identity(dag_id, run_id, try_number, map_index):
    key = build_job_key(dag_id, "abc", run_id, try_number, map_index)

    assert key == TaskInstanceKey(dag_id, "abc", run_id, try_number, map_index)


@time_machine.travel(datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc), tick=False)
def test_queued_dttm_defaults_to_now():
    job = _make_job()

    assert job.queued_dttm == datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)


def test_queued_dttm_explicit_value_is_kept():
    queued = datetime(2025, 6, 1, 8, 30, 0, tzinfo=dt_timezone.utc)

    job = _make_job(queued_dttm=queued)

    assert job.queued_dttm == queued


def test_last_update_t_returns_timestamp_of_last_update():
    last_update = datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)

    job = _make_job(last_update=last_update)

    assert job.last_update_t == last_update.timestamp()


@time_machine.travel(datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc), tick=False)
def test_last_update_t_falls_back_to_now_when_unset():
    job = _make_job()

    assert job.last_update_t == datetime.now().timestamp()


@pytest.mark.db_test
class TestEdgeJobModelPersistence:
    @pytest.fixture(autouse=True)
    def _clean_table(self, session: Session):
        session.execute(delete(EdgeJobModel))
        session.commit()

    def test_round_trip(self, session: Session):
        queued = datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)
        session.add(
            _make_job(
                queued_dttm=queued,
                edge_worker="worker-1",
                team_name="team-a",
            )
        )
        session.commit()
        session.expunge_all()

        job = session.scalars(select(EdgeJobModel)).one()
        assert job.key == TaskInstanceKey("test_dag", "test_task", "test_run", 1, -1)
        assert job.state == TaskInstanceState.QUEUED
        assert job.queue == "default"
        assert job.concurrency_slots == 1
        assert job.queued_dttm == queued
        assert job.edge_worker == "worker-1"
        assert job.team_name == "team-a"

    def test_try_numbers_are_separate_rows(self, session: Session):
        session.add(_make_job(try_number=1))
        session.add(_make_job(try_number=2))
        session.commit()

        try_numbers = set(session.scalars(select(EdgeJobModel.try_number)))
        assert try_numbers == {1, 2}
