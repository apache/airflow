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

from datetime import datetime, timedelta, timezone as dt_timezone
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select

from airflow.providers.edge3.models.edge_logs import EdgeLogsModel

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

CHUNK_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt_timezone.utc)


def _make_log_chunk(
    *,
    map_index: int = -1,
    try_number: int = 1,
    log_chunk_time: datetime = CHUNK_TIME,
    log_chunk_data: str = "log line 1\n",
) -> EdgeLogsModel:
    return EdgeLogsModel(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=map_index,
        try_number=try_number,
        log_chunk_time=log_chunk_time,
        log_chunk_data=log_chunk_data,
    )


def test_constructor_maps_all_fields():
    chunk = _make_log_chunk(map_index=2, try_number=3)

    assert chunk.dag_id == "test_dag"
    assert chunk.task_id == "test_task"
    assert chunk.run_id == "test_run"
    assert chunk.map_index == 2
    assert chunk.try_number == 3
    assert chunk.log_chunk_time == CHUNK_TIME
    assert chunk.log_chunk_data == "log line 1\n"


@pytest.mark.db_test
class TestEdgeLogsModelPersistence:
    @pytest.fixture(autouse=True)
    def _clean_table(self, session: Session):
        session.execute(delete(EdgeLogsModel))
        session.commit()

    def test_round_trip(self, session: Session):
        session.add(_make_log_chunk())
        session.commit()
        session.expunge_all()

        chunk = session.scalars(select(EdgeLogsModel)).one()
        assert chunk.log_chunk_time == CHUNK_TIME
        assert chunk.log_chunk_data == "log line 1\n"

    def test_incremental_chunks_of_same_task_are_separate_rows(self, session: Session):
        session.add(_make_log_chunk())
        session.add(
            _make_log_chunk(
                log_chunk_time=CHUNK_TIME + timedelta(seconds=10),
                log_chunk_data="log line 2\n",
            )
        )
        session.commit()

        chunks = session.scalars(select(EdgeLogsModel).order_by(EdgeLogsModel.log_chunk_time)).all()
        assert [chunk.log_chunk_data for chunk in chunks] == ["log line 1\n", "log line 2\n"]
