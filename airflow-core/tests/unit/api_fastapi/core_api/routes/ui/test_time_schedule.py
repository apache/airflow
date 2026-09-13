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

import json
from datetime import timedelta

import pendulum
import pytest

from airflow.models.dagbag import DBDagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    return DBDagBag()


@pytest.fixture(autouse=True)
def clean_database():
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture
def time_schedule_dags(dag_maker, session):
    start = pendulum.parse("2026-08-03T09:00:00Z")
    dag_specs = [
        ("scheduled_tagged", "@daily", ["selected"], DagRunState.SUCCESS),
        ("scheduled_other", "@daily", ["other"], DagRunState.FAILED),
        ("manual_tagged", None, ["selected"], DagRunState.SUCCESS),
    ]

    for dag_id, schedule, tags, state in dag_specs:
        with dag_maker(dag_id=dag_id, schedule=schedule, serialized=True, session=session, tags=tags):
            pass
        dag_run = dag_maker.create_dagrun(
            run_id=f"run-{dag_id}",
            run_type=DagRunType.SCHEDULED if schedule else DagRunType.MANUAL,
            state=state,
            logical_date=start,
            run_after=start,
            start_date=start,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        dag_run.end_date = start + timedelta(minutes=10)

    dag_maker.sync_dagbag_to_db()
    session.flush()


def _get_stream_items(response) -> list[dict]:
    return [item for line in response.text.splitlines() for item in json.loads(line)["items"]]


@pytest.mark.usefixtures("time_schedule_dags")
def test_time_schedule_filters_dags_and_runs_on_the_server(test_client):
    response = test_client.get(
        "/time-schedule",
        params=[
            ("tags", "selected"),
            ("show_scheduled_only", "true"),
            ("state", "success"),
            ("view_mode", "day"),
        ],
    )

    assert response.status_code == 200
    items = _get_stream_items(response)
    assert {item["dag_id"] for item in items} == {"scheduled_tagged"}
    assert items[0]["run_count"] == 1


@pytest.mark.usefixtures("time_schedule_dags")
def test_time_schedule_can_include_unscheduled_dags(test_client):
    response = test_client.get(
        "/time-schedule",
        params=[("tags", "selected"), ("show_scheduled_only", "false")],
    )

    assert response.status_code == 200
    assert {item["dag_id"] for item in _get_stream_items(response)} == {
        "manual_tagged",
        "scheduled_tagged",
    }


def test_time_schedule_returns_an_empty_stream_when_there_are_no_dag_runs(test_client):
    response = test_client.get("/time-schedule")

    assert response.status_code == 200
    assert response.text == ""


@pytest.mark.parametrize(
    ("params", "expected_status"),
    [
        ({"limit": 5001}, 422),
        ({"time_scale": 2}, 422),
        ({"timezone": "not-a-timezone"}, 422),
    ],
)
def test_time_schedule_rejects_unbounded_or_invalid_requests(test_client, params, expected_status):
    response = test_client.get("/time-schedule", params=params)

    assert response.status_code == expected_status
