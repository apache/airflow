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

from airflow._shared.timezones import timezone
from airflow.utils.state import DagRunState

pytestmark = pytest.mark.db_test

ADDED_IN_2026_06_30 = frozenset({"team_name", "partition_date"})


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before ``team_name`` and ``partition_date`` were added."""
    client.headers["Airflow-API-Version"] = "2026-04-06"
    return client


@pytest.fixture
def dag_runs(session, dag_maker):
    with dag_maker(dag_id="test_dag_run_fields", session=session, serialized=True):
        pass
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 1),
        run_id="run1",
    )
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 10),
        run_id="run2",
    )
    session.commit()


@pytest.mark.usefixtures("dag_runs")
def test_get_dag_run_omits_new_fields(old_ver_client):
    response = old_ver_client.get("/execution/dag-runs/test_dag_run_fields/run1")

    assert response.status_code == 200
    assert not ADDED_IN_2026_06_30 & response.json().keys()


@pytest.mark.usefixtures("dag_runs")
def test_get_previous_dag_run_omits_new_fields(old_ver_client):
    response = old_ver_client.get(
        "/execution/dag-runs/previous",
        params={
            "dag_id": "test_dag_run_fields",
            "logical_date": timezone.datetime(2025, 1, 10).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run1"
    assert not ADDED_IN_2026_06_30 & response.json().keys()


@pytest.mark.usefixtures("dag_runs")
def test_get_previous_dag_run_without_a_match(old_ver_client):
    response = old_ver_client.get(
        "/execution/dag-runs/previous",
        params={
            "dag_id": "test_dag_run_fields",
            "logical_date": timezone.datetime(2024, 1, 1).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json() is None
