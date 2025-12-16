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


@pytest.fixture
def ver_client(client):
    client.headers["Airflow-API-Version"] = "2025-11-07"
    return client


def test_get_previous_dag_run_redirect(ver_client, session, dag_maker):
    with dag_maker(dag_id="test_dag_id", session=session, serialized=True):
        pass
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 1),
        run_id="run1",
    )
    dag_maker.create_dagrun(
        state=DagRunState.FAILED,
        logical_date=timezone.datetime(2025, 1, 5),
        run_id="run2",
    )
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 10),
        run_id="run3",
    )
    session.commit()

    response = ver_client.get(
        "/execution/dag-runs/test_dag_id/previous",
        params={"logical_date": timezone.datetime(2025, 1, 10).isoformat()},
    )
    assert response.status_code == 200
    result = response.json()
    assert result["dag_id"] == "test_dag_id"
    assert result["run_id"] == "run2"  # Most recent before 2025-01-10
    assert result["state"] == "failed"
