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

# Fields added to DagRun after each version. Clients of a version forbid unknown keys, so a
# field must be absent for every version older than the one that introduced it.
ADDED_AFTER = {
    "2025-08-10": ("triggering_user_name", "note", "partition_key", "team_name", "partition_date"),
    "2025-11-05": ("note", "partition_key", "team_name", "partition_date"),
}


@pytest.fixture
def ver_client(client):
    client.headers["Airflow-API-Version"] = "2025-11-05"
    return client


@pytest.mark.parametrize("api_version", sorted(ADDED_AFTER))
def test_compat_previous_dag_run_hides_later_fields(api_version, client, session, dag_maker):
    """The compat previous-run route must answer in the shape each version defines.

    It has no response model, so the schema-based converters cannot match it and it is
    addressed by path instead.
    """
    client.headers["Airflow-API-Version"] = api_version
    ver_client = client
    with dag_maker(dag_id="compat_dag", session=session, serialized=True):
        pass
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS, logical_date=timezone.datetime(2025, 1, 1), run_id="previous"
    )
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS, logical_date=timezone.datetime(2025, 1, 10), run_id="current"
    )
    session.commit()

    response = ver_client.get(
        "/execution/dag-runs/compat_dag/previous",
        params={"logical_date": timezone.datetime(2025, 1, 10).isoformat()},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "previous"
    assert [f for f in ADDED_AFTER[api_version] if f in body] == []
    if api_version == "2025-11-05":
        # Introduced in this version, so it must still be served here.
        assert "triggering_user_name" in body


def test_compat_previous_dag_run_start_date_is_never_null(ver_client, session, dag_maker):
    """A queued previous run must still report a non-null start_date at this version."""
    with dag_maker(dag_id="compat_dag_queued", session=session, serialized=True):
        pass
    dag_maker.create_dagrun(
        state=DagRunState.QUEUED,
        logical_date=timezone.datetime(2025, 1, 1),
        run_id="queued_previous",
        start_date=None,
    )
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS, logical_date=timezone.datetime(2025, 1, 10), run_id="current"
    )
    session.commit()

    response = ver_client.get(
        "/execution/dag-runs/compat_dag_queued/previous",
        params={"logical_date": timezone.datetime(2025, 1, 10).isoformat()},
    )

    assert response.status_code == 200
    assert response.json()["start_date"] is not None
