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
from sqlalchemy import select

from airflow.models.dagrun import DagRun

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
def test_trigger_timeout_schema_follows_api_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")

    assert response.status_code == 200
    schema = response.json()["components"]["schemas"]["TriggerDAGRunPayload"]
    assert ("dag_run_timeout" in schema["properties"]) == (version == "2027-02-28")


@pytest.mark.parametrize(
    ("version", "payload", "expected_status", "expected_timeout"),
    [
        ("2026-10-30", {}, 204, None),
        ("2026-10-30", {"dag_run_timeout": "PT30M"}, 422, None),
        ("2027-02-28", {"dag_run_timeout": "PT30M"}, 204, 1800),
    ],
)
def test_trigger_timeout_follows_api_version(
    client, session, dag_maker, version, payload, expected_status, expected_timeout
):
    with dag_maker(dag_id="versioned_timeout", session=session, serialized=True):
        pass
    session.commit()
    client.headers["Airflow-API-Version"] = version

    response = client.post("/execution/dag-runs/versioned_timeout/triggered", json=payload)

    assert response.status_code == expected_status
    run = session.scalar(select(DagRun).where(DagRun.dag_id == "versioned_timeout"))
    if expected_status == 204:
        assert run.dagrun_timeout_seconds == expected_timeout
    else:
        assert run is None
