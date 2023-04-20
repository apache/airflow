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

import datetime
import os
import uuid
from unittest import mock
from unittest.mock import ANY, MagicMock, call, patch

from openlineage.client.facet import (
    DocumentationJobFacet,
    ErrorMessageRunFacet,
    NominalTimeRunFacet,
    ProcessingEngineRunFacet,
)
from openlineage.client.run import Job, Run, RunEvent, RunState

from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.plugins.adapter import _PRODUCER, OpenLineageAdapter


@patch.dict(os.environ, {"OPENLINEAGE_URL": "http://ol-api:5000", "OPENLINEAGE_API_KEY": "api-key"})
def test_create_client_from_ol_env():
    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.url == "http://ol-api:5000"
    assert "Authorization" in client.transport.session.headers
    assert client.transport.session.headers["Authorization"] == "Bearer api-key"


def test_emit_start_event():
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.start_task(
        run_id=run_id,
        job_name="job",
        job_description="description",
        event_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        code_location=None,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        owners=[],
        task=None,
        run_facets=None,
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.START,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "nominalTime": NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00", nominalEndTime="2022-01-01T00:00:00"
                        ),
                        "processing_engine": ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                    },
                ),
                job=Job(
                    namespace="default",
                    name="job",
                    facets={"documentation": DocumentationJobFacet(description="description")},
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )


def test_emit_complete_event():
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        job_name="job",
        task=OperatorLineage(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time,
                run=Run(runId=run_id, facets={}),
                job=Job(namespace="default", name="job", facets={}),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )


def test_emit_failed_event():
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        job_name="job",
        task=OperatorLineage(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time,
                run=Run(runId=run_id, facets={}),
                job=Job(namespace="default", name="job", facets={}),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
def test_emit_dag_started_event(uuid):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.now()
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    dagrun_mock = MagicMock()
    dagrun_mock.start_date = event_time
    dagrun_mock.run_id = run_id
    dagrun_mock.dag_id = dag_id
    uuid.uuid3.return_value = random_uuid

    adapter.dag_started(
        dag_run=dagrun_mock,
        msg="",
        nominal_start_time=event_time.isoformat(),
        nominal_end_time=event_time.isoformat(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.START,
                eventTime=event_time.isoformat(),
                run=Run(
                    runId=random_uuid,
                    facets={
                        "nominalTime": NominalTimeRunFacet(
                            nominalStartTime=event_time.isoformat(), nominalEndTime=event_time.isoformat()
                        )
                    },
                ),
                job=Job(namespace="default", name="dag_id", facets={}),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
def test_emit_dag_complete_event(uuid):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.now()
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    dagrun_mock = MagicMock()
    dagrun_mock.start_date = event_time
    dagrun_mock.end_date = event_time
    dagrun_mock.run_id = run_id
    dagrun_mock.dag_id = dag_id
    uuid.uuid3.return_value = random_uuid

    adapter.dag_success(
        dag_run=dagrun_mock,
        msg="",
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time.isoformat(),
                run=Run(runId=random_uuid, facets={}),
                job=Job(namespace="default", name="dag_id", facets={}),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
def test_emit_dag_failed_event(uuid):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.now()
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    dagrun_mock = MagicMock()
    dagrun_mock.start_date = event_time
    dagrun_mock.end_date = event_time
    dagrun_mock.run_id = run_id
    dagrun_mock.dag_id = dag_id
    uuid.uuid3.return_value = random_uuid

    adapter.dag_failed(
        dag_run=dagrun_mock,
        msg="error msg",
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time.isoformat(),
                run=Run(
                    runId=random_uuid,
                    facets={
                        "errorMessage": ErrorMessageRunFacet(
                            message="error msg", programmingLanguage="python"
                        )
                    },
                ),
                job=Job(namespace="default", name="dag_id", facets={}),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )
