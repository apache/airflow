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
import pathlib
import uuid
from unittest import mock
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from openlineage.client.facet import (
    DocumentationJobFacet,
    ErrorMessageRunFacet,
    ExternalQueryRunFacet,
    JobTypeJobFacet,
    NominalTimeRunFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    ParentRunFacet,
    ProcessingEngineRunFacet,
    SqlJobFacet,
)
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState

from airflow.providers.openlineage.conf import (
    config_path,
    custom_extractors,
    disabled_operators,
    is_disabled,
    is_source_enabled,
    namespace,
    transport,
)
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.plugins.adapter import _PRODUCER, OpenLineageAdapter
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clear_cache():
    config_path.cache_clear()
    is_source_enabled.cache_clear()
    disabled_operators.cache_clear()
    custom_extractors.cache_clear()
    namespace.cache_clear()
    transport.cache_clear()
    is_disabled.cache_clear()
    try:
        yield
    finally:
        config_path.cache_clear()
        is_source_enabled.cache_clear()
        disabled_operators.cache_clear()
        custom_extractors.cache_clear()
        namespace.cache_clear()
        transport.cache_clear()
        is_disabled.cache_clear()


@patch.dict(
    os.environ,
    {"OPENLINEAGE_URL": "http://ol-api:5000", "OPENLINEAGE_API_KEY": "api-key"},
)
def test_create_client_from_ol_env():
    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.url == "http://ol-api:5000"


@conf_vars(
    {
        ("openlineage", "transport"): '{"type": "http", "url": "http://ol-api:5000",'
        ' "auth": {"type": "api_key", "apiKey": "api-key"}}'
    }
)
def test_create_client_from_config_with_options():
    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.kind == "http"
    assert client.transport.url == "http://ol-api:5000"


@conf_vars(
    {
        ("openlineage", "transport"): '{"url": "http://ol-api:5000",'
        ' "auth": {"type": "api_key", "apiKey": "api-key"}}'
    }
)
def test_fails_to_create_client_without_type():
    with pytest.raises(KeyError):
        OpenLineageAdapter().get_or_create_openlineage_client()


def test_create_client_from_yaml_config():
    current_folder = pathlib.Path(__file__).parent.resolve()
    yaml_config = str((current_folder / "openlineage_configs" / "http.yaml").resolve())
    with conf_vars({("openlineage", "config_path"): yaml_config}):
        client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.kind == "http"


def test_create_client_from_env_var_config():
    current_folder = pathlib.Path(__file__).parent.resolve()
    yaml_config = str((current_folder / "openlineage_configs" / "http.yaml").resolve())
    with patch.dict(os.environ, {"OPENLINEAGE_CONFIG": yaml_config}):
        client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.kind == "http"
    assert client.transport.url == "http://localhost:5050"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://ol-from-env:5000",
        "OPENLINEAGE_API_KEY": "api-key-from-env",
    },
)
@patch.dict(os.environ, {"OPENLINEAGE_CONFIG": "some/config.yml"})
def test_create_client_overrides_env_vars():
    current_folder = pathlib.Path(__file__).parent.resolve()
    yaml_config = str((current_folder / "openlineage_configs" / "http.yaml").resolve())
    with conf_vars({("openlineage", "config_path"): yaml_config}):
        client = OpenLineageAdapter().get_or_create_openlineage_client()

        assert client.transport.kind == "http"
        assert client.transport.url == "http://localhost:5050"

    transport.cache_clear()
    config_path.cache_clear()

    with conf_vars({("openlineage", "transport"): '{"type": "console"}'}):
        client = OpenLineageAdapter().get_or_create_openlineage_client()

        assert client.transport.kind == "console"


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_start_event(mock_stats_incr, mock_stats_timer):
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
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                        "processing_engine": ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "documentation": DocumentationJobFacet(description="description"),
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_start_event_with_additional_information(mock_stats_incr, mock_stats_timer):
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.start_task(
        run_id=run_id,
        job_name="job",
        job_description="description",
        event_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        code_location=None,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        owners=["owner1", "owner2"],
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
            run_facets={"externalQuery1": ExternalQueryRunFacet(externalQueryId="123", source="source")},
        ),
        run_facets={"externalQuery2": ExternalQueryRunFacet(externalQueryId="999", source="source")},
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
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                        "processing_engine": ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                        "parent": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "parentRun": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "externalQuery1": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                        "externalQuery2": ExternalQueryRunFacet(externalQueryId="999", source="source"),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "documentation": DocumentationJobFacet(description="description"),
                        "ownership": OwnershipJobFacet(
                            owners=[
                                OwnershipJobFacetOwners(name="owner1", type=None),
                                OwnershipJobFacetOwners(name="owner2", type=None),
                            ]
                        ),
                        "sql": SqlJobFacet(query="SELECT 1;"),
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                    },
                ),
                producer=_PRODUCER,
                inputs=[
                    Dataset(namespace="bigquery", name="a.b.c"),
                    Dataset(namespace="bigquery", name="x.y.z"),
                ],
                outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_complete_event(mock_stats_incr, mock_stats_timer):
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        job_name="job",
        task=OperatorLineage(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time,
                run=Run(runId=run_id, facets={}),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        )
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_complete_event_with_additional_information(mock_stats_incr, mock_stats_timer):
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        job_name="job",
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
            run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source")},
        ),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "parent": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "parentRun": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                    },
                ),
                job=Job(
                    namespace="default",
                    name="job",
                    facets={
                        "sql": SqlJobFacet(query="SELECT 1;"),
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                    },
                ),
                producer=_PRODUCER,
                inputs=[
                    Dataset(namespace="bigquery", name="a.b.c"),
                    Dataset(namespace="bigquery", name="x.y.z"),
                ],
                outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_failed_event(mock_stats_incr, mock_stats_timer):
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        job_name="job",
        task=OperatorLineage(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time,
                run=Run(runId=run_id, facets={}),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        )
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_failed_event_with_additional_information(mock_stats_incr, mock_stats_timer):
    client = MagicMock()
    adapter = OpenLineageAdapter(client)

    run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        job_name="job",
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source")},
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
        ),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "parent": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "parentRun": ParentRunFacet(
                            run={"runId": "parent_run_id"},
                            job={"namespace": namespace(), "name": "parent_job_name"},
                        ),
                        "externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                    },
                ),
                job=Job(
                    namespace="default",
                    name="job",
                    facets={
                        "sql": SqlJobFacet(query="SELECT 1;"),
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                    },
                ),
                producer=_PRODUCER,
                inputs=[
                    Dataset(namespace="bigquery", name="a.b.c"),
                    Dataset(namespace="bigquery", name="x.y.z"),
                ],
                outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_started_event(mock_stats_incr, mock_stats_timer, uuid):
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
                            nominalStartTime=event_time.isoformat(),
                            nominalEndTime=event_time.isoformat(),
                        )
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="dag_id",
                    facets={
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                        )
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_complete_event(mock_stats_incr, mock_stats_timer, uuid):
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
                job=Job(
                    namespace=namespace(),
                    name="dag_id",
                    facets={
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                        )
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.plugins.adapter.uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_failed_event(mock_stats_incr, mock_stats_timer, uuid):
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
                job=Job(
                    namespace=namespace(),
                    name="dag_id",
                    facets={
                        "jobType": JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                        )
                    },
                ),
                producer=_PRODUCER,
                inputs=[],
                outputs=[],
            )
        )
        in client.emit.mock_calls
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.get_or_create_openlineage_client")
@patch("airflow.providers.openlineage.plugins.adapter.OpenLineageRedactor")
@patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_openlineage_adapter_stats_emit_failed(
    mock_stats_incr, mock_stats_timer, mock_redact, mock_get_client
):
    adapter = OpenLineageAdapter()
    mock_get_client.return_value.emit.side_effect = Exception()

    adapter.emit(MagicMock())

    mock_stats_timer.assert_called_with("ol.emit.attempts")
    mock_stats_incr.assert_has_calls([mock.call("ol.emit.failed")])


def test_build_dag_run_id_is_valid_uuid():
    dag_id = "test_dag"
    dag_run_id = "run_1"
    result = OpenLineageAdapter.build_dag_run_id(dag_id, dag_run_id)
    assert uuid.UUID(result)


def test_build_dag_run_id_different_inputs_give_different_results():
    result1 = OpenLineageAdapter.build_dag_run_id("dag1", "run1")
    result2 = OpenLineageAdapter.build_dag_run_id("dag2", "run2")
    assert result1 != result2


def test_build_dag_run_id_uses_correct_methods_underneath():
    dag_id = "test_dag"
    dag_run_id = "run_1"
    expected = str(uuid.uuid3(uuid.NAMESPACE_URL, f"{namespace()}.{dag_id}.{dag_run_id}"))
    actual = OpenLineageAdapter.build_dag_run_id(dag_id, dag_run_id)
    assert actual == expected


def test_build_task_instance_run_id_is_valid_uuid():
    result = OpenLineageAdapter.build_task_instance_run_id("dag_1", "task_1", "2023-01-01", 1)
    assert uuid.UUID(result)


def test_build_task_instance_run_id_different_inputs_gives_different_results():
    result1 = OpenLineageAdapter.build_task_instance_run_id("dag_1", "task1", "2023-01-01", 1)
    result2 = OpenLineageAdapter.build_task_instance_run_id("dag_1", "task2", "2023-01-02", 2)
    assert result1 != result2


def test_build_task_instance_run_id_uses_correct_methods_underneath():
    dag_id = "dag_1"
    task_id = "task_1"
    execution_date = "2023-01-01"
    try_number = 1
    expected = str(
        uuid.uuid3(uuid.NAMESPACE_URL, f"{namespace()}.{dag_id}.{task_id}.{execution_date}.{try_number}")
    )
    actual = OpenLineageAdapter.build_task_instance_run_id(dag_id, task_id, execution_date, try_number)
    assert actual == expected


def test_configuration_precedence_when_creating_ol_client():
    _section_name = "openlineage"
    current_folder = pathlib.Path(__file__).parent.resolve()
    yaml_config = str((current_folder / "openlineage_configs" / "http.yaml").resolve())

    # First, check config_path in Airflow configuration (airflow.cfg or env variable)
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_URL": "http://wrong.com",
            "OPENLINEAGE_API_KEY": "wrong_api_key",
            "OPENLINEAGE_CONFIG": "some/config.yml",
        },
        clear=True,
    ):
        with conf_vars(
            {
                (_section_name, "transport"): '{"type": "kafka", "topic": "test", "config": {"acks": "all"}}',
                (_section_name, "config_path"): yaml_config,
            },
        ):
            client = OpenLineageAdapter().get_or_create_openlineage_client()
            assert client.transport.kind == "http"
            assert client.transport.config.url == "http://localhost:5050"
            assert client.transport.config.endpoint == "api/v1/lineage"
            assert client.transport.config.auth.api_key == "random_token"

    config_path.cache_clear()
    transport.cache_clear()

    # Second, check transport in Airflow configuration (airflow.cfg or env variable)
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_URL": "http://wrong.com",
            "OPENLINEAGE_API_KEY": "wrong_api_key",
            "OPENLINEAGE_CONFIG": "some/config.yml",
        },
        clear=True,
    ):
        with conf_vars(
            {
                (_section_name, "transport"): '{"type": "kafka", "topic": "test", "config": {"acks": "all"}}',
                (_section_name, "config_path"): "",
            },
        ):
            client = OpenLineageAdapter().get_or_create_openlineage_client()
            assert client.transport.kind == "kafka"
            assert client.transport.kafka_config.topic == "test"
            assert client.transport.kafka_config.config == {"acks": "all"}

    config_path.cache_clear()
    transport.cache_clear()

    # Third, check legacy OPENLINEAGE_CONFIG env variable
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_URL": "http://wrong.com",
            "OPENLINEAGE_API_KEY": "wrong_api_key",
            "OPENLINEAGE_CONFIG": yaml_config,
        },
        clear=True,
    ):
        with conf_vars(
            {
                (_section_name, "transport"): "",
                (_section_name, "config_path"): "",
            },
        ):
            client = OpenLineageAdapter().get_or_create_openlineage_client()
            assert client.transport.kind == "http"
            assert client.transport.config.url == "http://localhost:5050"
            assert client.transport.config.endpoint == "api/v1/lineage"
            assert client.transport.config.auth.api_key == "random_token"

    config_path.cache_clear()
    transport.cache_clear()

    # Fourth, check legacy OPENLINEAGE_URL env variable
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_URL": "http://test.com",
            "OPENLINEAGE_API_KEY": "test_api_key",
            "OPENLINEAGE_CONFIG": "",
        },
        clear=True,
    ):
        with conf_vars(
            {
                (_section_name, "transport"): "",
                (_section_name, "config_path"): "",
            },
        ):
            client = OpenLineageAdapter().get_or_create_openlineage_client()
            assert client.transport.kind == "http"
            assert client.transport.config.url == "http://test.com"
            assert client.transport.config.endpoint == "api/v1/lineage"
            assert client.transport.config.auth.api_key == "test_api_key"

    config_path.cache_clear()
    transport.cache_clear()

    # If all else fails, use console transport
    with patch.dict(os.environ, {}, clear=True):
        with conf_vars(
            {
                (_section_name, "transport"): "",
                (_section_name, "config_path"): "",
            },
        ):
            client = OpenLineageAdapter().get_or_create_openlineage_client()
            assert client.transport.kind == "console"
