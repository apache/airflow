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
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    documentation_job,
    error_message_run,
    external_query_run,
    job_type_job,
    nominal_time_run,
    ownership_job,
    parent_run,
    processing_engine_run,
    sql_job,
    tags_job,
)

from airflow import DAG
from airflow.models.dagrun import DagRun, DagRunState
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.plugins.adapter import _PRODUCER, OpenLineageAdapter
from airflow.providers.openlineage.plugins.facets import (
    AirflowDagRunFacet,
    AirflowDebugRunFacet,
    AirflowStateRunFacet,
)
from airflow.providers.openlineage.utils.utils import get_airflow_job_facet
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import BashOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


@pytest.mark.parametrize(
    ("env_vars", "expected_logging"),
    [
        ({"AIRFLOW__LOGGING__LOGGING_LEVEL": "DEBUG"}, "DEBUG"),
        ({"AIRFLOW__LOGGING__LOGGING_LEVEL": "INFO"}, None),
        ({}, None),  # When no value is provided, default should be INFO and propagation is skipped.
    ],
)
def test_create_client_logging_propagation(env_vars, expected_logging):
    with patch.dict(os.environ, env_vars, clear=True):
        assert os.getenv("OPENLINEAGE_CLIENT_LOGGING") is None
        OpenLineageAdapter().get_or_create_openlineage_client()
        assert os.getenv("OPENLINEAGE_CLIENT_LOGGING") == expected_logging


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
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        owners=[],
        tags=[],
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
                        "nominalTime": nominal_time_run.NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "documentation": documentation_job.DocumentationJobFacet(description="description"),
                        "jobType": job_type_job.JobTypeJobFacet(
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
    parent_run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.start_task(
        run_id=run_id,
        job_name="job",
        job_description="description",
        job_description_type="text/plain",
        event_time=event_time,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        owners=["owner1", "owner2"],
        tags=["tag1", "tag2"],
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": sql_job.SQLJobFacet(query="SELECT 1;")},
            run_facets={
                "parent": parent_run.ParentRunFacet(
                    run=parent_run.Run(runId=parent_run_id),
                    job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                    root=parent_run.Root(
                        run=parent_run.RootRun(runId=parent_run_id),
                        job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                    ),
                ),
                "externalQuery1": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="123", source="source"
                ),
            },
        ),
        run_facets={
            "externalQuery2": external_query_run.ExternalQueryRunFacet(externalQueryId="999", source="source")
        },
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.START,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "nominalTime": nominal_time_run.NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                        "parent": parent_run.ParentRunFacet(
                            run=parent_run.Run(runId=parent_run_id),
                            job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                            root=parent_run.Root(
                                run=parent_run.RootRun(runId=parent_run_id),
                                job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                            ),
                        ),
                        "externalQuery1": external_query_run.ExternalQueryRunFacet(
                            externalQueryId="123", source="source"
                        ),
                        "externalQuery2": external_query_run.ExternalQueryRunFacet(
                            externalQueryId="999", source="source"
                        ),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "documentation": documentation_job.DocumentationJobFacet(
                            description="description", contentType="text/plain"
                        ),
                        "ownership": ownership_job.OwnershipJobFacet(
                            owners=[
                                ownership_job.Owner(name="owner1", type=None),
                                ownership_job.Owner(name="owner2", type=None),
                            ]
                        ),
                        "sql": sql_job.SQLJobFacet(query="SELECT 1;"),
                        "jobType": job_type_job.JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                        "tags": tags_job.TagsJobFacet(
                            tags=[
                                tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                                tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                            ]
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
        job_name="job",
        task=OperatorLineage(),
        owners=[],
        tags=[],
        job_description=None,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                        "nominalTime": nominal_time_run.NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "jobType": job_type_job.JobTypeJobFacet(
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
    parent_run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        job_name="job",
        owners=["owner1", "owner2"],
        tags=["tag1", "tag2"],
        job_description="description",
        job_description_type="text/plain",
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": sql_job.SQLJobFacet(query="SELECT 1;")},
            run_facets={
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="123", source="source"
                )
            },
        ),
        run_facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent_run_id),
                job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=parent_run_id),
                    job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                ),
            ),
            "externalQuery2": external_query_run.ExternalQueryRunFacet(
                externalQueryId="999", source="source"
            ),
        },
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "parent": parent_run.ParentRunFacet(
                            run=parent_run.Run(runId=parent_run_id),
                            job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                            root=parent_run.Root(
                                run=parent_run.RootRun(runId=parent_run_id),
                                job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                            ),
                        ),
                        "nominalTime": nominal_time_run.NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                        "externalQuery": external_query_run.ExternalQueryRunFacet(
                            externalQueryId="123", source="source"
                        ),
                        "externalQuery2": external_query_run.ExternalQueryRunFacet(
                            externalQueryId="999", source="source"
                        ),
                    },
                ),
                job=Job(
                    namespace="default",
                    name="job",
                    facets={
                        "documentation": documentation_job.DocumentationJobFacet(
                            description="description", contentType="text/plain"
                        ),
                        "ownership": ownership_job.OwnershipJobFacet(
                            owners=[
                                ownership_job.Owner(name="owner1", type=None),
                                ownership_job.Owner(name="owner2", type=None),
                            ]
                        ),
                        "sql": sql_job.SQLJobFacet(query="SELECT 1;"),
                        "jobType": job_type_job.JobTypeJobFacet(
                            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                        ),
                        "tags": tags_job.TagsJobFacet(
                            tags=[
                                tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                                tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                            ]
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
        job_name="job",
        task=OperatorLineage(),
        owners=[],
        tags=[],
        job_description=None,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
    )

    assert (
        call(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time,
                run=Run(
                    runId=run_id,
                    facets={
                        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                        ),
                        "nominalTime": nominal_time_run.NominalTimeRunFacet(
                            nominalStartTime="2022-01-01T00:00:00",
                            nominalEndTime="2022-01-01T00:00:00",
                        ),
                    },
                ),
                job=Job(
                    namespace=namespace(),
                    name="job",
                    facets={
                        "jobType": job_type_job.JobTypeJobFacet(
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
    parent_run_id = str(uuid.uuid4())
    event_time = datetime.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        job_name="job",
        owners=["owner1", "owner2"],
        tags=["tag1", "tag2"],
        job_description="description",
        job_description_type="text/plain",
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        task=OperatorLineage(
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            run_facets={
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="123", source="source"
                )
            },
            job_facets={"sql": sql_job.SQLJobFacet(query="SELECT 1;")},
        ),
        run_facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent_run_id),
                job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=parent_run_id),
                    job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                ),
            ),
            "externalQuery2": external_query_run.ExternalQueryRunFacet(
                externalQueryId="999", source="source"
            ),
        },
        error=ValueError("Error message"),
    )

    assert client.emit.mock_calls[0] == call(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=parent_run_id),
                        job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                        root=parent_run.Root(
                            run=parent_run.RootRun(runId=parent_run_id),
                            job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                        ),
                    ),
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime="2022-01-01T00:00:00",
                        nominalEndTime="2022-01-01T00:00:00",
                    ),
                    "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                    "errorMessage": error_message_run.ErrorMessageRunFacet(
                        message="Error message", programmingLanguage="python", stackTrace=None
                    ),
                    "externalQuery": external_query_run.ExternalQueryRunFacet(
                        externalQueryId="123", source="source"
                    ),
                    "externalQuery2": external_query_run.ExternalQueryRunFacet(
                        externalQueryId="999", source="source"
                    ),
                },
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "documentation": documentation_job.DocumentationJobFacet(
                        description="description", contentType="text/plain"
                    ),
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[
                            ownership_job.Owner(name="owner1", type=None),
                            ownership_job.Owner(name="owner2", type=None),
                        ]
                    ),
                    "sql": sql_job.SQLJobFacet(query="SELECT 1;"),
                    "jobType": job_type_job.JobTypeJobFacet(
                        processingType="BATCH", integration="AIRFLOW", jobType="TASK"
                    ),
                    "tags": tags_job.TagsJobFacet(
                        tags=[
                            tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                            tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                        ]
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

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
@mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_started_event(mock_stats_incr, mock_stats_timer, generate_static_uuid, mock_debug_mode):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    with DAG(
        dag_id=dag_id,
        schedule=datetime.timedelta(days=1),
        start_date=datetime.datetime(2024, 6, 1),
        description="dag desc",
        tags=["mytag1"],
    ) as dag:
        tg = TaskGroup(group_id="tg1")
        tg2 = TaskGroup(group_id="tg2", parent_group=tg)
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")  # noqa: F841
        task_1 = BashOperator(task_id="task_1", bash_command="exit 0;", task_group=tg)  # noqa: F841
        task_2 = EmptyOperator(task_id="task_2.test.dot", task_group=tg2)  # noqa: F841

    if AIRFLOW_V_3_0_PLUS:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            logical_date=event_time,
            data_interval=(event_time, event_time),
        )
    else:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            execution_date=event_time,
            data_interval=(event_time, event_time),
        )
    dag_run.dag = dag
    generate_static_uuid.return_value = random_uuid

    job_facets = {**get_airflow_job_facet(dag_run)}

    expected_dag_info = {
        "timetable": {"delta": 86400.0},
        "dag_id": dag_id,
        "description": "dag desc",
        "owner": "airflow",
        "start_date": "2024-06-01T00:00:00+00:00",
        "tags": "['mytag1']",
        "fileloc": pathlib.Path(__file__).resolve().as_posix(),
    }
    if hasattr(dag, "schedule_interval"):  # Airflow 2 compat.
        expected_dag_info["schedule_interval"] = "86400.0 seconds"
    else:  # Airflow 3 and up.
        expected_dag_info["timetable_summary"] = "1 day, 0:00:00"

    dag_run_facet = AirflowDagRunFacet(
        dag=expected_dag_info,
        dagRun={
            "conf": {},
            "dag_id": "dag_id",
            "data_interval_start": event_time.isoformat(),
            "data_interval_end": event_time.isoformat(),
            "external_trigger": False if AIRFLOW_V_3_0_PLUS else None,
            "run_id": run_id,
            "run_type": DagRunType.MANUAL,
            "start_date": event_time.isoformat(),
        },
    )
    adapter.dag_started(
        dag_id=dag_id,
        start_date=event_time,
        logical_date=event_time,
        clear_number=0,
        nominal_start_time=event_time.isoformat(),
        nominal_end_time=event_time.isoformat(),
        owners=["owner1", "owner2"],
        job_description=dag.description,
        job_description_type="text/plain",
        tags=["tag1", "tag2"],
        run_facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=random_uuid),
                job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=random_uuid),
                    job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                ),
            ),
            "airflowDagRun": dag_run_facet,
        },
        job_facets=job_facets,
    )

    assert len(client.emit.mock_calls) == 1
    client.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.START,
            eventTime=event_time.isoformat(),
            run=Run(
                runId=random_uuid,
                facets={
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=random_uuid),
                        job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                        root=parent_run.Root(
                            run=parent_run.RootRun(runId=random_uuid),
                            job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                        ),
                    ),
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime=event_time.isoformat(),
                        nominalEndTime=event_time.isoformat(),
                    ),
                    "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                    "airflowDagRun": AirflowDagRunFacet(
                        dag=expected_dag_info,
                        dagRun={
                            "conf": {},
                            "dag_id": "dag_id",
                            "data_interval_start": event_time.isoformat(),
                            "data_interval_end": event_time.isoformat(),
                            "external_trigger": False if AIRFLOW_V_3_0_PLUS else None,
                            "run_id": run_id,
                            "run_type": DagRunType.MANUAL,
                            "start_date": event_time.isoformat(),
                        },
                    ),
                    "debug": AirflowDebugRunFacet(packages=ANY),
                },
            ),
            job=Job(
                namespace=namespace(),
                name="dag_id",
                facets={
                    "documentation": documentation_job.DocumentationJobFacet(
                        description="dag desc", contentType="text/plain"
                    ),
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[
                            ownership_job.Owner(name="owner1", type=None),
                            ownership_job.Owner(name="owner2", type=None),
                        ]
                    ),
                    "tags": tags_job.TagsJobFacet(
                        tags=[
                            tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                            tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                        ]
                    ),
                    **job_facets,
                    "jobType": job_type_job.JobTypeJobFacet(
                        processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                    ),
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
    )
    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
@mock.patch.object(DagRun, "fetch_task_instances")
@mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_complete_event(
    mock_stats_incr, mock_stats_timer, generate_static_uuid, mocked_fetch_tis, mock_debug_mode
):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.now()
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    with DAG(dag_id=dag_id, schedule=None, start_date=datetime.datetime(2024, 6, 1)):
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")
        task_1 = BashOperator(task_id="task_1", bash_command="exit 0;")
        task_2 = EmptyOperator(
            task_id="task_2.test",
        )
    if AIRFLOW_V_3_0_PLUS:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            logical_date=event_time,
        )
    else:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            execution_date=event_time,
        )

    dag_run._state = DagRunState.SUCCESS
    dag_run.end_date = event_time
    if AIRFLOW_V_3_0_PLUS:
        ti0 = TaskInstance(
            task=task_0, run_id=run_id, state=TaskInstanceState.SUCCESS, dag_version_id=mock.MagicMock()
        )
        ti1 = TaskInstance(
            task=task_1, run_id=run_id, state=TaskInstanceState.SKIPPED, dag_version_id=mock.MagicMock()
        )

        ti2 = TaskInstance(
            task=task_2, run_id=run_id, state=TaskInstanceState.FAILED, dag_version_id=mock.MagicMock()
        )
    else:
        ti0 = TaskInstance(task=task_0, run_id=run_id, state=TaskInstanceState.SUCCESS)
        ti1 = TaskInstance(task=task_1, run_id=run_id, state=TaskInstanceState.SKIPPED)
        ti2 = TaskInstance(task=task_2, run_id=run_id, state=TaskInstanceState.FAILED)

    ti0.start_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
    ti0.end_date = datetime.datetime(2022, 1, 1, 0, 10, 0)

    ti1.start_date = datetime.datetime(2022, 1, 1, 0, 10, 2)
    ti1.end_date = datetime.datetime(2022, 1, 1, 0, 13, 7)

    ti2.start_date = datetime.datetime(2022, 1, 1, 0, 13, 8)
    ti2.end_date = datetime.datetime(2022, 1, 1, 0, 14, 0)

    mocked_fetch_tis.return_value = [ti0, ti1, ti2]
    generate_static_uuid.return_value = random_uuid

    adapter.dag_success(
        dag_id=dag_id,
        run_id=run_id,
        end_date=event_time,
        logical_date=event_time,
        clear_number=0,
        dag_run_state=DagRunState.SUCCESS,
        task_ids=["task_0", "task_1", "task_2.test"],
        owners=["owner1", "owner2"],
        tags=["tag1", "tag2"],
        job_description="dag desc",
        job_description_type="text/plain",
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        run_facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=random_uuid),
                job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=random_uuid),
                    job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                ),
            ),
            "airflowDagRun": AirflowDagRunFacet(dag={"description": "dag desc"}, dagRun=dag_run),
        },
    )

    client.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time.isoformat(),
            run=Run(
                runId=random_uuid,
                facets={
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=random_uuid),
                        job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                        root=parent_run.Root(
                            run=parent_run.RootRun(runId=random_uuid),
                            job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                        ),
                    ),
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime="2022-01-01T00:00:00",
                        nominalEndTime="2022-01-01T00:00:00",
                    ),
                    "airflowState": AirflowStateRunFacet(
                        dagRunState=DagRunState.SUCCESS,
                        tasksState={
                            task_0.task_id: TaskInstanceState.SUCCESS,
                            task_1.task_id: TaskInstanceState.SKIPPED,
                            task_2.task_id: TaskInstanceState.FAILED,
                        },
                        tasksDuration={
                            task_0.task_id: 600.0,
                            task_1.task_id: 185.0,
                            task_2.task_id: 52.0,
                        },
                    ),
                    "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                    "debug": AirflowDebugRunFacet(packages=ANY),
                    "airflowDagRun": AirflowDagRunFacet(dag={"description": "dag desc"}, dagRun=dag_run),
                },
            ),
            job=Job(
                namespace=namespace(),
                name=dag_id,
                facets={
                    "documentation": documentation_job.DocumentationJobFacet(
                        description="dag desc", contentType="text/plain"
                    ),
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[
                            ownership_job.Owner(name="owner1", type=None),
                            ownership_job.Owner(name="owner2", type=None),
                        ]
                    ),
                    "tags": tags_job.TagsJobFacet(
                        tags=[
                            tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                            tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                        ]
                    ),
                    "jobType": job_type_job.JobTypeJobFacet(
                        processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                    ),
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
    )

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
@mock.patch.object(DagRun, "fetch_task_instances")
@mock.patch("airflow.providers.openlineage.plugins.adapter.generate_static_uuid")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.timer")
@mock.patch("airflow.providers.openlineage.plugins.adapter.Stats.incr")
def test_emit_dag_failed_event(
    mock_stats_incr, mock_stats_timer, generate_static_uuid, mocked_fetch_tis, mock_debug_mode
):
    random_uuid = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
    client = MagicMock()
    adapter = OpenLineageAdapter(client)
    event_time = datetime.datetime.now()
    dag_id = "dag_id"
    run_id = str(uuid.uuid4())

    with DAG(dag_id=dag_id, schedule=None, start_date=datetime.datetime(2024, 6, 1)):
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")
        task_1 = BashOperator(task_id="task_1", bash_command="exit 0;")
        task_2 = EmptyOperator(task_id="task_2.test")

    if AIRFLOW_V_3_0_PLUS:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            logical_date=event_time,
        )
    else:
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            start_date=event_time,
            execution_date=event_time,
        )
    dag_run._state = DagRunState.FAILED
    dag_run.end_date = event_time

    if AIRFLOW_V_3_0_PLUS:
        ti0 = TaskInstance(
            task=task_0, run_id=run_id, state=TaskInstanceState.SUCCESS, dag_version_id=mock.MagicMock()
        )
        ti1 = TaskInstance(
            task=task_1, run_id=run_id, state=TaskInstanceState.SKIPPED, dag_version_id=mock.MagicMock()
        )

        ti2 = TaskInstance(
            task=task_2, run_id=run_id, state=TaskInstanceState.FAILED, dag_version_id=mock.MagicMock()
        )
    else:
        ti0 = TaskInstance(task=task_0, run_id=run_id, state=TaskInstanceState.SUCCESS)
        ti1 = TaskInstance(task=task_1, run_id=run_id, state=TaskInstanceState.SKIPPED)
        ti2 = TaskInstance(task=task_2, run_id=run_id, state=TaskInstanceState.FAILED)

    ti0.start_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
    ti0.end_date = datetime.datetime(2022, 1, 1, 0, 10, 0)

    ti1.start_date = datetime.datetime(2022, 1, 1, 0, 10, 2)
    ti1.end_date = datetime.datetime(2022, 1, 1, 0, 13, 7)

    ti2.start_date = datetime.datetime(2022, 1, 1, 0, 13, 8)
    ti2.end_date = datetime.datetime(2022, 1, 1, 0, 14, 0)

    mocked_fetch_tis.return_value = [ti0, ti1, ti2]

    generate_static_uuid.return_value = random_uuid

    adapter.dag_failed(
        dag_id=dag_id,
        run_id=run_id,
        end_date=event_time,
        logical_date=event_time,
        clear_number=0,
        dag_run_state=DagRunState.FAILED,
        task_ids=["task_0", "task_1", "task_2.test"],
        tags=["tag1", "tag2"],
        msg="error msg",
        owners=["owner1", "owner2"],
        job_description="dag desc",
        job_description_type="text/plain",
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        run_facets={
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=random_uuid),
                job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=random_uuid),
                    job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                ),
            ),
            "airflowDagRun": AirflowDagRunFacet(dag={"description": "dag desc"}, dagRun=dag_run),
        },
    )

    client.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time.isoformat(),
            run=Run(
                runId=random_uuid,
                facets={
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=random_uuid),
                        job=parent_run.Job(namespace=namespace(), name="parent_job_name"),
                        root=parent_run.Root(
                            run=parent_run.RootRun(runId=random_uuid),
                            job=parent_run.RootJob(namespace=namespace(), name="parent_job_name"),
                        ),
                    ),
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime="2022-01-01T00:00:00",
                        nominalEndTime="2022-01-01T00:00:00",
                    ),
                    "errorMessage": error_message_run.ErrorMessageRunFacet(
                        message="error msg", programmingLanguage="python"
                    ),
                    "airflowState": AirflowStateRunFacet(
                        dagRunState=DagRunState.FAILED,
                        tasksState={
                            task_0.task_id: TaskInstanceState.SUCCESS,
                            task_1.task_id: TaskInstanceState.SKIPPED,
                            task_2.task_id: TaskInstanceState.FAILED,
                        },
                        tasksDuration={
                            task_0.task_id: 600.0,
                            task_1.task_id: 185.0,
                            task_2.task_id: 52.0,
                        },
                    ),
                    "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                    "debug": AirflowDebugRunFacet(packages=ANY),
                    "airflowDagRun": AirflowDagRunFacet(dag={"description": "dag desc"}, dagRun=dag_run),
                },
            ),
            job=Job(
                namespace=namespace(),
                name=dag_id,
                facets={
                    "documentation": documentation_job.DocumentationJobFacet(
                        description="dag desc", contentType="text/plain"
                    ),
                    "ownership": ownership_job.OwnershipJobFacet(
                        owners=[
                            ownership_job.Owner(name="owner1", type=None),
                            ownership_job.Owner(name="owner2", type=None),
                        ]
                    ),
                    "tags": tags_job.TagsJobFacet(
                        tags=[
                            tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                            tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
                        ]
                    ),
                    "jobType": job_type_job.JobTypeJobFacet(
                        processingType="BATCH", integration="AIRFLOW", jobType="DAG"
                    ),
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
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
    logical_date = datetime.datetime.now()
    result = OpenLineageAdapter.build_dag_run_id(
        dag_id=dag_id,
        logical_date=logical_date,
        clear_number=0,
    )
    uuid_result = uuid.UUID(result)
    assert uuid_result
    assert uuid_result.version == 7


def test_build_dag_run_id_same_input_give_same_result():
    result1 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1", logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1), clear_number=0
    )
    result2 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1", logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1), clear_number=0
    )
    assert result1 == result2


def test_build_dag_run_id_different_inputs_give_different_results():
    result1 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1", logical_date=datetime.datetime.now(), clear_number=0
    )
    result2 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag2", logical_date=datetime.datetime.now(), clear_number=0
    )
    assert result1 != result2


def test_build_dag_run_id_different_clear_number_give_different_results():
    result1 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1", logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1), clear_number=0
    )
    result2 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1", logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1), clear_number=1
    )
    assert result1 != result2


def test_build_task_instance_run_id_is_valid_uuid():
    result = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        logical_date=datetime.datetime.now(),
        map_index=-1,
    )
    uuid_result = uuid.UUID(result)
    assert uuid_result
    assert uuid_result.version == 7


def test_build_task_instance_run_id_same_input_gives_same_result():
    result1 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1),
        map_index=-1,
    )
    result2 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1),
        map_index=-1,
    )
    assert result1 == result2


def test_build_task_instance_run_id_different_map_index_gives_different_result():
    result1 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1),
        map_index=1,
    )
    result2 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        logical_date=datetime.datetime(2024, 1, 1, 1, 1, 1),
        map_index=2,
    )
    assert result1 != result2


def test_build_task_instance_run_id_different_inputs_gives_different_results():
    result1 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        logical_date=datetime.datetime.now(),
        map_index=-1,
    )
    result2 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag2",
        task_id="task2",
        try_number=2,
        logical_date=datetime.datetime.now(),
        map_index=-1,
    )
    assert result1 != result2


@skip_if_force_lowest_dependencies_marker
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


def test_adapter_build_run():
    run_id = str(uuid.uuid4())
    result = OpenLineageAdapter._build_run(
        run_id=run_id,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        run_facets={
            "my_custom_facet": external_query_run.ExternalQueryRunFacet(
                externalQueryId="123", source="source"
            ),
            "processing_engine": "this_should_be_gone",
        },
    )
    assert result.runId == run_id
    assert result.facets == {
        "my_custom_facet": external_query_run.ExternalQueryRunFacet(externalQueryId="123", source="source"),
        "nominalTime": nominal_time_run.NominalTimeRunFacet(
            nominalStartTime="2022-01-01T00:00:00",
            nominalEndTime="2022-01-01T00:00:00",
        ),
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            version=ANY, name="Airflow", openlineageAdapterVersion=ANY
        ),
    }


def test_adapter_build_job():
    result = OpenLineageAdapter._build_job(
        job_name="job_name",
        job_type="TASK",
        job_description="job_description",
        job_owners=["def", "abc"],
        job_tags=["tag2", "tag1"],
        job_facets={
            "my_custom_facet": sql_job.SQLJobFacet(query="sql"),
            "jobType": "this_should_be_gone",
            "documentation": "this_should_be_gone",
            "ownership": "this_should_be_gone",
            "tags": "this_should_be_gone",
        },
    )
    assert result.name == "job_name"
    assert result.facets == {
        "my_custom_facet": sql_job.SQLJobFacet(query="sql"),
        "documentation": documentation_job.DocumentationJobFacet(description="job_description"),
        "ownership": ownership_job.OwnershipJobFacet(
            owners=[
                ownership_job.Owner(name="abc", type=None),
                ownership_job.Owner(name="def", type=None),
            ]
        ),
        "tags": tags_job.TagsJobFacet(
            tags=[
                tags_job.TagsJobFacetFields(key="tag1", value="tag1", source="AIRFLOW"),
                tags_job.TagsJobFacetFields(key="tag2", value="tag2", source="AIRFLOW"),
            ]
        ),
        "jobType": job_type_job.JobTypeJobFacet(
            processingType="BATCH", integration="AIRFLOW", jobType="TASK"
        ),
    }
