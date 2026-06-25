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

import copy
import datetime
from unittest import mock

import pytest
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import job_type_job, parent_run

from airflow.providers.common.compat.openlineage.facet import (
    ErrorMessageRunFacet,
    ExternalQueryRunFacet,
    SQLJobFacet,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, timezone
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.utils.openlineage import (
    _create_ol_event_pair,
    _extract_new_clusters_from_databricks_job,
    _get_parent_run_facet,
    _get_queries_details_from_databricks,
    _process_data_from_api,
    _run_api_call,
    emit_openlineage_events_for_databricks_queries,
    inject_openlineage_properties_into_databricks_job,
)
from airflow.providers.openlineage.conf import namespace
from airflow.utils.state import TaskInstanceState


def test_get_parent_run_facet():
    logical_date = timezone.datetime(2025, 1, 1)
    dr = mock.MagicMock(logical_date=logical_date, clear_number=0)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,
        dag_run=dr,
    )
    mock_ti.get_template_context.return_value = {"dag_run": dr}

    result = _get_parent_run_facet(mock_ti)

    assert result.run.runId == "01941f29-7c00-7087-8906-40e512c257bd"
    assert result.job.namespace == namespace()
    assert result.job.name == "dag_id.task_id"
    assert result.root.run.runId == "01941f29-7c00-743e-b109-28b18d0a19c5"
    assert result.root.job.namespace == namespace()
    assert result.root.job.name == "dag_id"


def test_run_api_call_success():
    mock_hook = mock.MagicMock()
    mock_hook._get_token.return_value = "mock_token"
    mock_hook.host = "mock_host"

    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"res": [{"query_id": "123", "status": "success"}]}

    with mock.patch("requests.get", return_value=mock_response):
        result = _run_api_call(mock_hook, ["123"])

    assert result == [{"query_id": "123", "status": "success"}]


def test_run_api_call_request_error():
    mock_hook = mock.MagicMock()
    mock_hook._get_token.return_value = "mock_token"
    mock_hook.host = "mock_host"

    mock_response = mock.MagicMock()
    mock_response.status_code = 200

    with mock.patch("requests.get", side_effect=RuntimeError("request error")):
        with pytest.raises(RuntimeError):
            _run_api_call(mock_hook, ["123"])


def test_run_api_call_token_error():
    mock_hook = mock.MagicMock()
    mock_hook._get_token.side_effect = RuntimeError("Token error")
    mock_hook.host = "mock_host"

    mock_response = mock.MagicMock()
    mock_response.status_code = 200

    with mock.patch("requests.get", return_value=mock_response):
        with pytest.raises(RuntimeError):
            _run_api_call(mock_hook, ["123"])


def test_process_data_from_api():
    data = [
        {
            "query_id": "ABC",
            "status": "FINISHED",
            "query_start_time_ms": 1595357086200,
            "query_end_time_ms": 1595357087200,
            "query_text": "SELECT * FROM table1;",
            "error_message": "Error occurred",
        },
        {
            "query_id": "DEF",
            "query_start_time_ms": 1595357086200,
            "query_end_time_ms": 1595357087200,
        },
    ]
    expected_details = [
        {
            "query_id": "ABC",
            "status": "FINISHED",
            "query_start_time_ms": datetime.datetime(
                2020, 7, 21, 18, 44, 46, 200000, tzinfo=datetime.timezone.utc
            ),
            "query_end_time_ms": datetime.datetime(
                2020, 7, 21, 18, 44, 47, 200000, tzinfo=datetime.timezone.utc
            ),
            "query_text": "SELECT * FROM table1;",
            "error_message": "Error occurred",
        },
        {
            "query_id": "DEF",
            "query_start_time_ms": datetime.datetime(
                2020, 7, 21, 18, 44, 46, 200000, tzinfo=datetime.timezone.utc
            ),
            "query_end_time_ms": datetime.datetime(
                2020, 7, 21, 18, 44, 47, 200000, tzinfo=datetime.timezone.utc
            ),
        },
    ]
    result = _process_data_from_api(data=data)
    assert len(result) == 2
    assert result == expected_details


def test_process_data_from_api_error():
    with pytest.raises(KeyError):
        _process_data_from_api(data=[{"query_start_time_ms": 1595357086200}])


def test_get_queries_details_from_databricks_empty_query_ids():
    details = _get_queries_details_from_databricks(None, [])
    assert details == {}


@mock.patch("airflow.providers.databricks.utils.openlineage._run_api_call")
def test_get_queries_details_from_databricks_error(mock_api_call):
    mock_api_call.side_effect = RuntimeError("Token error")

    hook = DatabricksSqlHook()
    query_ids = ["ABC"]

    details = _get_queries_details_from_databricks(hook, query_ids)
    mock_api_call.assert_called_once_with(hook=hook, query_ids=query_ids)
    assert details == {}


@mock.patch("airflow.providers.databricks.utils.openlineage._run_api_call")
def test_get_queries_details_from_databricks(mock_api_call):
    hook = DatabricksSqlHook()
    query_ids = ["ABC"]
    fake_result = [
        {
            "query_id": "ABC",
            "status": "FINISHED",
            "query_start_time_ms": 1595357086200,
            "query_end_time_ms": 1595357087200,
            "query_text": "SELECT * FROM table1;",
            "error_message": "Error occurred",
        }
    ]
    mock_api_call.return_value = fake_result

    details = _get_queries_details_from_databricks(hook, query_ids)
    mock_api_call.assert_called_once_with(hook=hook, query_ids=query_ids)
    assert details == {
        "ABC": {
            "status": "FINISHED",
            "start_time": datetime.datetime(2020, 7, 21, 18, 44, 46, 200000, tzinfo=datetime.timezone.utc),
            "end_time": datetime.datetime(2020, 7, 21, 18, 44, 47, 200000, tzinfo=datetime.timezone.utc),
            "query_text": "SELECT * FROM table1;",
            "error_message": "Error occurred",
        }
    }


@mock.patch("airflow.providers.databricks.utils.openlineage._run_api_call")
def test_get_queries_details_from_databricks_no_data_found(mock_api_call):
    hook = DatabricksSqlHook()
    query_ids = ["ABC", "DEF"]
    mock_api_call.return_value = []

    details = _get_queries_details_from_databricks(hook, query_ids)
    mock_api_call.assert_called_once_with(hook=hook, query_ids=query_ids)
    assert details == {}


@pytest.mark.parametrize("is_successful", [True, False])
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_create_ol_event_pair_success(mock_generate_uuid, is_successful):
    fake_uuid = "01941f29-7c00-7087-8906-40e512c257bd"
    mock_generate_uuid.return_value = fake_uuid

    job_namespace = "test_namespace"
    job_name = "test_job"
    start_time = timezone.datetime(2021, 1, 1, 10, 0, 0)
    end_time = timezone.datetime(2021, 1, 1, 10, 30, 0)
    run_facets = {"run_key": "run_value"}
    job_facets = {"job_key": "job_value"}

    start_event, end_event = _create_ol_event_pair(
        job_namespace,
        job_name,
        start_time,
        end_time,
        is_successful=is_successful,
        run_facets=run_facets,
        job_facets=job_facets,
    )

    assert start_event.eventType == RunState.START
    assert start_event.eventTime == start_time.isoformat()
    assert end_event.eventType == RunState.COMPLETE if is_successful else RunState.FAIL
    assert end_event.eventTime == end_time.isoformat()

    assert start_event.run.runId == fake_uuid
    assert start_event.run.facets == run_facets

    assert start_event.job.namespace == job_namespace
    assert start_event.job.name == job_name
    assert start_event.job.facets == job_facets

    assert start_event.run is end_event.run
    assert start_event.job == end_event.job


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries(mock_generate_uuid, mock_version, time_machine):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    query_ids = ["query1", "query2", "query3"]
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_dagrun = mock.MagicMock(logical_date=logical_date, clear_number=0)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.FAILED,  # This will be query default state if no metadata found
        dag_run=mock_dagrun,
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock_dagrun}

    fake_metadata = {
        "query1": {
            "status": "FINISHED",
            "start_time": datetime.datetime(2020, 7, 21, 18, 44, 46, 200000, tzinfo=datetime.timezone.utc),
            "end_time": datetime.datetime(2020, 7, 21, 18, 44, 47, 200000, tzinfo=datetime.timezone.utc),
            "query_text": "SELECT * FROM table1",
            # No error for query1
        },
        "query2": {
            "status": "CANCELED",
            "start_time": datetime.datetime(2020, 7, 21, 18, 44, 48, 200000, tzinfo=datetime.timezone.utc),
            "end_time": datetime.datetime(2020, 7, 21, 18, 44, 49, 200000, tzinfo=datetime.timezone.utc),
            "query_text": "SELECT * FROM table2",
            "error_message": "Error occurred",
        },
        # No metadata for query3
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with (
        mock.patch(
            "airflow.providers.databricks.utils.openlineage._get_queries_details_from_databricks",
            return_value=fake_metadata,
        ),
        mock.patch(
            "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
            return_value=fake_listener,
        ),
    ):
        emit_openlineage_events_for_databricks_queries(
            query_ids=query_ids,
            query_source_namespace="databricks_ns",
            task_instance=mock_ti,
            hook=mock.MagicMock(),
            query_for_extra_metadata=True,
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 6  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event
                RunEvent(
                    eventTime="2020-07-21T18:44:46.200000+00:00",
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets={
                            "sql": SQLJobFacet(query="SELECT * FROM table1"),
                            **expected_common_job_facets,
                        },
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event
                RunEvent(
                    eventTime="2020-07-21T18:44:47.200000+00:00",
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets={
                            "sql": SQLJobFacet(query="SELECT * FROM table1"),
                            **expected_common_job_facets,
                        },
                    ),
                )
            ),
            mock.call(  # Query2: START event
                RunEvent(
                    eventTime="2020-07-21T18:44:48.200000+00:00",
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query2", source="databricks_ns"
                            ),
                            "error": ErrorMessageRunFacet(
                                message="Error occurred", programmingLanguage="SQL"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.2",
                        facets={
                            "sql": SQLJobFacet(query="SELECT * FROM table2"),
                            **expected_common_job_facets,
                        },
                    ),
                )
            ),
            mock.call(  # Query2: FAIL event
                RunEvent(
                    eventTime="2020-07-21T18:44:49.200000+00:00",
                    eventType=RunState.FAIL,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query2", source="databricks_ns"
                            ),
                            "error": ErrorMessageRunFacet(
                                message="Error occurred", programmingLanguage="SQL"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.2",
                        facets={
                            "sql": SQLJobFacet(query="SELECT * FROM table2"),
                            **expected_common_job_facets,
                        },
                    ),
                )
            ),
            mock.call(  # Query3: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),  # no metadata for query3
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query3", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.3",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query3: FAIL event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),  # no metadata for query3
                    eventType=RunState.FAIL,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query3", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.3",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries_without_metadata(
    mock_generate_uuid, mock_version, time_machine
):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    query_ids = ["query1"]
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,  # This will be query default state if no metadata found
        dag_run=mock.MagicMock(logical_date=logical_date, clear_number=0),
    )
    mock_ti.get_template_context.return_value = {
        "dag_run": mock.MagicMock(logical_date=logical_date, clear_number=0)
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_databricks_queries(
            query_ids=query_ids,
            query_source_namespace="databricks_ns",
            task_instance=mock_ti,
            hook=mock.MagicMock(),
            # query_for_extra_metadata=False,  # False by default
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries_without_explicit_query_ids(
    mock_generate_uuid, mock_version, time_machine
):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    query_ids = ["query1"]
    hook = mock.MagicMock()
    hook.query_ids = query_ids
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.RUNNING,  # This will be query default state if no metadata found
        dag_run=mock.MagicMock(logical_date=logical_date, clear_number=0),
    )
    mock_ti.get_template_context.return_value = {
        "dag_run": mock.MagicMock(logical_date=logical_date, clear_number=0)
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_databricks_queries(
            query_source_namespace="databricks_ns",
            task_instance=mock_ti,
            hook=hook,
            # query_for_extra_metadata=False,  # False by default
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch(
    "airflow.providers.openlineage.sqlparser.SQLParser.create_namespace", return_value="databricks_ns"
)
@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries_without_explicit_query_ids_and_namespace(
    mock_generate_uuid, mock_version, mock_parser, time_machine
):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    query_ids = ["query1"]
    hook = mock.MagicMock()
    hook.query_ids = query_ids
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.RUNNING,  # This will be query default state if no metadata found
        dag_run=mock.MagicMock(logical_date=logical_date, clear_number=0),
    )
    mock_ti.get_template_context.return_value = {
        "dag_run": mock.MagicMock(logical_date=logical_date, clear_number=0)
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_databricks_queries(
            task_instance=mock_ti,
            hook=hook,
            # query_for_extra_metadata=False,  # False by default
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries_without_explicit_query_ids_and_namespace_raw_ns(
    mock_generate_uuid, mock_version, time_machine
):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    query_ids = ["query1"]
    hook = DatabricksHook()
    hook.query_ids = query_ids
    hook.host = "some_host"
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.RUNNING,  # This will be query default state if no metadata found
        dag_run=mock.MagicMock(logical_date=logical_date, clear_number=0),
    )
    mock_ti.get_template_context.return_value = {
        "dag_run": mock.MagicMock(logical_date=logical_date, clear_number=0)
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_databricks_queries(
            task_instance=mock_ti,
            hook=hook,
            # query_for_extra_metadata=False,  # False by default
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks://some_host"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks://some_host"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_emit_openlineage_events_for_databricks_queries_ith_query_ids_and_hook_query_ids(
    mock_generate_uuid, mock_version, time_machine
):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    time_machine.move_to(default_event_time, tick=False)

    hook = DatabricksSqlHook()
    hook.query_ids = ["query2", "query3"]
    query_ids = ["query1"]
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,  # This will be query default state if no metadata found
        dag_run=mock.MagicMock(logical_date=logical_date, clear_number=0),
    )
    mock_ti.get_template_context.return_value = {
        "dag_run": mock.MagicMock(logical_date=logical_date, clear_number=0)
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_databricks_queries(
            query_ids=query_ids,
            query_source_namespace="databricks_ns",
            task_instance=mock_ti,
            hook=hook,
            # query_for_extra_metadata=False,  # False by default
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_adapter.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="DATABRICKS",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId="01941f29-7c00-743e-b109-28b18d0a19c5"),
                    job=parent_run.RootJob(namespace=namespace(), name="dag_id"),
                ),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
            mock.call(  # Query1: COMPLETE event (no metadata)
                RunEvent(
                    eventTime=default_event_time.isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="databricks_ns"
                            ),
                            **expected_common_run_facets,
                        },
                    ),
                    job=Job(
                        namespace=namespace(),
                        name="dag_id.task_id.query.1",
                        facets=expected_common_job_facets,
                    ),
                )
            ),
        ]

        assert fake_adapter.emit.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_databricks_queries_missing_query_ids_and_hook(mock_version):
    query_ids = []
    original_query_ids = copy.deepcopy(query_ids)

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        with pytest.raises(ValueError, match="If 'hook' is not provided, 'query_ids' must be set."):
            emit_openlineage_events_for_databricks_queries(
                query_ids=query_ids,
                query_source_namespace="databricks_ns",
                task_instance=None,
            )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_databricks_queries_missing_query_namespace_and_hook(mock_version):
    query_ids = ["1", "2"]
    original_query_ids = copy.deepcopy(query_ids)

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        with pytest.raises(
            ValueError, match="If 'hook' is not provided, 'query_source_namespace' must be set."
        ):
            emit_openlineage_events_for_databricks_queries(
                query_ids=query_ids,
                task_instance=None,
            )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_databricks_queries_missing_hook_and_query_for_extra_metadata_true(
    mock_version,
):
    query_ids = ["1", "2"]
    original_query_ids = copy.deepcopy(query_ids)

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        with pytest.raises(
            ValueError, match="If 'hook' is not provided, 'query_for_extra_metadata' must be False."
        ):
            emit_openlineage_events_for_databricks_queries(
                query_ids=query_ids,
                query_source_namespace="databricks_ns",
                task_instance=None,
                query_for_extra_metadata=True,
            )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted


@mock.patch("importlib.metadata.version", return_value="1.99.0")
def test_emit_openlineage_events_with_old_openlineage_provider(mock_version):
    query_ids = ["q1", "q2"]
    original_query_ids = copy.deepcopy(query_ids)

    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        expected_err = (
            "OpenLineage provider version `1.99.0` is lower than required `2.5.0`, "
            "skipping function `emit_openlineage_events_for_databricks_queries` execution"
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
            emit_openlineage_events_for_databricks_queries(
                query_ids=query_ids,
                query_source_namespace="databricks_ns",
                task_instance=None,
            )
        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted


OL_UTILS = "airflow.providers.databricks.utils.openlineage"


def test_extract_new_clusters_from_databricks_job():
    top_cluster = {"spark_version": "13.3.x-scala2.12"}
    task_cluster = {"spark_version": "14.3.x-scala2.12"}
    job_cluster = {"spark_version": "15.4.x-scala2.12"}
    for_each_cluster = {"spark_version": "16.4.x-scala2.12"}
    job = {
        "new_cluster": top_cluster,
        "tasks": [
            {"task_key": "a", "new_cluster": task_cluster},
            {"task_key": "b", "existing_cluster_id": "existing"},
            {"task_key": "c", "job_cluster_key": "shared"},
            {
                "task_key": "d",
                "for_each_task": {"task": {"task_key": "d/iter", "new_cluster": for_each_cluster}},
            },
        ],
        "job_clusters": [{"job_cluster_key": "shared", "new_cluster": job_cluster}],
    }
    assert _extract_new_clusters_from_databricks_job(job) == [
        top_cluster,
        task_cluster,
        job_cluster,
        for_each_cluster,
    ]


def test_extract_new_clusters_from_databricks_job_existing_cluster_only():
    assert _extract_new_clusters_from_databricks_job({"existing_cluster_id": "existing"}) == []


@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_disabled_returns_job_unchanged(mock_accessible):
    job = {"new_cluster": {"spark_conf": {}}}
    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=False, inject_transport_info=False
    )
    assert result == job
    mock_accessible.assert_not_called()


@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=False)
def test_inject_openlineage_properties_provider_inaccessible_returns_job_unchanged(mock_accessible):
    job = {"new_cluster": {"spark_conf": {}}}
    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=True
    )
    assert result == job


@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_no_new_cluster_returns_job_unchanged(mock_accessible):
    job = {"existing_cluster_id": "existing"}
    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=True
    )
    assert result == job


@mock.patch(f"{OL_UTILS}.inject_transport_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}.inject_parent_job_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_into_top_level_new_cluster(
    mock_accessible, mock_parent, mock_transport
):
    mock_parent.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.parentJobName": "dag_id.task_id",
    }
    mock_transport.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.transport.type": "http",
    }
    job = {"new_cluster": {"spark_conf": {"spark.executor.memory": "8g"}}, "notebook_task": {"x": "y"}}

    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=True
    )

    spark_conf = result["new_cluster"]["spark_conf"]
    assert spark_conf["spark.executor.memory"] == "8g"
    assert spark_conf["spark.openlineage.parentJobName"] == "dag_id.task_id"
    assert spark_conf["spark.openlineage.transport.type"] == "http"


@mock.patch(f"{OL_UTILS}.inject_parent_job_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_into_multi_task_new_clusters(mock_accessible, mock_parent):
    mock_parent.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.parentJobName": "dag_id.task_id",
    }
    job = {
        "tasks": [
            {"task_key": "a", "new_cluster": {"spark_conf": {}}},
            {"task_key": "b", "new_cluster": {"spark_conf": {}}},
            {"task_key": "c", "existing_cluster_id": "existing"},
        ]
    }

    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=False
    )

    assert result["tasks"][0]["new_cluster"]["spark_conf"]["spark.openlineage.parentJobName"] == (
        "dag_id.task_id"
    )
    assert result["tasks"][1]["new_cluster"]["spark_conf"]["spark.openlineage.parentJobName"] == (
        "dag_id.task_id"
    )
    assert result["tasks"][2] == {"task_key": "c", "existing_cluster_id": "existing"}


@mock.patch(f"{OL_UTILS}.inject_parent_job_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_into_shared_job_clusters(mock_accessible, mock_parent):
    mock_parent.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.parentJobName": "dag_id.task_id",
    }
    job = {
        "tasks": [{"task_key": "a", "job_cluster_key": "shared"}],
        "job_clusters": [{"job_cluster_key": "shared", "new_cluster": {"spark_conf": {}}}],
    }

    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=False
    )

    injected = result["job_clusters"][0]["new_cluster"]["spark_conf"]["spark.openlineage.parentJobName"]
    assert injected == "dag_id.task_id"


@mock.patch(f"{OL_UTILS}.inject_parent_job_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_into_for_each_task_new_cluster(mock_accessible, mock_parent):
    mock_parent.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.parentJobName": "dag_id.task_id",
    }
    job = {
        "tasks": [
            {
                "task_key": "a",
                "for_each_task": {"task": {"task_key": "a/iter", "new_cluster": {"spark_conf": {}}}},
            }
        ]
    }

    result = inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=False
    )

    injected = result["tasks"][0]["for_each_task"]["task"]["new_cluster"]["spark_conf"]
    assert injected["spark.openlineage.parentJobName"] == "dag_id.task_id"


@mock.patch(f"{OL_UTILS}.inject_parent_job_information_into_spark_properties")
@mock.patch(f"{OL_UTILS}._is_openlineage_provider_accessible", return_value=True)
def test_inject_openlineage_properties_does_not_mutate_input(mock_accessible, mock_parent):
    mock_parent.side_effect = lambda properties, context: {
        **properties,
        "spark.openlineage.parentJobName": "dag_id.task_id",
    }
    job = {"new_cluster": {"spark_conf": {"spark.executor.memory": "8g"}}}
    original = copy.deepcopy(job)

    inject_openlineage_properties_into_databricks_job(
        job, context=mock.MagicMock(), inject_parent_job_info=True, inject_transport_info=False
    )

    assert job == original
