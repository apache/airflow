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
from unittest import mock

import pytest
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import job_type_job, parent_run

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.openlineage.facet import (
    ErrorMessageRunFacet,
    ExternalQueryRunFacet,
    SQLJobFacet,
)
from airflow.providers.openlineage.conf import namespace
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.openlineage import (
    _create_snowflake_event_pair,
    _get_ol_run_id,
    _get_parent_run_facet,
    _get_queries_details_from_snowflake,
    _run_single_query_with_hook,
    emit_openlineage_events_for_snowflake_queries,
    fix_account_name,
    fix_snowflake_sqlalchemy_uri,
)
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS


@pytest.mark.parametrize(
    "source,target",
    [
        (
            "snowflake://user:pass@xy123456.us-east-1.aws/database/schema",
            "snowflake://xy123456.us-east-1.aws/database/schema",
        ),
        (
            "snowflake://xy123456/database/schema",
            "snowflake://xy123456.us-west-1.aws/database/schema",
        ),
        (
            "snowflake://xy12345.ap-southeast-1/database/schema",
            "snowflake://xy12345.ap-southeast-1.aws/database/schema",
        ),
        (
            "snowflake://user:pass@xy12345.south-central-us.azure/database/schema",
            "snowflake://xy12345.south-central-us.azure/database/schema",
        ),
        (
            "snowflake://user:pass@xy12345.us-east4.gcp/database/schema",
            "snowflake://xy12345.us-east4.gcp/database/schema",
        ),
        (
            "snowflake://user:pass@organization-account/database/schema",
            "snowflake://organization-account/database/schema",
        ),
        (
            "snowflake://user:p[ass@organization-account/database/schema",
            "snowflake://organization-account/database/schema",
        ),
        (
            "snowflake://user:pass@organization]-account/database/schema",
            "snowflake://organization%5D-account/database/schema",
        ),
    ],
)
def test_snowflake_sqlite_account_urls(source, target):
    assert fix_snowflake_sqlalchemy_uri(source) == target


# Unit Tests using pytest.mark.parametrize
@pytest.mark.parametrize(
    "name, expected",
    [
        ("xy12345", "xy12345.us-west-1.aws"),  # No '-' or '_' in name
        ("xy12345.us-west-1.aws", "xy12345.us-west-1.aws"),  # Already complete locator
        ("xy12345.us-west-2.gcp", "xy12345.us-west-2.gcp"),  # Already complete locator for GCP
        ("xy12345aws", "xy12345aws.us-west-1.aws"),  # AWS without '-' or '_'
        ("xy12345-aws", "xy12345-aws"),  # AWS with '-'
        ("xy12345_gcp-europe-west1", "xy12345.europe-west1.gcp"),  # GCP with '_'
        ("myaccount_gcp-asia-east1", "myaccount.asia-east1.gcp"),  # GCP with region and '_'
        ("myaccount_azure-eastus", "myaccount.eastus.azure"),  # Azure with region
        ("myorganization-1234", "myorganization-1234"),  # No change needed
        ("my.organization", "my.organization.us-west-1.aws"),  # Dot in name
    ],
)
def test_fix_account_name(name, expected):
    assert fix_account_name(name) == expected
    assert (
        fix_snowflake_sqlalchemy_uri(f"snowflake://{name}/database/schema")
        == f"snowflake://{expected}/database/schema"
    )


def test_get_ol_run_id_ti_success():
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1 if AIRFLOW_V_2_10_PLUS else 2,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock.MagicMock(logical_date=logical_date)}

    result = _get_ol_run_id(mock_ti)
    assert result == "01941f29-7c00-7087-8906-40e512c257bd"


def test_get_ol_run_id_ti_failed():
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.FAILED,
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock.MagicMock(logical_date=logical_date)}

    result = _get_ol_run_id(mock_ti)
    assert result == "01941f29-7c00-7087-8906-40e512c257bd"


def test_get_parent_run_facet():
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1 if AIRFLOW_V_2_10_PLUS else 2,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock.MagicMock(logical_date=logical_date)}

    result = _get_parent_run_facet(mock_ti)

    assert result.run.runId == "01941f29-7c00-7087-8906-40e512c257bd"
    assert result.job.namespace == namespace()
    assert result.job.name == "dag_id.task_id"


@mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.get_conn")
@mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.set_autocommit")
@mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_cursor")
def test_run_single_query_with_hook(mock_get_cursor, mock_set_autocommit, mock_get_conn):
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchall.return_value = [{"col1": "value1"}, {"col2": "value2"}]
    mock_get_cursor.return_value.__enter__.return_value = mock_cursor
    hook = SnowflakeHook(snowflake_conn_id="test_conn")

    sql_query = "SELECT * FROM test_table;"
    result = _run_single_query_with_hook(hook, sql_query)

    mock_cursor.execute.assert_called_once_with(sql_query)
    assert result == [{"col1": "value1"}, {"col2": "value2"}]


def test_get_queries_details_from_snowflake_empty_query_ids():
    details = _get_queries_details_from_snowflake(None, [])
    assert details == {}


@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_single_query(mock_run_single_query):
    hook = mock.MagicMock()
    query_ids = ["ABC"]
    fake_result = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": timezone.datetime(2025, 1, 1),
            "END_TIME": timezone.datetime(2025, 1, 1),
            "QUERY_TEXT": "SELECT * FROM test_table;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        }
    ]
    mock_run_single_query.return_value = fake_result

    details = _get_queries_details_from_snowflake(hook, query_ids)
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        "FROM table(information_schema.query_history()) "
        "WHERE QUERY_ID = 'ABC';"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {"ABC": fake_result[0]}


@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_multiple_queries(mock_run_single_query):
    hook = mock.MagicMock()
    query_ids = ["ABC", "DEF"]
    fake_result = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": timezone.datetime(2025, 1, 1),
            "END_TIME": timezone.datetime(2025, 1, 1),
            "QUERY_TEXT": "SELECT * FROM table1;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        },
        {
            "QUERY_ID": "DEF",
            "EXECUTION_STATUS": "FAILED",
            "START_TIME": timezone.datetime(2025, 1, 1),
            "END_TIME": timezone.datetime(2025, 1, 1),
            "QUERY_TEXT": "SELECT * FROM table2;",
            "ERROR_CODE": "123",
            "ERROR_MESSAGE": "Some error",
        },
    ]
    mock_run_single_query.return_value = fake_result

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        "FROM table(information_schema.query_history()) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {row["QUERY_ID"]: row for row in fake_result}


@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_no_data_found(mock_run_single_query):
    hook = mock.MagicMock()
    query_ids = ["ABC", "DEF"]
    mock_run_single_query.return_value = []

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        "FROM table(information_schema.query_history()) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {}


@pytest.mark.parametrize("is_successful", [True, False])
@mock.patch("openlineage.client.uuid.generate_new_uuid")
def test_create_snowflake_event_pair_success(mock_generate_uuid, is_successful):
    fake_uuid = "01941f29-7c00-7087-8906-40e512c257bd"
    mock_generate_uuid.return_value = fake_uuid

    job_namespace = "test_namespace"
    job_name = "test_job"
    start_time = timezone.datetime(2021, 1, 1, 10, 0, 0)
    end_time = timezone.datetime(2021, 1, 1, 10, 30, 0)
    run_facets = {"run_key": "run_value"}
    job_facets = {"job_key": "job_value"}

    start_event, end_event = _create_snowflake_event_pair(
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


@mock.patch("openlineage.client.uuid.generate_new_uuid")
@mock.patch("airflow.utils.timezone.utcnow")
def test_emit_openlineage_events_for_snowflake_queries_with_hook(mock_now, mock_generate_uuid):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    mock_now.return_value = default_event_time

    query_ids = ["query1", "query2", "query3"]
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1,
        logical_date=logical_date,
        state=TaskInstanceState.FAILED,  # This will be query default state if no metadata found
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock.MagicMock(logical_date=logical_date)}

    fake_metadata = {
        "query1": {
            "START_TIME": timezone.datetime(2025, 1, 1, 0, 0, 0),
            "END_TIME": timezone.datetime(2025, 1, 2, 0, 0, 0),
            "EXECUTION_STATUS": "SUCCESS",
            "QUERY_TEXT": "SELECT * FROM table1",
            # No error for query1
        },
        "query2": {
            "START_TIME": timezone.datetime(2025, 1, 3, 0, 0, 0),
            "END_TIME": timezone.datetime(2025, 1, 4, 0, 0, 0),
            "EXECUTION_STATUS": "FAIL",
            "QUERY_TEXT": "SELECT * FROM table2",
            "ERROR_MESSAGE": "Error occurred",
            "ERROR_CODE": "ERR001",
        },
        # No metadata for query3
    }

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_client = mock.MagicMock()
    fake_client.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter.get_or_create_openlineage_client.return_value = fake_client

    with (
        mock.patch(
            "airflow.providers.snowflake.utils.openlineage._get_queries_details_from_snowflake",
            return_value=fake_metadata,
        ),
        mock.patch(
            "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
            return_value=fake_listener,
        ),
    ):
        emit_openlineage_events_for_snowflake_queries(
            query_ids=query_ids,
            query_source_namespace="snowflake_ns",
            task_instance=mock_ti,
            hook=mock.MagicMock(),  # any non-None hook to trigger metadata retrieval
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_client.emit.call_count == 6  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="SNOWFLAKE",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
            ),
            "custom_run": "value_run",
        }

        expected_calls = [
            mock.call(  # Query1: START event
                RunEvent(
                    eventTime=fake_metadata["query1"]["START_TIME"].isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="snowflake_ns"
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
                    eventTime=fake_metadata["query1"]["END_TIME"].isoformat(),
                    eventType=RunState.COMPLETE,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query1", source="snowflake_ns"
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
                    eventTime=fake_metadata["query2"]["START_TIME"].isoformat(),
                    eventType=RunState.START,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query2", source="snowflake_ns"
                            ),
                            "error": ErrorMessageRunFacet(
                                message="ERR001 : Error occurred", programmingLanguage="SQL"
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
                    eventTime=fake_metadata["query2"]["END_TIME"].isoformat(),
                    eventType=RunState.FAIL,
                    run=Run(
                        runId=fake_uuid,
                        facets={
                            "externalQuery": ExternalQueryRunFacet(
                                externalQueryId="query2", source="snowflake_ns"
                            ),
                            "error": ErrorMessageRunFacet(
                                message="ERR001 : Error occurred", programmingLanguage="SQL"
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
                                externalQueryId="query3", source="snowflake_ns"
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
                                externalQueryId="query3", source="snowflake_ns"
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

        assert fake_client.emit.call_args_list == expected_calls


@mock.patch("openlineage.client.uuid.generate_new_uuid")
@mock.patch("airflow.utils.timezone.utcnow")
def test_emit_openlineage_events_for_snowflake_queries_without_hook(mock_now, mock_generate_uuid):
    fake_uuid = "01958e68-03a2-79e3-9ae9-26865cc40e2f"
    mock_generate_uuid.return_value = fake_uuid

    default_event_time = timezone.datetime(2025, 1, 5, 0, 0, 0)
    mock_now.return_value = default_event_time

    query_ids = ["query1"]
    original_query_ids = copy.deepcopy(query_ids)
    logical_date = timezone.datetime(2025, 1, 1)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        map_index=1,
        try_number=1 if AIRFLOW_V_2_10_PLUS else 2,
        logical_date=logical_date,
        state=TaskInstanceState.SUCCESS,  # This will be query default state if no metadata found
    )
    mock_ti.get_template_context.return_value = {"dag_run": mock.MagicMock(logical_date=logical_date)}

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    fake_client = mock.MagicMock()
    fake_client.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter.get_or_create_openlineage_client.return_value = fake_client

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_snowflake_queries(
            query_ids=query_ids,
            query_source_namespace="snowflake_ns",
            task_instance=mock_ti,
            hook=None,  # None so metadata retrieval is not triggered
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        assert fake_client.emit.call_count == 2  # Expect two events per query.

        expected_common_job_facets = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="QUERY",
                processingType="BATCH",
                integration="SNOWFLAKE",
            ),
            "custom_job": "value_job",
        }
        expected_common_run_facets = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId="01941f29-7c00-7087-8906-40e512c257bd"),
                job=parent_run.Job(namespace=namespace(), name="dag_id.task_id"),
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
                                externalQueryId="query1", source="snowflake_ns"
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
                                externalQueryId="query1", source="snowflake_ns"
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

        assert fake_client.emit.call_args_list == expected_calls


def test_emit_openlineage_events_for_snowflake_queries_without_query_ids():
    query_ids = []
    original_query_ids = copy.deepcopy(query_ids)

    fake_client = mock.MagicMock()
    fake_client.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter.get_or_create_openlineage_client.return_value = fake_client

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        emit_openlineage_events_for_snowflake_queries(
            query_ids=query_ids,
            query_source_namespace="snowflake_ns",
            task_instance=None,
        )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_client.emit.assert_not_called()  # No events should be emitted


# emit_openlineage_events_for_snowflake_queries requires OL provider 2.0.0
@mock.patch("importlib.metadata.version", return_value="1.99.0")
def test_emit_openlineage_events_with_old_openlineage_provider(mock_version):
    query_ids = ["q1", "q2"]
    original_query_ids = copy.deepcopy(query_ids)

    fake_client = mock.MagicMock()
    fake_client.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter.get_or_create_openlineage_client.return_value = fake_client

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        expected_err = (
            "OpenLineage provider version `1.99.0` is lower than required `2.0.0`, "
            "skipping function `emit_openlineage_events_for_snowflake_queries` execution"
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
            emit_openlineage_events_for_snowflake_queries(
                query_ids=query_ids,
                query_source_namespace="snowflake_ns",
                task_instance=None,
            )
        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_client.emit.assert_not_called()  # No events should be emitted
