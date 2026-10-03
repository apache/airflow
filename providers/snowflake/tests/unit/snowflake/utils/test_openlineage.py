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
import time_machine

from airflow.providers.common.compat.openlineage.facet import SQLJobFacet
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, timezone
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.providers.snowflake.utils.openlineage import (
    _get_queries_details_from_snowflake,
    _process_data_from_api,
    _run_single_query_with_api_hook,
    _run_single_query_with_hook,
    emit_openlineage_events_for_snowflake_queries,
    fix_account_name,
    fix_snowflake_sqlalchemy_uri,
)
from airflow.utils.state import TaskInstanceState

FROZEN_NOW = timezone.datetime(2025, 1, 1, 12, 0, 0)
_EXPECTED_RANGE_START = (FROZEN_NOW - datetime.timedelta(minutes=5)).isoformat()
_EXPECTED_RANGE_END = (FROZEN_NOW + datetime.timedelta(minutes=5)).isoformat()
_EXPECTED_TIME_FILTER = (
    f"end_time_range_start=>to_timestamp_tz('{_EXPECTED_RANGE_START}'), "
    f"end_time_range_end=>to_timestamp_tz('{_EXPECTED_RANGE_END}'), "
    "result_limit=>10000"
)


@pytest.mark.parametrize(
    ("source", "target"),
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
    ("name", "expected"),
    [
        ("xy12345", "xy12345.us-west-1.aws"),  # No '-' or '_' in name
        ("xy12345.us-west-1.aws", "xy12345.us-west-1.aws"),  # Already complete locator
        ("xy12345.us-west-2.gcp", "xy12345.us-west-2.gcp"),  # Already complete locator for GCP
        ("xy12345.us-west-2.gcp.us-west-2.gcp", "xy12345.us-west-2.gcp"),  # Duplicated region
        ("xy12345.us-west-2.gcp.us-west-2.gcp.us-west-2.gcp", "xy12345.us-west-2.gcp"),  # Triple region
        ("xy12345.us-west-2.gcp.some_random_part", "xy12345.us-west-2.gcp"),  # Suffix to locator, ignored
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


def test_process_data_from_api():
    data = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": "1750245171.326000",
            "END_TIME": 1750245171.387000,
            "QUERY_TEXT": "SELECT * FROM test_table;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        },
        {
            "START_TIME": 1750245171.326000,
            "END_TIME": "1750245171.387000",
        },
    ]
    expected_details = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 326000, tzinfo=datetime.timezone.utc),
            "END_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 387000, tzinfo=datetime.timezone.utc),
            "QUERY_TEXT": "SELECT * FROM test_table;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        },
        {
            "START_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 326000, tzinfo=datetime.timezone.utc),
            "END_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 387000, tzinfo=datetime.timezone.utc),
        },
    ]
    result = _process_data_from_api(data=data)
    assert len(result) == 2
    assert result == expected_details


def test_process_data_from_api_error():
    with pytest.raises(KeyError):
        _process_data_from_api(data=[{"START_TIME": "1750245171.326000"}])


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

    mock_cursor.execute.assert_has_calls(
        [mock.call("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3;"), mock.call(sql_query)]
    )
    assert result == [{"col1": "value1"}, {"col2": "value2"}]


@mock.patch(
    "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_result_from_successful_sql_api_query"
)
@mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.wait_for_query")
@mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.execute_query")
def test_run_single_query_with_api_hook_success(mock_execute, mock_wait, mock_get_result):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    hook.query_ids = ["old-id"]

    # Simulate that execute_query overwrites hook.query_ids
    def execute_query_side_effect(*args, **kwargs):
        hook.query_ids = ["overwritten-id"]
        return ["new-id"]

    mock_execute.side_effect = execute_query_side_effect
    mock_get_result.return_value = [{"col": "value"}]

    result = _run_single_query_with_api_hook(hook, "SELECT 1")

    assert result == [{"col": "value"}]
    mock_execute.assert_called_once_with(sql="SELECT 1", statement_count=0)
    mock_wait.assert_called_once_with(query_id="new-id", raise_error=True, poll_interval=1, timeout=3)
    mock_get_result.assert_called_once_with(query_id="new-id")
    assert hook.query_ids == ["old-id"]


@mock.patch(
    "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.get_result_from_successful_sql_api_query"
)
@mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.wait_for_query")
@mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.execute_query")
def test_run_single_query_exception_restores_query_ids(mock_execute, mock_wait, mock_get_result):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    hook.query_ids = ["persistent-id"]

    # Simulate that execute_query overwrites hook.query_ids
    def execute_query_side_effect(*args, **kwargs):
        hook.query_ids = []
        return ["new-id"]

    mock_execute.side_effect = execute_query_side_effect
    mock_wait.side_effect = RuntimeError("execution failed")

    with pytest.raises(RuntimeError, match="execution failed"):
        _run_single_query_with_api_hook(hook, "SELECT 1")

    assert hook.query_ids == ["persistent-id"]
    mock_execute.assert_called_once_with(sql="SELECT 1", statement_count=0)
    mock_wait.assert_called_once_with(query_id="new-id", raise_error=True, poll_interval=1, timeout=3)
    mock_get_result.assert_not_called()


def test_get_queries_details_from_snowflake_empty_query_ids():
    details = _get_queries_details_from_snowflake(None, [])
    assert details == {}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_single_query(mock_run_single_query):
    hook = SnowflakeHook(snowflake_conn_id="test_conn")
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
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        "WHERE QUERY_ID = 'ABC';"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {"ABC": fake_result[0]}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_api_hook")
def test_get_queries_details_from_snowflake_single_query_api_hook(mock_run_single_query_api):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC"]
    fake_result = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": "1750245171.326000",
            "END_TIME": "1750245171.387000",
            "QUERY_TEXT": "SELECT * FROM test_table;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        }
    ]
    mock_run_single_query_api.return_value = fake_result

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        "WHERE QUERY_ID = 'ABC';"
    )
    expected_details = {
        "QUERY_ID": "ABC",
        "EXECUTION_STATUS": "SUCCESS",
        "START_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 326000, tzinfo=datetime.timezone.utc),
        "END_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 387000, tzinfo=datetime.timezone.utc),
        "QUERY_TEXT": "SELECT * FROM test_table;",
        "ERROR_CODE": None,
        "ERROR_MESSAGE": None,
    }
    mock_run_single_query_api.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {"ABC": expected_details}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_multiple_queries(mock_run_single_query):
    hook = SnowflakeHook(snowflake_conn_id="test_conn")
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
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {row["QUERY_ID"]: row for row in fake_result}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_api_hook")
def test_get_queries_details_from_snowflake_multiple_queries_api_hook(mock_run_single_query_api):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    fake_result = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": "1750245171.326000",
            "END_TIME": "1750245171.387000",
            "QUERY_TEXT": "SELECT * FROM table1;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        },
        {
            "QUERY_ID": "DEF",
            "EXECUTION_STATUS": "FAILED",
            "START_TIME": "1750245171.326000",
            "END_TIME": "1750245171.387000",
            "QUERY_TEXT": "SELECT * FROM table2;",
            "ERROR_CODE": "123",
            "ERROR_MESSAGE": "Some error",
        },
    ]
    mock_run_single_query_api.return_value = fake_result

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    expected_details = [
        {
            "QUERY_ID": "ABC",
            "EXECUTION_STATUS": "SUCCESS",
            "START_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 326000, tzinfo=datetime.timezone.utc),
            "END_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 387000, tzinfo=datetime.timezone.utc),
            "QUERY_TEXT": "SELECT * FROM table1;",
            "ERROR_CODE": None,
            "ERROR_MESSAGE": None,
        },
        {
            "QUERY_ID": "DEF",
            "EXECUTION_STATUS": "FAILED",
            "START_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 326000, tzinfo=datetime.timezone.utc),
            "END_TIME": datetime.datetime(2025, 6, 18, 11, 12, 51, 387000, tzinfo=datetime.timezone.utc),
            "QUERY_TEXT": "SELECT * FROM table2;",
            "ERROR_CODE": "123",
            "ERROR_MESSAGE": "Some error",
        },
    ]
    mock_run_single_query_api.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {row["QUERY_ID"]: row for row in expected_details}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_no_data_found(mock_run_single_query):
    hook = SnowflakeHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    mock_run_single_query.return_value = []

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_api_hook")
def test_get_queries_details_from_snowflake_no_data_found_api_hook(mock_run_single_query_api):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    mock_run_single_query_api.return_value = []

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query_api.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_hook")
def test_get_queries_details_from_snowflake_error(mock_run_single_query):
    hook = SnowflakeHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    mock_run_single_query.side_effect = ValueError("Query failure")

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_api_hook")
def test_get_queries_details_from_snowflake_error_api_hook(mock_run_single_query_api):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    mock_run_single_query_api.side_effect = ValueError("Query failure")

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query_api.assert_called_once_with(hook=hook, sql=expected_query)
    assert details == {}


@time_machine.travel(FROZEN_NOW, tick=False)
@mock.patch("airflow.providers.snowflake.utils.openlineage._process_data_from_api")
@mock.patch("airflow.providers.snowflake.utils.openlineage._run_single_query_with_api_hook")
def test_get_queries_details_from_snowflake_error_api_hook_process_data(
    mock_run_single_query_api, mock_process_data
):
    hook = SnowflakeSqlApiHook(snowflake_conn_id="test_conn")
    query_ids = ["ABC", "DEF"]
    mock_run_single_query_api.return_value = ["some_data"]
    mock_process_data.side_effect = ValueError("Processing failure")

    details = _get_queries_details_from_snowflake(hook, query_ids)

    expected_query_condition = f"IN {tuple(query_ids)}"
    expected_query = (
        "SELECT QUERY_ID, EXECUTION_STATUS, START_TIME, END_TIME, QUERY_TEXT, ERROR_CODE, ERROR_MESSAGE "
        f"FROM table(snowflake.information_schema.query_history({_EXPECTED_TIME_FILTER})) "
        f"WHERE QUERY_ID {expected_query_condition};"
    )
    mock_run_single_query_api.assert_called_once_with(hook=hook, sql=expected_query)
    mock_process_data.assert_called_once_with(data=["some_data"])
    assert details == {}


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("airflow.providers.openlineage.api.emit_query_lineage")
@time_machine.travel(FROZEN_NOW, tick=False)
def test_emit_openlineage_events_for_snowflake_queries_with_extra_metadata(
    mock_emit_query_lineage, mock_version
):
    query_ids = ["query1", "query2", "query3"]
    original_query_ids = copy.deepcopy(query_ids)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        state=TaskInstanceState.FAILED,  # This will be query default state if no metadata found
    )

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
    default_database = "MY_DB"
    default_schema = "MY_SCHEMA"

    with mock.patch(
        "airflow.providers.snowflake.utils.openlineage._get_queries_details_from_snowflake",
        return_value=fake_metadata,
    ):
        emit_openlineage_events_for_snowflake_queries(
            query_ids=query_ids,
            query_source_namespace="snowflake_ns",
            task_instance=mock_ti,
            hook=mock.MagicMock(),
            query_for_extra_metadata=True,
            default_database=default_database,
            default_schema=default_schema,
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        )

    assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.

    expected_calls = [
        mock.call(
            query_id="query1",
            query_source_namespace="snowflake_ns",
            query_text=None,
            default_database=default_database,
            default_schema=default_schema,
            start_time=fake_metadata["query1"]["START_TIME"],
            end_time=fake_metadata["query1"]["END_TIME"],
            is_successful=True,
            error_message=None,
            job_name="dag_id.task_id.query.1",
            task_instance=mock_ti,
            additional_run_facets=additional_run_facets,
            additional_job_facets={
                "custom_job": "value_job",
                "sql": SQLJobFacet(query="SELECT * FROM table1"),
            },
        ),
        mock.call(
            query_id="query2",
            query_source_namespace="snowflake_ns",
            query_text=None,
            default_database=default_database,
            default_schema=default_schema,
            start_time=fake_metadata["query2"]["START_TIME"],
            end_time=fake_metadata["query2"]["END_TIME"],
            is_successful=False,
            error_message="ERR001 : Error occurred",
            job_name="dag_id.task_id.query.2",
            task_instance=mock_ti,
            additional_run_facets=additional_run_facets,
            additional_job_facets={
                "custom_job": "value_job",
                "sql": SQLJobFacet(query="SELECT * FROM table2"),
            },
        ),
        mock.call(
            query_id="query3",
            query_source_namespace="snowflake_ns",
            query_text=None,
            default_database=default_database,
            default_schema=default_schema,
            start_time=FROZEN_NOW,
            end_time=FROZEN_NOW,
            is_successful=False,  # no metadata for query3, default state ("failed") is used
            error_message=None,
            job_name="dag_id.task_id.query.3",
            task_instance=mock_ti,
            additional_run_facets=additional_run_facets,
            additional_job_facets=additional_job_facets,
        ),
    ]
    assert mock_emit_query_lineage.call_args_list == expected_calls


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("airflow.providers.openlineage.api.emit_query_lineage")
@time_machine.travel(FROZEN_NOW, tick=False)
def test_emit_openlineage_events_for_snowflake_queries_without_extra_metadata(
    mock_emit_query_lineage, mock_version
):
    query_ids = ["query1"]
    original_query_ids = copy.deepcopy(query_ids)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        state=TaskInstanceState.SUCCESS,  # This will be query default state if no metadata found
    )

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}
    hook = mock.MagicMock()

    emit_openlineage_events_for_snowflake_queries(
        query_ids=query_ids,
        query_source_namespace="snowflake_ns",
        task_instance=mock_ti,
        hook=hook,
        # query_for_extra_metadata=False,  # False by default
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )

    assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
    mock_emit_query_lineage.assert_called_once_with(
        query_id="query1",
        query_source_namespace="snowflake_ns",
        query_text=None,
        default_database=None,
        default_schema=None,
        start_time=FROZEN_NOW,
        end_time=FROZEN_NOW,
        is_successful=True,  # no metadata, default state ("success") is used
        error_message=None,
        job_name="dag_id.task_id.query.1",
        task_instance=mock_ti,
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("airflow.providers.openlineage.api.emit_query_lineage")
@time_machine.travel(FROZEN_NOW, tick=False)
def test_emit_openlineage_events_for_snowflake_queries_without_query_ids(
    mock_emit_query_lineage, mock_version
):
    hook = mock.MagicMock()
    hook.query_ids = ["query1"]
    original_query_ids = copy.deepcopy(hook.query_ids)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        state=TaskInstanceState.RUNNING,  # Success will be query default state if no metadata found
    )

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    emit_openlineage_events_for_snowflake_queries(
        query_ids=[],
        query_source_namespace="snowflake_ns",
        task_instance=mock_ti,
        hook=hook,
        # query_for_extra_metadata=False,  # False by default
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )

    assert hook.query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
    mock_emit_query_lineage.assert_called_once_with(
        query_id="query1",
        query_source_namespace="snowflake_ns",
        query_text=None,
        default_database=None,
        default_schema=None,
        start_time=FROZEN_NOW,
        end_time=FROZEN_NOW,
        is_successful=True,  # no metadata, "running" default state is treated as success
        error_message=None,
        job_name="dag_id.task_id.query.1",
        task_instance=mock_ti,
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )


@mock.patch("airflow.providers.openlineage.sqlparser.SQLParser.create_namespace", return_value="snowflake_ns")
@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("airflow.providers.openlineage.api.emit_query_lineage")
@time_machine.travel(FROZEN_NOW, tick=False)
def test_emit_openlineage_events_for_snowflake_queries_without_query_ids_and_namespace(
    mock_emit_query_lineage, mock_version, mock_parser
):
    hook = mock.MagicMock()
    hook.query_ids = ["query1"]
    original_query_ids = copy.deepcopy(hook.query_ids)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        state="running",  # Success will be query default state if no metadata found
    )

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    emit_openlineage_events_for_snowflake_queries(
        query_ids=[],
        query_source_namespace=None,
        task_instance=mock_ti,
        hook=hook,
        # query_for_extra_metadata=False,  # False by default
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )

    assert hook.query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
    mock_emit_query_lineage.assert_called_once_with(
        query_id="query1",
        query_source_namespace="snowflake_ns",  # resolved from the hook via the patched SQLParser
        query_text=None,
        default_database=None,
        default_schema=None,
        start_time=FROZEN_NOW,
        end_time=FROZEN_NOW,
        is_successful=True,  # no metadata, "running" default state is treated as success
        error_message=None,
        job_name="dag_id.task_id.query.1",
        task_instance=mock_ti,
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )


@mock.patch("importlib.metadata.version", return_value="3.0.0")
@mock.patch("airflow.providers.openlineage.api.emit_query_lineage")
@time_machine.travel(FROZEN_NOW, tick=False)
def test_emit_openlineage_events_for_snowflake_queries_with_query_ids_and_hook_query_ids(
    mock_emit_query_lineage, mock_version
):
    hook = mock.MagicMock()
    hook.query_ids = ["query1"]
    original_query_ids = copy.deepcopy(hook.query_ids)
    mock_ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        state="running",  # Success will be query default state if no metadata found
    )

    additional_run_facets = {"custom_run": "value_run"}
    additional_job_facets = {"custom_job": "value_job"}

    emit_openlineage_events_for_snowflake_queries(
        query_ids=["query2"],
        query_source_namespace="snowflake_ns",
        task_instance=mock_ti,
        hook=hook,
        # query_for_extra_metadata=False,  # False by default
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )

    # The explicitly passed `query_ids=["query2"]` takes precedence over `hook.query_ids`.
    assert hook.query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
    mock_emit_query_lineage.assert_called_once_with(
        query_id="query2",
        query_source_namespace="snowflake_ns",
        query_text=None,
        default_database=None,
        default_schema=None,
        start_time=FROZEN_NOW,
        end_time=FROZEN_NOW,
        is_successful=True,  # no metadata, "running" default state is treated as success
        error_message=None,
        job_name="dag_id.task_id.query.1",
        task_instance=mock_ti,
        additional_run_facets=additional_run_facets,
        additional_job_facets=additional_job_facets,
    )


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_snowflake_queries_missing_query_ids_and_hook(mock_version):
    fake_adapter = mock.MagicMock()
    fake_adapter.emit = mock.MagicMock()
    fake_listener = mock.MagicMock()
    fake_listener.adapter = fake_adapter

    with mock.patch(
        "airflow.providers.openlineage.plugins.listener.get_openlineage_listener",
        return_value=fake_listener,
    ):
        with pytest.raises(ValueError, match="If 'hook' is not provided, 'query_ids' must be set."):
            emit_openlineage_events_for_snowflake_queries(
                task_instance=None, query_source_namespace="snowflake_ns", query_for_extra_metadata=False
            )

        fake_adapter.emit.assert_not_called()  # No events should be emitted


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_snowflake_queries_missing_query_namespace_and_hook(mock_version):
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
            emit_openlineage_events_for_snowflake_queries(
                task_instance=None, query_ids=query_ids, query_for_extra_metadata=False
            )

        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted


@mock.patch("importlib.metadata.version", return_value="3.0.0")
def test_emit_openlineage_events_for_snowflake_queries_missing_hook_and_query_for_extra_metadata_true(
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
            emit_openlineage_events_for_snowflake_queries(
                task_instance=None,
                query_source_namespace="snowflake_ns",
                query_ids=query_ids,
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
            "OpenLineage provider version `1.99.0` is lower than required `2.16.0`, "
            "skipping function `emit_openlineage_events_for_snowflake_queries` execution"
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
            emit_openlineage_events_for_snowflake_queries(
                query_ids=query_ids,
                query_source_namespace="snowflake_ns",
                task_instance=None,
            )
        assert query_ids == original_query_ids  # Verify that the input query_ids list is unchanged.
        fake_adapter.emit.assert_not_called()  # No events should be emitted
