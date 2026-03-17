#
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

from unittest import mock

import pendulum
import pytest

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.snowflake.operators.snowflake_notebook import SnowflakeNotebookOperator
from airflow.providers.snowflake.triggers.snowflake_trigger import SnowflakeSqlApiTrigger
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, timezone

TASK_ID = "snowflake_notebook"
CONN_ID = "my_snowflake_conn"
NOTEBOOK = "MY_DB.MY_SCHEMA.MY_NOTEBOOK"

HOOK_MODULE = "airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook"


@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="snowflake_default",
            conn_type="snowflake",
            host="test_host",
            port=443,
            schema="test_schema",
            login="test_user",
            password="test_password",
        )
    )


def create_context(task, dag=None):
    if dag is None:
        dag = DAG(dag_id="dag", schedule=None)
    tzinfo = pendulum.timezone("UTC")
    logical_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dag_version import DagVersion

        sync_dag_to_db(dag)
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        task_instance = create_task_instance(task=task, run_id="test_run_id", dag_version_id=dag_version.id)
        dag_run = DagRun(
            dag_id=dag.dag_id,
            logical_date=logical_date,
            run_id=DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=logical_date, run_after=logical_date
            ),
        )
    else:
        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=logical_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
        )
        task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock(spec=TaskInstance.xcom_push)
    date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
    return {
        "dag": dag,
        "ts": logical_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
        "dag_run": dag_run,
        "data_interval_end": logical_date,
        date_key: logical_date,
    }


class TestSnowflakeNotebookOperatorSQL:
    """Tests for SQL query building."""

    def test_build_sql_no_params(self):
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            notebook=NOTEBOOK,
        )
        assert operator.sql == "EXECUTE NOTEBOOK MY_DB.MY_SCHEMA.MY_NOTEBOOK()"

    def test_build_sql_with_params(self):
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            notebook=NOTEBOOK,
            parameters=["param1", "target_db=PROD"],
        )
        assert operator.sql == "EXECUTE NOTEBOOK MY_DB.MY_SCHEMA.MY_NOTEBOOK('param1', 'target_db=PROD')"

    def test_build_sql_escapes_single_quotes(self):
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            notebook=NOTEBOOK,
            parameters=["O'Brien", "it's"],
        )
        assert operator.sql == "EXECUTE NOTEBOOK MY_DB.MY_SCHEMA.MY_NOTEBOOK('O''Brien', 'it''s')"

    def test_build_sql_empty_params(self):
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            notebook=NOTEBOOK,
            parameters=[],
        )
        assert operator.sql == "EXECUTE NOTEBOOK MY_DB.MY_SCHEMA.MY_NOTEBOOK()"

    def test_template_fields(self):
        assert "notebook" in SnowflakeNotebookOperator.template_fields
        assert "parameters" in SnowflakeNotebookOperator.template_fields
        assert "snowflake_conn_id" in SnowflakeNotebookOperator.template_fields

    def test_statement_count_is_one(self):
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            notebook=NOTEBOOK,
        )
        assert operator.statement_count == 1

    def test_is_subclass_of_snowflake_sql_api_operator(self):
        assert issubclass(SnowflakeNotebookOperator, SnowflakeSqlApiOperator)


@pytest.mark.db_test
class TestSnowflakeNotebookOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

        yield

        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    @pytest.fixture
    def mock_execute_query(self):
        with mock.patch(f"{HOOK_MODULE}.execute_query") as execute_query:
            yield execute_query

    @pytest.fixture
    def mock_get_sql_api_query_status(self):
        with mock.patch(f"{HOOK_MODULE}.get_sql_api_query_status") as get_status:
            yield get_status

    def test_execute_success_immediate(self, mock_execute_query, mock_get_sql_api_query_status):
        """Notebook completes immediately without polling or deferring."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            notebook=NOTEBOOK,
            do_xcom_push=False,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}]
        operator.execute(context=None)

    def test_execute_failure_immediate(self, mock_execute_query, mock_get_sql_api_query_status):
        """Notebook fails immediately, raises AirflowException."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            notebook=NOTEBOOK,
            do_xcom_push=False,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "error", "message": "Notebook failed"}]
        with pytest.raises(AirflowException):
            operator.execute(context=None)

    @mock.patch(f"{HOOK_MODULE}.execute_query")
    def test_execute_deferred(self, mock_execute_query, mock_get_sql_api_query_status):
        """Running notebook with deferrable=True raises TaskDeferred."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            deferrable=True,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "running"}]

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(exc.value.trigger, SnowflakeSqlApiTrigger)

    def test_execute_polling_success(self, mock_execute_query, mock_get_sql_api_query_status):
        """Non-deferrable mode polls until success."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            do_xcom_push=False,
            deferrable=False,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [
            # Initial status check in execute()
            {"status": "running"},
            # 1st poll
            {"status": "running"},
            # 2nd poll — success
            {"status": "success"},
        ]

        with mock.patch("time.sleep"), mock.patch(f"{HOOK_MODULE}.check_query_output"):
            operator.execute(context=None)

    def test_execute_polling_failure(self, mock_execute_query, mock_get_sql_api_query_status):
        """Non-deferrable mode raises when polling finds error."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            do_xcom_push=False,
            deferrable=False,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [
            {"status": "running"},
            {"status": "error", "message": "Notebook execution failed"},
        ]

        with pytest.raises(AirflowException):
            operator.execute(context=None)

    def test_execute_xcom_push(self, mock_execute_query, mock_get_sql_api_query_status):
        """XCom push stores query_ids when do_xcom_push is True."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            notebook=NOTEBOOK,
            do_xcom_push=True,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}]

        mock_ti = mock.Mock(spec=TaskInstance)
        context = {"ti": mock_ti}
        operator.execute(context=context)
        mock_ti.xcom_push.assert_called_once_with(key="query_ids", value=["uuid1"])

    def test_execute_complete_success(self):
        """execute_complete handles success event."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            deferrable=True,
        )
        event = {"status": "success", "statement_query_ids": ["uuid1"]}
        with mock.patch(f"{HOOK_MODULE}.check_query_output"):
            operator.execute_complete(context=None, event=event)

    def test_execute_complete_failure(self):
        """execute_complete raises AirflowException on error event."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=None,
                event={"status": "error", "message": "Notebook failed"},
            )

    def test_execute_complete_none_event(self):
        """execute_complete handles None event gracefully."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            deferrable=True,
        )
        operator.execute_complete(context=None, event=None)

    def test_execute_complete_reassigns_query_ids(self):
        """execute_complete sets query_ids from event."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
            deferrable=True,
        )
        assert operator.query_ids == []
        with mock.patch(f"{HOOK_MODULE}.check_query_output"):
            operator.execute_complete(
                context=None,
                event={"status": "success", "statement_query_ids": ["uuid1", "uuid2"]},
            )
        assert operator.query_ids == ["uuid1", "uuid2"]

    @mock.patch(f"{HOOK_MODULE}.cancel_queries")
    def test_on_kill_with_queries(self, mock_cancel_queries):
        """on_kill cancels running queries."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
        )
        operator.query_ids = ["uuid1", "uuid2"]
        operator.on_kill()
        mock_cancel_queries.assert_called_once_with(["uuid1", "uuid2"])

    @mock.patch(f"{HOOK_MODULE}.cancel_queries")
    def test_on_kill_no_queries(self, mock_cancel_queries):
        """on_kill does nothing when no query ids exist."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
        )
        operator.query_ids = []
        operator.on_kill()
        mock_cancel_queries.assert_not_called()

    def test_hook_caching(self):
        """_hook property returns the same instance on repeated access."""
        operator = SnowflakeNotebookOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            notebook=NOTEBOOK,
        )
        hook1 = operator._hook
        hook2 = operator._hook
        assert hook1 is hook2
