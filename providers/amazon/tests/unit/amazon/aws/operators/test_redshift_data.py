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

import botocore.exceptions
import pytest

from airflow.providers.amazon.aws.hooks.redshift_data import QueryExecutionOutput
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

CONN_ID = "aws_conn_test"
TASK_ID = "task_id"
SQL = "sql"
DATABASE = "database"
STATEMENT_ID = "statement_id"
NEW_STATEMENT_ID = "new_statement_id"
SESSION_ID = "session_id"


@pytest.fixture
def deferrable_operator():
    cluster_identifier = "cluster_identifier"
    db_user = "db_user"
    secret_arn = "secret_arn"
    statement_name = "statement_name"
    parameters = [{"name": "id", "value": "1"}]
    poll_interval = 5

    operator = RedshiftDataOperator(
        aws_conn_id=CONN_ID,
        task_id=TASK_ID,
        sql=SQL,
        database=DATABASE,
        cluster_identifier=cluster_identifier,
        db_user=db_user,
        secret_arn=secret_arn,
        statement_name=statement_name,
        parameters=parameters,
        wait_for_completion=True,
        poll_interval=poll_interval,
        deferrable=True,
    )
    return operator


class TestRedshiftDataOperator:
    def test_init(self):
        op = RedshiftDataOperator(
            task_id="fake_task_id",
            database="fake-db",
            sql="SELECT 1",
            aws_conn_id="fake-conn-id",
            region_name="eu-central-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.client_type == "redshift-data"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-central-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = RedshiftDataOperator(task_id="fake_task_id", database="fake-db", sql="SELECT 1")
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(RedshiftDataOperator, "log", new_callable=mock.MagicMock)
    def test_init_warns_when_deferrable_has_no_effect(self, mock_log):
        """deferrable=True is a no-op when wait_for_completion=False; the user should be told."""
        RedshiftDataOperator(
            task_id=TASK_ID,
            database=DATABASE,
            sql=SQL,
            deferrable=True,
            wait_for_completion=False,
        )
        mock_log.warning.assert_called_once_with(
            "deferrable=True and wait_for_completion=False are set; deferrable will be "
            "ignored and this task will run non-deferrable."
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute(self, mock_conn, mock_exec_query):
        cluster_identifier = "cluster_identifier"
        workgroup_name = None
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5

        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=None)

        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            wait_for_completion=True,
            poll_interval=poll_interval,
        )

        # Mock the TaskInstance, call the execute method
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        actual_result = operator.execute({"ti": mock_ti})

        mock_exec_query.assert_called_once_with(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            with_event=False,
            wait_for_completion=False,  # submit_job always submits async; the mixin owns waiting
            poll_interval=poll_interval,
            session_id=None,
            session_keep_alive_seconds=None,
        )

        # Check that the result returned is a list of the statement_id's
        assert actual_result == [STATEMENT_ID]
        mock_ti.xcom_push.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_with_workgroup_name(self, mock_conn, mock_exec_query):
        cluster_identifier = None
        workgroup_name = "workgroup_name"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5

        # Like before, return a statement ID and a status
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=None)

        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            wait_for_completion=True,
            poll_interval=poll_interval,
        )

        # Mock the TaskInstance, call the execute method
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        actual_result = operator.execute({"ti": mock_ti})

        # Assertions
        assert actual_result == [STATEMENT_ID]
        mock_exec_query.assert_called_once_with(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,  # Called with workgroup_name
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            with_event=False,
            wait_for_completion=False,  # submit_job always submits async; the mixin owns waiting
            poll_interval=poll_interval,
            session_id=None,
            session_keep_alive_seconds=None,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_new_session(self, mock_conn, mock_exec_query):
        cluster_identifier = "cluster_identifier"
        workgroup_name = None
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5

        # Like before, return a statement ID and a status
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=SESSION_ID)

        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            wait_for_completion=True,
            poll_interval=poll_interval,
            session_keep_alive_seconds=123,
        )

        # Mock the TaskInstance and call the execute method
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        operator.execute({"ti": mock_ti})

        mock_exec_query.assert_called_once_with(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            with_event=False,
            wait_for_completion=False,  # submit_job always submits async; the mixin owns waiting
            poll_interval=poll_interval,
            session_id=None,
            session_keep_alive_seconds=123,
        )
        assert mock_ti.xcom_push.call_args.kwargs["key"] == "session_id"
        assert mock_ti.xcom_push.call_args.kwargs["value"] == SESSION_ID

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_on_kill_without_query(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            wait_for_completion=False,
        )
        operator.on_kill()
        mock_conn.cancel_statement.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_on_kill_with_query(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID, "SessionId": SESSION_ID}
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            cluster_identifier="cluster_identifier",
            sql=SQL,
            database=DATABASE,
            wait_for_completion=False,
        )
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        operator.execute({"ti": mock_ti})
        operator.on_kill()
        mock_conn.cancel_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_return_sql_result(self, mock_conn):
        expected_result = [{"Result": True}]
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"

        # Mock the conn object
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID, "SessionId": SESSION_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_conn.get_statement_result.return_value = {"Result": True}

        operator = RedshiftDataOperator(
            task_id=TASK_ID,
            cluster_identifier=cluster_identifier,
            database=DATABASE,
            db_user=db_user,
            sql=SQL,
            statement_name=statement_name,
            secret_arn=secret_arn,
            aws_conn_id=CONN_ID,
            return_sql_result=True,
        )

        # Mock the TaskInstance, run the execute method
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        actual_result = operator.execute({"ti": mock_ti})

        assert actual_result == expected_result
        mock_conn.execute_statement.assert_called_once_with(
            Database=DATABASE,
            Sql=SQL,
            ClusterIdentifier=cluster_identifier,
            DbUser=db_user,
            SecretArn=secret_arn,
            StatementName=statement_name,
            WithEvent=False,
        )
        mock_conn.get_statement_result.assert_called_once_with(Id=STATEMENT_ID)

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.defer")
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished",
        return_value=True,
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute_finished_before_defer(
        self, mock_exec_query, check_query_is_finished, mock_defer, mock_conn
    ):
        cluster_identifier = "cluster_identifier"
        workgroup_name = None
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5

        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            wait_for_completion=False,
            poll_interval=poll_interval,
            deferrable=True,
        )

        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        operator.execute({"ti": mock_ti})

        assert not mock_defer.called
        mock_exec_query.assert_called_once_with(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            with_event=False,
            wait_for_completion=False,
            poll_interval=poll_interval,
            session_id=None,
            session_keep_alive_seconds=None,
        )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished",
        return_value=False,
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute_defer(self, mock_exec_query, check_query_is_finished, deferrable_operator):
        mock_ti = mock.MagicMock(name="MockedTaskInstance")
        with pytest.raises(TaskDeferred) as exc:
            deferrable_operator.execute({"ti": mock_ti})

        assert isinstance(exc.value.trigger, RedshiftDataTrigger)

    def test_execute_complete_failure(self, deferrable_operator):
        """Tests that an AirflowException is raised in case of error event"""
        with pytest.raises(AirflowException):
            deferrable_operator.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_execute_complete_exception(self, deferrable_operator):
        """Tests that an AirflowException is raised in case of empty event"""
        with pytest.raises(AirflowException) as exc:
            deferrable_operator.execute_complete(context=None, event=None)
        assert exc.value.args[0] == "Trigger error: event is None"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_execute_complete(self, mock_conn, deferrable_operator):
        """Asserts that logging occurs as expected"""

        deferrable_operator.statement_id = "uuid"

        with mock.patch.object(deferrable_operator.log, "info") as mock_log_info:
            assert deferrable_operator.execute_complete(
                context=None,
                event={"status": "success", "message": "Job completed", "statement_id": "uuid"},
            ) == ["uuid"]
        mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)

    @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_no_wait_for_completion(
        self, mock_exec_query, mock_conn, mock_check_query_is_finished, mock_defer
    ):
        """Tests that the operator does not check for completion nor defers when wait_for_completion is False,
        no matter the value of deferrable"""
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5
        wait_for_completion = False

        # Mock the describe_statement call
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=SESSION_ID)

        for deferrable in [True, False]:
            operator = RedshiftDataOperator(
                aws_conn_id=CONN_ID,
                task_id=TASK_ID,
                sql=SQL,
                database=DATABASE,
                cluster_identifier=cluster_identifier,
                db_user=db_user,
                secret_arn=secret_arn,
                statement_name=statement_name,
                parameters=parameters,
                wait_for_completion=wait_for_completion,
                poll_interval=poll_interval,
                deferrable=deferrable,
            )

            # Mock the TaskInstance, call the execute method
            mock_ti = mock.MagicMock(name="MockedTaskInstance")
            actual_results = operator.execute({"ti": mock_ti})

            assert not mock_check_query_is_finished.called
            assert not mock_defer.called
            assert actual_results == [STATEMENT_ID]

    def test_template_fields(self):
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            cluster_identifier="cluster_identifier",
            sql=SQL,
            database=DATABASE,
            wait_for_completion=False,
        )
        validate_template_fields(operator)


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)
class TestRedshiftDataOperatorDurable:
    @staticmethod
    def _context(task_store=None):
        ctx: dict = {"ti": mock.MagicMock(stats_tags={})}
        if task_store is not None:
            ctx["task_state_store"] = task_store
        return ctx

    @staticmethod
    def _make_operator(**kwargs):
        return RedshiftDataOperator(
            task_id=TASK_ID,
            database=DATABASE,
            sql=SQL,
            cluster_identifier="cluster_identifier",
            **kwargs,
        )

    @staticmethod
    def _not_found_error():
        return botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            operation_name="DescribeStatement",
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_persists_statement_id_to_task_state_store_on_fresh_submit(self, mock_conn, mock_exec_query):
        operator = self._make_operator()
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=None)
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = None

        operator.execute(self._context(task_store))

        mock_exec_query.assert_called_once()
        task_store.set.assert_called_once_with("redshift_statement_id", STATEMENT_ID)

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_reconnects_to_running_statement_without_resubmitting(self, mock_conn, mock_exec_query):
        operator = self._make_operator()
        # get_job_status sees it still running, wait_for_results then sees it finish,
        # get_sql_results makes one more describe_statement call to check for sub-statements.
        mock_conn.describe_statement.side_effect = [
            {"Status": "STARTED"},
            {"Status": "FINISHED"},
            {"Status": "FINISHED"},
        ]
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = STATEMENT_ID

        result = operator.execute(self._context(task_store))

        mock_exec_query.assert_not_called()
        task_store.set.assert_not_called()
        assert result == [STATEMENT_ID]

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_already_succeeded_returns_result_without_polling(self, mock_conn, mock_exec_query):
        operator = self._make_operator()
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = STATEMENT_ID

        result = operator.execute(self._context(task_store))

        mock_exec_query.assert_not_called()
        assert result == [STATEMENT_ID]
        # Only get_job_status + get_sql_results should have called describe_statement -- no poll loop.
        assert mock_conn.describe_statement.call_count == 2

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_resubmits_when_stored_statement_in_terminal_failure(self, mock_conn, mock_exec_query):
        operator = self._make_operator()
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=NEW_STATEMENT_ID, session_id=None)
        # get_job_status sees the stored statement failed; after fresh resubmit, polling succeeds.
        mock_conn.describe_statement.side_effect = [
            {"Status": "FAILED"},
            {"Status": "FINISHED"},
            {"Status": "FINISHED"},
        ]
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = STATEMENT_ID

        result = operator.execute(self._context(task_store))

        mock_exec_query.assert_called_once()
        task_store.set.assert_called_once_with("redshift_statement_id", NEW_STATEMENT_ID)
        assert result == [NEW_STATEMENT_ID]

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_resubmits_when_stored_statement_not_found(self, mock_conn, mock_exec_query):
        operator = self._make_operator()
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=NEW_STATEMENT_ID, session_id=None)
        mock_conn.describe_statement.side_effect = [
            self._not_found_error(),
            {"Status": "FINISHED"},
            {"Status": "FINISHED"},
        ]
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = STATEMENT_ID

        result = operator.execute(self._context(task_store))

        mock_exec_query.assert_called_once()
        task_store.set.assert_called_once_with("redshift_statement_id", NEW_STATEMENT_ID)
        assert result == [NEW_STATEMENT_ID]

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_durable_false_never_touches_task_state_store(self, mock_conn, mock_exec_query):
        operator = self._make_operator(durable=False)
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=None)
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        task_store = mock.MagicMock(spec_set=["get", "set"])

        operator.execute(self._context(task_store))

        mock_exec_query.assert_called_once()
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_deferrable_unaffected_by_durable(self, mock_conn, mock_exec_query, mock_defer):
        operator = self._make_operator(deferrable=True)
        mock_exec_query.return_value = QueryExecutionOutput(statement_id=STATEMENT_ID, session_id=None)
        mock_conn.describe_statement.return_value = {"Status": "STARTED"}
        task_store = mock.MagicMock(spec_set=["get", "set"])

        operator.execute(self._context(task_store))

        assert mock_defer.called
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_wait_for_completion_false_still_protects_against_duplicate_submission(
        self, mock_conn, mock_exec_query
    ):
        """A prior attempt already submitted successfully; a retry must not resubmit even
        though wait_for_completion=False means this attempt never polls."""
        operator = self._make_operator(wait_for_completion=False)
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = STATEMENT_ID

        result = operator.execute(self._context(task_store))

        mock_exec_query.assert_not_called()
        assert result == [STATEMENT_ID]

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_get_job_status_returns_raw_status(self, mock_conn):
        operator = self._make_operator()
        mock_conn.describe_statement.return_value = {"Status": "STARTED"}

        assert operator.get_job_status(STATEMENT_ID, context={}) == "STARTED"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_get_job_status_not_found_on_resource_not_found(self, mock_conn):
        operator = self._make_operator()
        mock_conn.describe_statement.side_effect = self._not_found_error()

        assert operator.get_job_status(STATEMENT_ID, context={}) == "NOT_FOUND"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_get_job_status_reraises_other_client_errors(self, mock_conn):
        operator = self._make_operator()
        mock_conn.describe_statement.side_effect = botocore.exceptions.ClientError(
            error_response={"Error": {"Code": "InternalServerException", "Message": "boom"}},
            operation_name="DescribeStatement",
        )

        with pytest.raises(botocore.exceptions.ClientError):
            operator.get_job_status(STATEMENT_ID, context={})

    def test_is_job_active_and_is_job_succeeded_predicates(self):
        operator = self._make_operator()

        for status in ("PICKED", "STARTED", "SUBMITTED"):
            assert operator.is_job_active(status) is True
        for status in ("FINISHED", "FAILED", "ABORTED", "NOT_FOUND"):
            assert operator.is_job_active(status) is False

        assert operator.is_job_succeeded("FINISHED") is True
        assert operator.is_job_succeeded("STARTED") is False
