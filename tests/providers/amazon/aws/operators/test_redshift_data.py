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

import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, TaskDeferred
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger

CONN_ID = "aws_conn_test"
TASK_ID = "task_id"
SQL = "sql"
DATABASE = "database"
STATEMENT_ID = "statement_id"


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
        wait_for_completion=False,
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
        with pytest.warns(AirflowProviderDeprecationWarning):
            # Check deprecated region argument
            assert op.region == "eu-central-1"
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

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute(self, mock_exec_query):
        cluster_identifier = "cluster_identifier"
        workgroup_name = None
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5
        wait_for_completion = True

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
        operator.execute(None)
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
            wait_for_completion=wait_for_completion,
            poll_interval=poll_interval,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute_with_workgroup_name(self, mock_exec_query):
        cluster_identifier = None
        workgroup_name = "workgroup_name"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5
        wait_for_completion = True

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
        operator.execute(None)
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
            wait_for_completion=wait_for_completion,
            poll_interval=poll_interval,
        )

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
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            cluster_identifier="cluster_identifier",
            sql=SQL,
            database=DATABASE,
            wait_for_completion=False,
        )
        operator.execute(None)
        operator.on_kill()
        mock_conn.cancel_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_return_sql_result(self, mock_conn):
        expected_result = {"Result": True}
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        mock_conn.describe_statement.return_value = {"Status": "FINISHED"}
        mock_conn.get_statement_result.return_value = expected_result
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
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
        actual_result = operator.execute(None)
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
        mock_conn.get_statement_result.assert_called_once_with(
            Id=STATEMENT_ID,
        )

    @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.defer")
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished",
        return_value=True,
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute_finished_before_defer(self, mock_exec_query, check_query_is_finished, mock_defer):
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
        operator.execute(None)

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
        )

    # @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.defer")
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished",
        return_value=False,
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute_defer(self, mock_exec_query, check_query_is_finished, deferrable_operator):
        with pytest.raises(TaskDeferred) as exc:
            deferrable_operator.execute(None)

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
            assert exc.value.args[0] == "Did not receive valid event from the trigerrer"

    def test_execute_complete(self, deferrable_operator):
        """Asserts that logging occurs as expected"""

        deferrable_operator.statement_id = "uuid"

        with mock.patch.object(deferrable_operator.log, "info") as mock_log_info:
            assert (
                deferrable_operator.execute_complete(
                    context=None,
                    event={"status": "success", "message": "Job completed", "statement_id": "uuid"},
                )
                == "uuid"
            )
        mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)
