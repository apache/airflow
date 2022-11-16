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

import pandas as pd
import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, Connection
from airflow.providers.slack.transfers.sql_to_slack import (
    BaseSqlToSlackOperator,
    SqlToSlackApiFileOperator,
    SqlToSlackOperator,
)
from airflow.utils import timezone

TEST_DAG_ID = "sql_to_slack_unit_test"
TEST_TASK_ID = "sql_to_slack_unit_test_task"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestBaseSqlToSlackOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "sql": "SELECT 1",
            "sql_conn_id": "test-sql-conn-id",
            "sql_hook_params": None,
            "parameters": None,
        }

    def test_execute_not_implemented(self):
        """Test that no base implementation for ``BaseSqlToSlackOperator.execute()``."""
        op = BaseSqlToSlackOperator(task_id="test_base_not_implements", **self.default_op_kwargs)
        with pytest.raises(NotImplementedError):
            op.execute(mock.MagicMock())

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    @mock.patch("airflow.models.connection.Connection.get_hook")
    @pytest.mark.parametrize("conn_type", ["postgres", "snowflake"])
    @pytest.mark.parametrize("sql_hook_params", [None, {"foo": "bar"}])
    def test_get_hook(self, mock_get_hook, mock_get_conn, conn_type, sql_hook_params):
        class SomeDummyHook:
            """Hook which implements ``get_pandas_df`` method"""

            def get_pandas_df(self):
                pass

        expected_hook = SomeDummyHook()
        mock_get_conn.return_value = Connection(conn_id=f"test_connection_{conn_type}", conn_type=conn_type)
        mock_get_hook.return_value = expected_hook
        op_kwargs = {
            **self.default_op_kwargs,
            "sql_hook_params": sql_hook_params,
        }
        op = BaseSqlToSlackOperator(task_id="test_get_hook", **op_kwargs)
        hook = op._get_hook()
        mock_get_hook.assert_called_once_with(hook_params=sql_hook_params)
        assert hook == expected_hook

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    @mock.patch("airflow.models.connection.Connection.get_hook")
    def test_get_not_supported_hook(self, mock_get_hook, mock_get_conn):
        class SomeDummyHook:
            """Hook which not implemented ``get_pandas_df`` method"""

        mock_get_conn.return_value = Connection(conn_id="test_connection", conn_type="test_connection")
        mock_get_hook.return_value = SomeDummyHook()
        op = BaseSqlToSlackOperator(task_id="test_get_not_supported_hook", **self.default_op_kwargs)
        error_message = r"This hook is not supported. The hook class must have get_pandas_df method\."
        with pytest.raises(AirflowException, match=error_message):
            op._get_hook()

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_hook")
    @pytest.mark.parametrize("sql", ["SELECT 42", "SELECT 1 FROM DUMMY WHERE col = ?"])
    @pytest.mark.parametrize("parameters", [None, {"col": "spam-egg"}])
    def test_get_query_results(self, mock_op_get_hook, sql, parameters):
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        mock_get_pandas_df = mock.MagicMock(return_value=test_df)
        mock_hook = mock.MagicMock()
        mock_hook.get_pandas_df = mock_get_pandas_df
        mock_op_get_hook.return_value = mock_hook
        op_kwargs = {
            **self.default_op_kwargs,
            "sql": sql,
            "parameters": parameters,
        }
        op = BaseSqlToSlackOperator(task_id="test_get_query_results", **op_kwargs)
        df = op._get_query_results()
        mock_get_pandas_df.assert_called_once_with(sql, parameters=parameters)
        assert df is test_df


class TestSqlToSlackOperator:
    def setup_method(self):
        self.example_dag = DAG(TEST_DAG_ID, start_date=DEFAULT_DATE)

    @staticmethod
    def _construct_operator(**kwargs):
        operator = SqlToSlackOperator(task_id=TEST_TASK_ID, **kwargs)
        return operator

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook")
    def test_rendering_and_message_execution(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id="slack_connection",
            webhook_token=None,
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook")
    def test_rendering_and_message_execution_with_slack_hook(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_conn_id": "slack_connection",
            "slack_webhook_token": "test_token",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id="slack_connection",
            webhook_token="test_token",
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    def test_non_existing_slack_parameters_provided_exception_thrown(self):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        with pytest.raises(AirflowException):
            self._construct_operator(**operator_args)

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook")
    def test_rendering_custom_df_name_message_execution(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ testing }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "results_df_name": "testing",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id="slack_connection",
            webhook_token=None,
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    def test_hook_params_building(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="snowflake_connection", conn_type="snowflake")
        hook_params = {
            "schema": "test_schema",
            "role": "test_role",
            "database": "test_database",
            "warehouse": "test_warehouse",
        }
        operator_args = {
            "sql_conn_id": "dummy_connection",
            "sql": "sql {{ ds }}",
            "results_df_name": "xxxx",
            "sql_hook_params": hook_params,
            "parameters": ["1", "2", "3"],
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "slack_webhook_token": "test_token",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = SqlToSlackOperator(task_id=TEST_TASK_ID, **operator_args)

        assert sql_to_slack_operator.sql_hook_params == hook_params

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    def test_hook_params(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="postgres_test", conn_type="postgres")
        op = SqlToSlackOperator(
            task_id="sql_hook_params",
            sql_conn_id="postgres_test",
            slack_webhook_token="slack_token",
            sql="SELECT 1",
            slack_message="message: {{ ds }}, {{ xxxx }}",
            sql_hook_params={
                "log_sql": False,
            },
        )
        hook = op._get_hook()
        assert hook.log_sql == op.sql_hook_params["log_sql"]

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    def test_hook_params_snowflake(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="snowflake_default", conn_type="snowflake")
        op = SqlToSlackOperator(
            task_id="snowflake_hook_params",
            sql_conn_id="snowflake_default",
            slack_conn_id="slack_default",
            results_df_name="xxxx",
            sql="SELECT 1",
            slack_message="message: {{ ds }}, {{ xxxx }}",
            sql_hook_params={
                "warehouse": "warehouse",
                "database": "database",
                "role": "role",
                "schema": "schema",
            },
        )
        hook = op._get_hook()

        assert hook.warehouse == "warehouse"
        assert hook.database == "database"
        assert hook.role == "role"
        assert hook.schema == "schema"


class TestSqlToSlackApiFileOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "sql": "SELECT 1",
            "sql_conn_id": "test-sql-conn-id",
            "slack_conn_id": "test-slack-conn-id",
            "sql_hook_params": None,
            "parameters": None,
        }

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_query_results")
    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
    @pytest.mark.parametrize(
        "filename,df_method",
        [
            ("awesome.json", "to_json"),
            ("awesome.json.zip", "to_json"),
            ("awesome.csv", "to_csv"),
            ("awesome.csv.xz", "to_csv"),
            ("awesome.html", "to_html"),
        ],
    )
    @pytest.mark.parametrize("df_kwargs", [None, {}, {"foo": "bar"}])
    @pytest.mark.parametrize("channels", ["#random", "#random,#general", None])
    @pytest.mark.parametrize("initial_comment", [None, "Test Comment"])
    @pytest.mark.parametrize("title", [None, "Test File Title"])
    def test_send_file(
        self,
        mock_slack_hook_cls,
        mock_get_query_results,
        filename,
        df_method,
        df_kwargs,
        channels,
        initial_comment,
        title,
    ):
        # Mock Hook
        mock_send_file = mock.MagicMock()
        mock_slack_hook_cls.return_value.send_file = mock_send_file

        # Mock returns pandas.DataFrame and expected method
        mock_df = mock.MagicMock()
        mock_df_output_method = mock.MagicMock()
        setattr(mock_df, df_method, mock_df_output_method)
        mock_get_query_results.return_value = mock_df

        op_kwargs = {
            **self.default_op_kwargs,
            "slack_conn_id": "expected-test-slack-conn-id",
            "slack_filename": filename,
            "slack_channels": channels,
            "slack_initial_comment": initial_comment,
            "slack_title": title,
            "df_kwargs": df_kwargs,
        }
        op = SqlToSlackApiFileOperator(task_id="test_send_file", **op_kwargs)
        op.execute(mock.MagicMock())

        mock_slack_hook_cls.assert_called_once_with(slack_conn_id="expected-test-slack-conn-id")
        mock_get_query_results.assert_called_once_with()
        mock_df_output_method.assert_called_once_with(mock.ANY, **(df_kwargs or {}))
        mock_send_file.assert_called_once_with(
            channels=channels,
            filename=filename,
            initial_comment=initial_comment,
            title=title,
            file=mock.ANY,
        )

    @pytest.mark.parametrize(
        "filename",
        [
            "foo.parquet",
            "bat.parquet.snappy",
            "spam.xml",
            "egg.xlsx",
        ],
    )
    def test_unsupported_format(self, filename):
        op = SqlToSlackApiFileOperator(
            task_id="test_send_file", slack_filename=filename, **self.default_op_kwargs
        )
        with pytest.raises(ValueError):
            op.execute(mock.MagicMock())
