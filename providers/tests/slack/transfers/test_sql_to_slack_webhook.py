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

from contextlib import nullcontext
from unittest import mock

import pandas as pd
import pytest

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.slack.transfers.sql_to_slack_webhook import (
    SqlToSlackWebhookOperator,
)
from airflow.utils import timezone
from airflow.utils.task_instance_session import set_current_task_instance_session

TEST_DAG_ID = "sql_to_slack_unit_test"
TEST_TASK_ID = "sql_to_slack_unit_test_task"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


@pytest.fixture
def mocked_hook():
    with mock.patch(
        "airflow.providers.slack.transfers.sql_to_slack_webhook.SlackWebhookHook"
    ) as m:
        yield m


@pytest.mark.db_test
class TestSqlToSlackWebhookOperator:
    def setup_method(self):
        self.example_dag = DAG(TEST_DAG_ID, schedule=None, start_date=DEFAULT_DATE)
        self.default_hook_parameters = {
            "timeout": None,
            "proxy": None,
            "retry_handlers": None,
        }

    @staticmethod
    def _construct_operator(**kwargs):
        operator = SqlToSlackWebhookOperator(task_id=TEST_TASK_ID, **kwargs)
        return operator

    @pytest.mark.parametrize(
        "slack_op_kwargs, hook_extra_kwargs",
        [
            pytest.param(
                {},
                {"timeout": None, "proxy": None, "retry_handlers": None},
                id="default-hook-parameters",
            ),
            pytest.param(
                {
                    "slack_timeout": 42,
                    "slack_proxy": "http://spam.egg",
                    "slack_retry_handlers": [],
                },
                {"timeout": 42, "proxy": "http://spam.egg", "retry_handlers": []},
                id="with-extra-hook-parameters",
            ),
        ],
    )
    def test_rendering_and_message_execution(
        self, slack_op_kwargs, hook_extra_kwargs, mocked_hook
    ):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "dag": self.example_dag,
            **slack_op_kwargs,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

        # Test that the Slack hook is instantiated with the right parameters
        mocked_hook.assert_called_once_with(
            slack_webhook_conn_id="slack_connection", **hook_extra_kwargs
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    def test_rendering_and_message_execution_with_slack_hook(self, mocked_hook):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

        # Test that the Slack hook is instantiated with the right parameters
        mocked_hook.assert_called_once_with(
            slack_webhook_conn_id="slack_connection", **self.default_hook_parameters
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    @pytest.mark.parametrize(
        "slack_webhook_conn_id, slack_conn_id, warning_expected, expected_conn_id",
        [
            pytest.param("foo", None, False, "foo", id="slack-webhook-conn-id"),
            pytest.param(None, "bar", True, "bar", id="slack-conn-id"),
            pytest.param("spam", "spam", True, "spam", id="mixin-conn-ids"),
        ],
    )
    def test_resolve_conn_ids(
        self, slack_webhook_conn_id, slack_conn_id, warning_expected, expected_conn_id
    ):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        if slack_webhook_conn_id:
            operator_args["slack_webhook_conn_id"] = slack_webhook_conn_id
        if slack_conn_id:
            operator_args["slack_conn_id"] = slack_conn_id
        ctx = (
            pytest.warns(
                AirflowProviderDeprecationWarning,
                match="Parameter `slack_conn_id` is deprecated",
            )
            if warning_expected
            else nullcontext()
        )

        with ctx:
            op = self._construct_operator(**operator_args)

        assert op.slack_webhook_conn_id == expected_conn_id
        with pytest.warns(
            AirflowProviderDeprecationWarning, match="slack_conn_id` property deprecated"
        ):
            assert op.slack_conn_id == expected_conn_id

    def test_conflicting_conn_id(self):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        with (
            pytest.raises(ValueError, match="Conflicting Connection ids provided"),
            pytest.warns(
                AirflowProviderDeprecationWarning,
                match="Parameter `slack_conn_id` is deprecated",
            ),
        ):
            self._construct_operator(
                **operator_args, slack_webhook_conn_id="foo", slack_conn_id="bar"
            )

    def test_non_existing_slack_webhook_conn_id(self):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        with pytest.raises(
            ValueError, match="Got an empty `slack_webhook_conn_id` value"
        ):
            self._construct_operator(**operator_args)

    def test_rendering_custom_df_name_message_execution(self, mocked_hook):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ testing }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "results_df_name": "testing",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

        # Test that the Slack hook is instantiated with the right parameters
        mocked_hook.assert_called_once_with(
            slack_webhook_conn_id="slack_connection", **self.default_hook_parameters
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    def test_hook_params_building(self, mocked_get_connection):
        mocked_get_connection.return_value = Connection(
            conn_id="snowflake_connection", conn_type="snowflake"
        )
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
            "slack_webhook_conn_id": "slack_connection",
            "parameters": ["1", "2", "3"],
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "dag": self.example_dag,
        }
        sql_to_slack_operator = SqlToSlackWebhookOperator(
            task_id=TEST_TASK_ID, **operator_args
        )

        assert sql_to_slack_operator.sql_hook_params == hook_params

    def test_hook_params(self, mocked_get_connection):
        mocked_get_connection.return_value = Connection(
            conn_id="postgres_test", conn_type="postgres"
        )
        op = SqlToSlackWebhookOperator(
            task_id="sql_hook_params",
            sql_conn_id="postgres_test",
            slack_webhook_conn_id="slack_connection",
            sql="SELECT 1",
            slack_message="message: {{ ds }}, {{ xxxx }}",
            sql_hook_params={
                "log_sql": False,
            },
        )
        hook = op._get_hook()
        assert hook.log_sql == op.sql_hook_params["log_sql"]

    def test_hook_params_snowflake(self, mocked_get_connection):
        mocked_get_connection.return_value = Connection(
            conn_id="snowflake_default", conn_type="snowflake"
        )
        op = SqlToSlackWebhookOperator(
            task_id="snowflake_hook_params",
            sql_conn_id="snowflake_default",
            slack_webhook_conn_id="slack_default",
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

    @pytest.mark.parametrize(
        "slack_conn_id, slack_webhook_conn_id",
        [
            pytest.param("slack_conn_id", None, id="slack-conn-id-only"),
            pytest.param("slack_conn_id", "slack_conn_id", id="non-ambiguous-params"),
        ],
    )
    def test_partial_deprecated_slack_conn_id(
        self, slack_conn_id, slack_webhook_conn_id, dag_maker, session
    ):
        with dag_maker(dag_id="test_partial_deprecated_slack_conn_id", session=session):
            SqlToSlackWebhookOperator.partial(
                task_id="fake-task-id",
                slack_conn_id=slack_conn_id,
                slack_webhook_conn_id=slack_webhook_conn_id,
                sql_conn_id="fake-sql-conn-id",
                slack_message="<https://github.com/apache/airflow|Apache Airflow®>",
            ).expand(sql=["SELECT 1", "SELECT 2"])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        with set_current_task_instance_session(session=session):
            warning_match = r"Parameter `slack_conn_id` is deprecated"
            for ti in tis:
                with pytest.warns(AirflowProviderDeprecationWarning, match=warning_match):
                    ti.render_templates()
                assert ti.task.slack_webhook_conn_id == slack_conn_id

    def test_partial_ambiguous_slack_connections(self, dag_maker, session):
        with dag_maker("test_partial_ambiguous_slack_connections", session=session):
            SqlToSlackWebhookOperator.partial(
                task_id="fake-task-id",
                slack_conn_id="slack_conn_id",
                slack_webhook_conn_id="slack_webhook_conn_id",
                sql_conn_id="fake-sql-conn-id",
                slack_message="<https://github.com/apache/airflow|Apache Airflow®>",
            ).expand(sql=["SELECT 1", "SELECT 2"])

        dr = dag_maker.create_dagrun(session=session)
        tis = dr.get_task_instances(session=session)
        with set_current_task_instance_session(session=session):
            warning_match = r"Parameter `slack_conn_id` is deprecated"
            for ti in tis:
                with (
                    pytest.warns(AirflowProviderDeprecationWarning, match=warning_match),
                    pytest.raises(
                        ValueError, match="Conflicting Connection ids provided"
                    ),
                ):
                    ti.render_templates()
