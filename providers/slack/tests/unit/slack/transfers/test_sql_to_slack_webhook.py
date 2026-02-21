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

from airflow.models import Connection
from airflow.providers.slack.transfers.sql_to_slack_webhook import SqlToSlackWebhookOperator

from tests_common.test_utils.compat import timezone

TEST_DAG_ID = "sql_to_slack_unit_test"
TEST_TASK_ID = "sql_to_slack_unit_test_task"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


@pytest.fixture
def mocked_hook():
    with mock.patch("airflow.providers.slack.transfers.sql_to_slack_webhook.SlackWebhookHook") as m:
        yield m


class TestSqlToSlackWebhookOperator:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="slack_connection",
                conn_type="slackwebhook",
                password="xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx",
            )
        )

    def setup_method(self):
        self.default_hook_parameters = {"timeout": None, "proxy": None, "retry_handlers": None}

    @staticmethod
    def _construct_operator(**kwargs):
        operator = SqlToSlackWebhookOperator(task_id=TEST_TASK_ID, **kwargs)
        return operator

    @pytest.mark.parametrize(
        ("slack_op_kwargs", "hook_extra_kwargs"),
        [
            pytest.param(
                {}, {"timeout": None, "proxy": None, "retry_handlers": None}, id="default-hook-parameters"
            ),
            pytest.param(
                {"slack_timeout": 42, "slack_proxy": "http://spam.egg", "slack_retry_handlers": []},
                {"timeout": 42, "proxy": "http://spam.egg", "retry_handlers": []},
                id="with-extra-hook-parameters",
            ),
        ],
    )
    def test_rendering_and_message_execution(self, slack_op_kwargs, hook_extra_kwargs, mocked_hook):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            **slack_op_kwargs,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.render_template_fields({"ds": "2017-01-01"})
        sql_to_slack_operator.execute({"ds": "2017-01-01"})

        # Test that the Slack hook is instantiated with the right parameters
        mocked_hook.assert_called_once_with(slack_webhook_conn_id="slack_connection", **hook_extra_kwargs)

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f"message: 2017-01-01, {test_df}",
            channel="#test",
        )

    def test_rendering_and_message_execution_with_slack_hook(self, mocked_hook):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ results_df }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook

        sql_to_slack_operator.render_template_fields({"ds": "2017-01-01"})
        sql_to_slack_operator.execute({"ds": "2017-01-01"})

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
        ("slack_webhook_conn_id", "warning_expected", "expected_conn_id"),
        [
            pytest.param("foo", False, "foo", id="slack-webhook-conn-id"),
            pytest.param("spam", True, "spam", id="mixin-conn-ids"),
        ],
    )
    def test_resolve_conn_ids(self, slack_webhook_conn_id, warning_expected, expected_conn_id):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        if slack_webhook_conn_id:
            operator_args["slack_webhook_conn_id"] = slack_webhook_conn_id

        op = self._construct_operator(**operator_args)

        assert op.slack_webhook_conn_id == expected_conn_id

    def test_non_existing_slack_webhook_conn_id(self):
        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_message": "message: {{ ds }}, {{ xxxx }}",
            "sql": "sql {{ ds }}",
        }
        with pytest.raises(ValueError, match="Got an empty `slack_webhook_conn_id` value"):
            self._construct_operator(**operator_args)

    def test_rendering_custom_df_name_message_execution(self, mocked_hook):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        operator_args = {
            "sql_conn_id": "snowflake_connection",
            "slack_webhook_conn_id": "slack_connection",
            "slack_message": "message: {{ ds }}, {{ testing }}",
            "slack_channel": "#test",
            "sql": "sql {{ ds }}",
            "results_df_name": "testing",
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mocked_hook.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook

        sql_to_slack_operator.render_template_fields({"ds": "2017-01-01"})
        sql_to_slack_operator.execute({"ds": "2017-01-01"})

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
        mocked_get_connection.return_value = Connection(conn_id="snowflake_connection", conn_type="snowflake")
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
        }
        sql_to_slack_operator = SqlToSlackWebhookOperator(task_id=TEST_TASK_ID, **operator_args)

        assert sql_to_slack_operator.sql_hook_params == hook_params

    def test_hook_params(self, mocked_get_connection):
        mocked_get_connection.return_value = Connection(conn_id="postgres_test", conn_type="postgres")
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
        mocked_get_connection.return_value = Connection(conn_id="snowflake_default", conn_type="snowflake")
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
