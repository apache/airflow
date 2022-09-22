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
from airflow.providers.slack.transfers.sql_to_slack import SqlToSlackOperator
from airflow.utils import timezone

TEST_DAG_ID = 'sql_to_slack_unit_test'
TEST_TASK_ID = 'sql_to_slack_unit_test_task'
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSqlToSlackOperator:
    def setup_method(self):
        self.example_dag = DAG(TEST_DAG_ID, start_date=DEFAULT_DATE)

    @staticmethod
    def _construct_operator(**kwargs):
        operator = SqlToSlackOperator(task_id=TEST_TASK_ID, **kwargs)
        return operator

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_rendering_and_message_execution(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({'a': '1', 'b': '2'}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            'sql_conn_id': 'snowflake_connection',
            'slack_conn_id': 'slack_connection',
            'slack_message': 'message: {{ ds }}, {{ results_df }}',
            'slack_channel': '#test',
            'sql': "sql {{ ds }}",
            'dag': self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id='slack_connection',
            webhook_token=None,
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f'message: 2017-01-01, {test_df}',
            channel='#test',
        )

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_rendering_and_message_execution_with_slack_hook(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({'a': '1', 'b': '2'}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            'sql_conn_id': 'snowflake_connection',
            'slack_conn_id': 'slack_connection',
            'slack_webhook_token': 'test_token',
            'slack_message': 'message: {{ ds }}, {{ results_df }}',
            'slack_channel': '#test',
            'sql': "sql {{ ds }}",
            'dag': self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id='slack_connection',
            webhook_token='test_token',
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f'message: 2017-01-01, {test_df}',
            channel='#test',
        )

    def test_non_existing_slack_parameters_provided_exception_thrown(self):
        operator_args = {
            'sql_conn_id': 'snowflake_connection',
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'sql': "sql {{ ds }}",
        }
        with pytest.raises(AirflowException):
            self._construct_operator(**operator_args)

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_rendering_custom_df_name_message_execution(self, mock_slack_hook_class):
        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({'a': '1', 'b': '2'}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df

        operator_args = {
            'sql_conn_id': 'snowflake_connection',
            'slack_conn_id': 'slack_connection',
            'slack_message': 'message: {{ ds }}, {{ testing }}',
            'slack_channel': '#test',
            'sql': "sql {{ ds }}",
            'results_df_name': 'testing',
            'dag': self.example_dag,
        }
        sql_to_slack_operator = self._construct_operator(**operator_args)

        slack_webhook_hook = mock_slack_hook_class.return_value
        sql_to_slack_operator._get_hook = mock_dbapi_hook
        sql_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id='slack_connection',
            webhook_token=None,
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text=f'message: 2017-01-01, {test_df}',
            channel='#test',
        )

    @mock.patch('airflow.providers.common.sql.operators.sql.BaseHook.get_connection')
    def test_hook_params_building(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id='snowflake_connection', conn_type='snowflake')
        hook_params = {
            'schema': 'test_schema',
            'role': 'test_role',
            'database': 'test_database',
            'warehouse': 'test_warehouse',
        }
        operator_args = {
            'sql_conn_id': 'dummy_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'sql_hook_params': hook_params,
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_webhook_token': 'test_token',
            'dag': self.example_dag,
        }
        sql_to_slack_operator = SqlToSlackOperator(task_id=TEST_TASK_ID, **operator_args)

        assert sql_to_slack_operator.sql_hook_params == hook_params

    @mock.patch('airflow.providers.common.sql.operators.sql.BaseHook.get_connection')
    def test_hook_params(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id='postgres_test', conn_type='postgres')
        op = SqlToSlackOperator(
            task_id='sql_hook_params',
            sql_conn_id='postgres_test',
            slack_webhook_token='slack_token',
            sql="SELECT 1",
            slack_message='message: {{ ds }}, {{ xxxx }}',
            sql_hook_params={
                'schema': 'public',
            },
        )
        hook = op._get_hook()
        assert hook.schema == 'public'

    @mock.patch('airflow.providers.common.sql.operators.sql.BaseHook.get_connection')
    def test_hook_params_snowflake(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id='snowflake_default', conn_type='snowflake')
        op = SqlToSlackOperator(
            task_id='snowflake_hook_params',
            sql_conn_id='snowflake_default',
            slack_conn_id='slack_default',
            results_df_name='xxxx',
            sql="SELECT 1",
            slack_message='message: {{ ds }}, {{ xxxx }}',
            sql_hook_params={
                'warehouse': 'warehouse',
                'database': 'database',
                'role': 'role',
                'schema': 'schema',
            },
        )
        hook = op._get_hook()

        assert hook.warehouse == 'warehouse'
        assert hook.database == 'database'
        assert hook.role == 'role'
        assert hook.schema == 'schema'
