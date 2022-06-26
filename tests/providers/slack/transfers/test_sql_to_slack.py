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

from unittest import mock

import pandas as pd
import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
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
            http_conn_id='slack_connection', message=f'message: 2017-01-01, {test_df}', channel='#test'
        )

        # Test that the Slack hook's execute method gets run once
        slack_webhook_hook.execute.assert_called_once()

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_duplicated_slack_parameters_provided_exception_thrown(self, mock_slack_hook_class):
        operator_args = {
            'sql_conn_id': 'snowflake_connection',
            'slack_conn_id': 'slack_connection',
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'sql': "sql {{ ds }}",
            'slack_webhook_token': 'test_token',
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
            http_conn_id='slack_connection', message=f'message: 2017-01-01, {test_df}', channel='#test'
        )

        # Test that the Slack hook's execute method gets run once
        slack_webhook_hook.execute.assert_called_once()
