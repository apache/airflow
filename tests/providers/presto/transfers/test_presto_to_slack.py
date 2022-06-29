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

from airflow.models import DAG
from airflow.providers.presto.transfers.presto_to_slack import PrestoToSlackOperator
from airflow.utils import timezone
from tests.test_utils.db import clear_db_runs

TEST_DAG_ID = 'presto_to_slack_unit_test'
DEFAULT_DATE = timezone.datetime(2022, 1, 1)


class TestPrestoToSlackOperator:
    def setup_class(self):
        clear_db_runs()

    def setup_method(self):
        self.example_dag = DAG('unit_test_dag_presto_to_slack', start_date=DEFAULT_DATE)

    def teardown_method(self):
        clear_db_runs()

    @staticmethod
    def _construct_operator(**kwargs):
        operator = PrestoToSlackOperator(task_id=TEST_DAG_ID, **kwargs)
        return operator

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_hooks_and_rendering_with_slack_conn(self, mock_slack_hook_class):
        operator_args = {
            'presto_conn_id': 'presto_connection',
            'slack_conn_id': 'slack_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_channel': 'my_channel',
            'dag': self.example_dag,
        }
        presto_to_slack_operator = self._construct_operator(**operator_args)
        mock_dbapi_hook = mock.Mock()
        presto_to_slack_operator._get_hook = mock_dbapi_hook

        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = '1234'

        slack_webhook_hook = mock_slack_hook_class.return_value
        presto_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        mock_slack_hook_class.assert_called_once_with(
            http_conn_id='slack_connection',
            message='message: 2022-01-01, 1234',
            channel='my_channel',
            webhook_token=None,
        )

        slack_webhook_hook.execute.assert_called_once()

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_hooks_and_rendering_with_slack_conn_and_webhook(self, mock_slack_hook_class):
        operator_args = {
            'presto_conn_id': 'presto_connection',
            'slack_conn_id': 'slack_connection',
            'slack_token': 'test_token',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_channel': 'my_channel',
            'dag': self.example_dag,
        }
        presto_to_slack_operator = self._construct_operator(**operator_args)
        mock_dbapi_hook = mock.Mock()
        presto_to_slack_operator._get_hook = mock_dbapi_hook

        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = '1234'

        slack_webhook_hook = mock_slack_hook_class.return_value
        presto_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        mock_slack_hook_class.assert_called_once_with(
            http_conn_id='slack_connection',
            message='message: 2022-01-01, 1234',
            channel='my_channel',
            webhook_token='test_token',
        )

        slack_webhook_hook.execute.assert_called_once()
