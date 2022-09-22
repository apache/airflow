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

from airflow.models import DAG
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils import timezone
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs

TEST_DAG_ID = 'snowflake_to_slack_unit_test'
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSnowflakeToSlackOperator:
    def setup_class(self):
        clear_db_runs()

    def setup_method(self):
        self.example_dag = DAG('unit_test_dag_snowflake_to_slack', start_date=DEFAULT_DATE)

    def teardown_method(self):
        clear_db_runs()

    @staticmethod
    def _construct_operator(**kwargs):
        with conf_vars({('operators', 'allow_illegal_arguments'): 'True'}):
            operator = SnowflakeToSlackOperator(task_id=TEST_DAG_ID, **kwargs)
            return operator

    @mock.patch('airflow.providers.slack.transfers.sql_to_slack.SlackWebhookHook')
    def test_hooks_and_rendering(self, mock_slack_hook_class):
        slack_webhook_hook = mock_slack_hook_class.return_value
        operator_args = {
            'snowflake_conn_id': 'snowflake_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'warehouse': 'test_warehouse',
            'database': 'test_database',
            'role': 'test_role',
            'schema': 'test_schema',
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_token': 'test_token',
            'dag': self.example_dag,
        }
        snowflake_to_slack_operator = self._construct_operator(**operator_args)

        mock_dbapi_hook = mock.Mock()
        snowflake_to_slack_operator._get_hook = mock_dbapi_hook

        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = '1234'

        snowflake_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            slack_webhook_conn_id='slack_default',
            webhook_token='test_token',
        )

        # Test that the `SlackWebhookHook.send` method gets run once
        slack_webhook_hook.send.assert_called_once_with(
            text='message: 2017-01-01, 1234',
            channel=None,
        )

    def test_hook_params_building(self):
        hook_params = {
            'schema': 'test_schema',
            'role': 'test_role',
            'database': 'test_database',
            'warehouse': 'test_warehouse',
        }
        operator_args = {
            'snowflake_conn_id': 'snowflake_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'warehouse': hook_params['warehouse'],
            'database': hook_params['database'],
            'role': hook_params['role'],
            'schema': hook_params['schema'],
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_token': 'test_token',
            'dag': self.example_dag,
        }
        snowflake_operator = self._construct_operator(**operator_args)

        assert snowflake_operator.sql_hook_params == hook_params

    def test_partial_hook_params_building(self):
        hook_params = {'role': 'test_role', 'database': 'test_database', 'warehouse': 'test_warehouse'}
        operator_args = {
            'snowflake_conn_id': 'snowflake_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'warehouse': hook_params['warehouse'],
            'database': hook_params['database'],
            'role': hook_params['role'],
            'schema': None,
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_token': 'test_token',
            'dag': self.example_dag,
        }
        snowflake_operator = self._construct_operator(**operator_args)

        assert snowflake_operator.sql_hook_params == hook_params

    def test_no_hook_params_building(self):
        operator_args = {
            'snowflake_conn_id': 'snowflake_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_token': 'test_token',
            'dag': self.example_dag,
        }
        snowflake_operator = self._construct_operator(**operator_args)

        assert snowflake_operator.sql_hook_params == {}
