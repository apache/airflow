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
#

import unittest
from unittest import mock

from airflow.models import DAG, TaskInstance
from airflow.providers.microsoft.azure.hooks.adx import AzureDataExplorerHook
from airflow.providers.microsoft.azure.operators.adx import AzureDataExplorerQueryOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2019, 1, 1)

MOCK_DATA = {
    'task_id': 'test_azure_data_explorer_query_operator',
    'query': 'Logs | schema',
    'database': 'Database',
    'options': {
        'option1': 'option_value'
    }
}

MOCK_RESULT = {
    'name': 'getschema',
    'kind': 'PrimaryResult',
    'data': [{
        'ColumnName': 'Source',
        'ColumnOrdinal': 0,
        'DataType': 'System.String',
        'ColumnType': 'string'
    }, {
        'ColumnName': 'Timestamp',
        'ColumnOrdinal': 1,
        'DataType': 'System.DateTime',
        'ColumnType': 'datetime'
    }]
}


class MockResponse:
    primary_results = [MOCK_RESULT]


class TestAzureDataExplorerQueryOperator(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }

        self.dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once',
                       default_args=args,
                       schedule_interval='@once')
        self.operator = AzureDataExplorerQueryOperator(dag=self.dag,
                                                       **MOCK_DATA)

    def test_init(self):
        self.assertEqual(self.operator.task_id, MOCK_DATA['task_id'])
        self.assertEqual(self.operator.query, MOCK_DATA['query'])
        self.assertEqual(self.operator.database, MOCK_DATA['database'])
        self.assertEqual(self.operator.azure_data_explorer_conn_id, 'azure_data_explorer_default')

    @mock.patch.object(AzureDataExplorerHook, 'run_query', return_value=MockResponse())
    @mock.patch.object(AzureDataExplorerHook, 'get_conn')
    def test_run_query(self, mock_conn, mock_run_query):
        self.operator.execute(None)
        mock_run_query.assert_called_once_with(MOCK_DATA['query'],
                                               MOCK_DATA['database'],
                                               MOCK_DATA['options'])

    @mock.patch.object(AzureDataExplorerHook, 'run_query', return_value=MockResponse())
    @mock.patch.object(AzureDataExplorerHook, 'get_conn')
    def test_xcom_push_and_pull(self, mock_conn, mock_run_query):
        ti = TaskInstance(task=self.operator, execution_date=timezone.utcnow())
        ti.run()

        self.assertEqual(ti.xcom_pull(task_ids=MOCK_DATA['task_id']),
                         MOCK_RESULT)


if __name__ == '__main__':
    unittest.main()
