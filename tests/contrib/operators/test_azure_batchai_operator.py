# -*- coding: utf-8 -*-
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

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.contrib.operators.azure_batchai_operator import AzureBatchAIOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

CONFIG_DATA = {
    "clientId": "Id",
    "clientSecret": "secret",
    "tenantId": "tenant",
    "subscription_id": "subscription",
}


class TestAzureBatchAIOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.azure_batchai_operator.AzureBatchAIHook')
    def setUp(self, azure_batchai_hook_mock):
        configuration.load_test_config()

        self.azure_batchai_hook_mock = azure_batchai_hook_mock(CONFIG_DATA)
        self.batch = AzureBatchAIOperator('azure_default',
                                          'batch-ai-test-rg',
                                          'batch-ai-workspace',
                                          'batch-ai-cluster',
                                          'eastus',
                                          'auto',
                                          environment_variables=CONFIG_DATA,
                                          volumes=[],
                                          task_id='test_operator')

    @mock.patch('airflow.contrib.operators.azure_batchai_operator.AzureBatchAIHook')
    def test_execute(self, abai_mock):
        abai_mock.return_value.get_state_exitcode.return_value = 'Terminated', 0
        self.batch = AzureBatchAIOperator('azure_default',
                                          'batch-ai-test-rg',
                                          'batch-ai-workspace',
                                          'batch-ai-cluster',
                                          'eastus',
                                          'auto',
                                          environment_variables={
                                              'USERNAME': 'azureuser',
                                              'PASSWORD': 'azurepass'
                                          },
                                          volumes=[],
                                          task_id='test_operator')
        self.batch.execute()

        self.assertEqual(self.batch.resource_group, 'batch-ai-test-rg')
        self.assertEqual(self.batch.workspace_name, 'batch-ai-workspace')
        self.assertEqual(self.batch.cluster_name, 'batch-ai-cluster')
        self.assertEqual(self.batch.location, 'eastus')
        self.assertEqual(self.batch.scale_type, 'auto')

    @mock.patch('airflow.contrib.operators.azure_batchai_operator.AzureBatchAIHook')
    def test_execute_with_failures(self, abai_mock):
        abai_mock.return_value.get_state_exitcode.return_value = "Terminated", 1
        self.batch = AzureBatchAIOperator('azure_default',
                                          'batch-ai-test-rg',
                                          'batch-ai-workspace',
                                          'batch-ai-cluster',
                                          'eastus',
                                          'auto',
                                          environment_variables={
                                              'USERNAME': 'azureuser',
                                              'PASSWORD': 'azurepass'
                                          },
                                          volumes=[],
                                          task_id='test_operator')

        with self.assertRaises(AirflowException):
            self.batch.execute()


if __name__ == '__main__':
    unittest.main()
