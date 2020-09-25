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
import json
import unittest

import mock

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_batch import AzureBatchHook
from airflow.providers.microsoft.azure.operators.azure_batch import AzureBatchOperator
from airflow.utils import db

TASK_ID = "MyDag"
BATCH_POOL_ID = "MyPool"
BATCH_JOB_ID = "MyJob"
BATCH_TASK_ID = "MyTask"
BATCH_VM_SIZE = "Standard"


class TestAzureBatchOperator(unittest.TestCase):
    # set up the test environment
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.AzureBatchHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient")
    def setUp(self, mock_batch, mock_hook):
        # set up the test variable
        self.test_vm_conn_id = "test_azure_batch_vm2"
        self.test_cloud_conn_id = "test_azure_batch_cloud2"
        self.test_account_name = "test_account_name"
        self.test_account_key = 'test_account_key'
        self.test_account_url = "http://test-endpoint:29000"
        self.test_vm_size = "test-vm-size"
        self.test_vm_publisher = "test.vm.publisher"
        self.test_vm_offer = "test.vm.offer"
        self.test_vm_sku = "test-sku"
        self.test_cloud_os_family = "test-family"
        self.test_cloud_os_version = "test-version"
        self.test_node_agent_sku = "test-node-agent-sku"

        # connect with vm configuration
        db.merge_conn(
            Connection(
                conn_id=self.test_vm_conn_id,
                conn_type="azure_batch",
                extra=json.dumps(
                    {
                        "account_name": self.test_account_name,
                        "account_key": self.test_account_key,
                        "account_url": self.test_account_url,
                        "vm_publisher": self.test_vm_publisher,
                        "vm_offer": self.test_vm_offer,
                        "vm_sku": self.test_vm_sku,
                        "node_agent_sku_id": self.test_node_agent_sku,
                    }
                ),
            )
        )
        # connect with cloud service
        db.merge_conn(
            Connection(
                conn_id=self.test_cloud_conn_id,
                conn_type="azure_batch",
                extra=json.dumps(
                    {
                        "account_name": self.test_account_name,
                        "account_key": self.test_account_key,
                        "account_url": self.test_account_url,
                        "os_family": self.test_cloud_os_family,
                        "os_version": self.test_cloud_os_version,
                        "node_agent_sku_id": self.test_node_agent_sku,
                    }
                ),
            )
        )
        self.operator = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            timeout=2,
        )
        self.batch_client = mock_batch.return_value
        self.assertEqual(self.batch_client, self.operator.hook.connection)

    @mock.patch.object(AzureBatchHook, 'wait_for_all_node_state')
    def test_execute_without_failures(self, wait_mock):
        wait_mock.return_value = True  # No wait
        self.operator.execute(None)
        # test pool creation
        self.batch_client.pool.add.assert_called()
        self.batch_client.job.add.assert_called()
        self.batch_client.task.add.assert_called()

    @mock.patch.object(AzureBatchHook, 'wait_for_all_node_state')
    def test_execute_with_failures(self, wait_mock):
        wait_mock.return_value = True  # No wait
        # Remove pool id
        self.operator.batch_pool_id = None

        # test that it raises
        with self.assertRaises(AirflowException):
            self.operator.execute(None)

    @mock.patch.object(AzureBatchHook, 'wait_for_all_node_state')
    @mock.patch.object(AzureBatchOperator, "clean_up")
    def test_execute_with_cleaning(self, mock_clean, wait_mock):
        wait_mock.return_value = True  # No wait
        # Remove pool id
        self.operator.should_delete_job = True
        self.operator.execute(None)
        mock_clean.assert_called()
        mock_clean.assert_called_once_with(job_id=BATCH_JOB_ID)
