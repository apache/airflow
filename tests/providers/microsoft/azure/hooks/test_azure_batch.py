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
from azure.batch import BatchServiceClient, models as batch_models

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_batch import AzureBatchHook
from airflow.utils import db


class TestAzureBatchHook(unittest.TestCase):
    # set up the test environment
    def setUp(self):
        # set up the test variable
        self.test_vm_conn_id = "test_azure_batch_vm"
        self.test_cloud_conn_id = "test_azure_batch_cloud"
        self.test_account_name = "test_account_name"
        self.test_account_key = "test_account_key"
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

    def test_connection_and_client(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        self.assertIsInstance(hook._connection(), Connection)
        self.assertIsInstance(hook.get_conn(), BatchServiceClient)

    def test_configure_pool_with_vm_config(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        pool = hook.configure_pool(
            pool_id='mypool',
            vm_size="test_vm_size",
            target_dedicated_nodes=1,
        )
        self.assertIsInstance(pool, batch_models.PoolAddParameter)

    def test_configure_pool_with_cloud_config(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        pool = hook.configure_pool(
            pool_id='mypool',
            vm_size="test_vm_size",
            target_dedicated_nodes=1,
        )
        self.assertIsInstance(pool, batch_models.PoolAddParameter)

    def test_configure_pool_with_latest_vm(self):
        with mock.patch(
            "airflow.providers.microsoft.azure.hooks."
            "azure_batch.AzureBatchHook._get_latest_verified_image_vm_and_sku"
        ) as mock_getvm:
            hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
            getvm_instance = mock_getvm
            getvm_instance.return_value = ['test-image', 'test-sku']
            pool = hook.configure_pool(
                pool_id='mypool',
                vm_size="test_vm_size",
                use_latest_image_and_sku=True,
            )
            self.assertIsInstance(pool, batch_models.PoolAddParameter)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient")
    def test_create_pool_with_vm_config(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.pool.add
        pool = hook.configure_pool(
            pool_id='mypool',
            vm_size="test_vm_size",
            target_dedicated_nodes=1,
        )
        hook.create_pool(pool=pool)
        mock_instance.assert_called_once_with(pool)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient")
    def test_create_pool_with_cloud_config(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        mock_instance = mock_batch.return_value.pool.add
        pool = hook.configure_pool(
            pool_id='mypool',
            vm_size="test_vm_size",
            target_dedicated_nodes=1,
        )
        hook.create_pool(pool=pool)
        mock_instance.assert_called_once_with(pool)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient")
    def test_wait_for_all_nodes(self, mock_batch):
        # TODO: Add test
        pass

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient")
    def test_job_configuration_and_create_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.job.add
        job = hook.configure_job(job_id='myjob', pool_id='mypool')
        hook.create_job(job)
        self.assertIsInstance(job, batch_models.JobAddParameter)
        mock_instance.assert_called_once_with(job)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient')
    def test_add_single_task_to_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.task.add
        task = hook.configure_task(task_id="mytask", command_line="echo hello")
        hook.add_single_task_to_job(job_id='myjob', task=task)
        self.assertIsInstance(task, batch_models.TaskAddParameter)
        mock_instance.assert_called_once_with(job_id="myjob", task=task)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_batch.BatchServiceClient')
    def test_wait_for_all_task_to_complete(self, mock_batch):
        # TODO: Add test
        pass
