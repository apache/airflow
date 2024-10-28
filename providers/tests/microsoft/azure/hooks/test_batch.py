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
from __future__ import annotations

from unittest import mock
from unittest.mock import PropertyMock

import pytest
from azure.batch import BatchServiceClient, models as batch_models

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook

MODULE = "airflow.providers.microsoft.azure.hooks.batch"


class TestAzureBatchHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connections):
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

        create_mock_connections(
            # connect with vm configuration
            Connection(
                conn_id=self.test_vm_conn_id,
                conn_type="azure-batch",
                extra={"account_url": self.test_account_url},
            ),
            # connect with cloud service
            Connection(
                conn_id=self.test_cloud_conn_id,
                conn_type="azure-batch",
                extra={"account_url": self.test_account_url},
            ),
        )

    def test_connection_and_client(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        assert isinstance(hook.get_conn(), BatchServiceClient)
        conn = hook.connection
        assert isinstance(conn, BatchServiceClient)
        assert hook.connection is conn, "`connection` property should be cached"

    @mock.patch(f"{MODULE}.batch_auth.SharedKeyCredentials")
    @mock.patch(f"{MODULE}.AzureIdentityCredentialAdapter")
    def test_fallback_to_azure_identity_credential_adppter_when_name_and_key_is_not_provided(
        self, mock_azure_identity_credential_adapter, mock_shared_key_credentials
    ):
        self.test_account_name = None
        self.test_account_key = None

        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        assert isinstance(hook.get_conn(), BatchServiceClient)
        mock_azure_identity_credential_adapter.assert_called_with(
            None,
            resource_id="https://batch.core.windows.net/.default",
            managed_identity_client_id=None,
            workload_identity_tenant_id=None,
        )
        assert not mock_shared_key_credentials.auth.called

        self.test_account_name = "test_account_name"
        self.test_account_key = "test_account_key"

    def test_configure_pool_with_vm_config(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        pool = hook.configure_pool(
            pool_id="mypool",
            vm_size="test_vm_size",
            vm_node_agent_sku_id=self.test_vm_sku,
            target_dedicated_nodes=1,
            vm_publisher="test.vm.publisher",
            vm_offer="test.vm.offer",
            sku_starts_with="test-sku",
        )
        assert isinstance(pool, batch_models.PoolAddParameter)

    def test_configure_pool_with_cloud_config(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        pool = hook.configure_pool(
            pool_id="mypool",
            vm_size="test_vm_size",
            vm_node_agent_sku_id=self.test_vm_sku,
            target_dedicated_nodes=1,
            vm_publisher="test.vm.publisher",
            vm_offer="test.vm.offer",
            sku_starts_with="test-sku",
        )
        assert isinstance(pool, batch_models.PoolAddParameter)

    def test_configure_pool_with_latest_vm(self):
        with mock.patch(
            "airflow.providers.microsoft.azure.hooks."
            "batch.AzureBatchHook._get_latest_verified_image_vm_and_sku"
        ) as mock_getvm:
            hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
            getvm_instance = mock_getvm
            getvm_instance.return_value = ["test-image", "test-sku"]
            pool = hook.configure_pool(
                pool_id="mypool",
                vm_size="test_vm_size",
                vm_node_agent_sku_id=self.test_vm_sku,
                use_latest_image_and_sku=True,
                vm_publisher="test.vm.publisher",
                vm_offer="test.vm.offer",
                sku_starts_with="test-sku",
            )
            assert isinstance(pool, batch_models.PoolAddParameter)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_create_pool_with_vm_config(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.pool.add
        pool = hook.configure_pool(
            pool_id="mypool",
            vm_size="test_vm_size",
            vm_node_agent_sku_id=self.test_vm_sku,
            target_dedicated_nodes=1,
            vm_publisher="test.vm.publisher",
            vm_offer="test.vm.offer",
            sku_starts_with="test-sku",
        )
        hook.create_pool(pool=pool)
        mock_instance.assert_called_once_with(pool)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_create_pool_with_cloud_config(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        mock_instance = mock_batch.return_value.pool.add
        pool = hook.configure_pool(
            pool_id="mypool",
            vm_size="test_vm_size",
            vm_node_agent_sku_id=self.test_vm_sku,
            target_dedicated_nodes=1,
            vm_publisher="test.vm.publisher",
            vm_offer="test.vm.offer",
            sku_starts_with="test-sku",
        )
        hook.create_pool(pool=pool)
        mock_instance.assert_called_once_with(pool)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_wait_for_all_nodes(self, mock_batch):
        # TODO: Add test
        pass

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_job_configuration_and_create_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.job.add
        job = hook.configure_job(job_id="myjob", pool_id="mypool")
        hook.create_job(job)
        assert isinstance(job, batch_models.JobAddParameter)
        mock_instance.assert_called_once_with(job)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_add_single_task_to_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_instance = mock_batch.return_value.task.add
        task = hook.configure_task(task_id="mytask", command_line="echo hello")
        hook.add_single_task_to_job(job_id="myjob", task=task)
        assert isinstance(task, batch_models.TaskAddParameter)
        mock_instance.assert_called_once_with(job_id="myjob", task=task)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_wait_for_all_task_to_complete_timeout(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        with pytest.raises(TimeoutError):
            hook.wait_for_job_tasks_to_complete("myjob", -1)

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_wait_for_all_task_to_complete_all_success(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        hook.connection.task.list.return_value = iter(
            [
                batch_models.CloudTask(
                    id="mytask_1",
                    execution_info=batch_models.TaskExecutionInformation(
                        retry_count=0,
                        requeue_count=0,
                        result=batch_models.TaskExecutionResult.success,
                    ),
                    state=batch_models.TaskState.completed,
                ),
                batch_models.CloudTask(
                    id="mytask_2",
                    execution_info=batch_models.TaskExecutionInformation(
                        retry_count=0,
                        requeue_count=0,
                        result=batch_models.TaskExecutionResult.success,
                    ),
                    state=batch_models.TaskState.completed,
                ),
            ]
        )

        results = hook.wait_for_job_tasks_to_complete("myjob", 60)
        assert results == []
        hook.connection.task.list.assert_called_once_with("myjob")

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_wait_for_all_task_to_complete_failures(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        tasks = [
            batch_models.CloudTask(
                id="mytask_1",
                execution_info=batch_models.TaskExecutionInformation(
                    retry_count=0,
                    requeue_count=0,
                    result=batch_models.TaskExecutionResult.success,
                ),
                state=batch_models.TaskState.completed,
            ),
            batch_models.CloudTask(
                id="mytask_2",
                execution_info=batch_models.TaskExecutionInformation(
                    retry_count=0,
                    requeue_count=0,
                    result=batch_models.TaskExecutionResult.failure,
                ),
                state=batch_models.TaskState.completed,
            ),
        ]
        hook.connection.task.list.return_value = iter(tasks)

        results = hook.wait_for_job_tasks_to_complete("myjob", 60)
        assert results == [tasks[1]]
        hook.connection.task.list.assert_called_once_with("myjob")

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_connection_success(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        hook.connection.job.return_value = {}
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure Batch."

    @mock.patch(f"{MODULE}.BatchServiceClient")
    def test_connection_failure(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_cloud_conn_id)
        hook.connection.job.list = PropertyMock(
            side_effect=Exception("Authentication failed.")
        )
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."
