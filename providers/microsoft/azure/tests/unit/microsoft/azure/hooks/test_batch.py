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
from azure.batch import BatchClient, models as batch_models

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook

MODULE = "airflow.providers.microsoft.azure.hooks.batch"


class TestAzureBatchHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connections):
        # set up the test variable
        self.test_vm_conn_id = "test_azure_batch_vm"
        self.test_account_name = "test_account_name"
        self.test_account_key = "test_account_key"
        self.test_account_url = "http://test-endpoint:29000"
        self.test_vm_size = "test-vm-size"
        self.test_vm_publisher = "test.vm.publisher"
        self.test_vm_offer = "test.vm.offer"
        self.test_vm_sku = "test-sku"
        self.test_node_agent_sku = "test-node-agent-sku"

        create_mock_connections(
            Connection(
                conn_id=self.test_vm_conn_id,
                conn_type="azure-batch",
                login=self.test_account_name,
                password=self.test_account_key,
                extra={"account_url": self.test_account_url},
            ),
        )

    def test_connection_and_client(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        assert isinstance(hook.get_conn(), BatchClient)
        conn = hook.connection
        assert isinstance(conn, BatchClient)
        assert hook.connection is conn, "`connection` property should be cached"

    @mock.patch(f"{MODULE}.AzureNamedKeyCredential")
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential")
    def test_fallback_to_azure_identity_credential_when_name_and_key_is_not_provided(
        self, mock_default_credential, mock_named_key_credential, create_mock_connections
    ):
        conn_id = "test_azure_batch_no_creds"
        create_mock_connections(
            Connection(
                conn_id=conn_id,
                conn_type="azure-batch",
                extra={"account_url": self.test_account_url},
            ),
        )

        hook = AzureBatchHook(azure_batch_conn_id=conn_id)
        assert isinstance(hook.get_conn(), BatchClient)
        mock_default_credential.assert_called_with(
            managed_identity_client_id=None,
            workload_identity_tenant_id=None,
        )
        mock_named_key_credential.assert_not_called()

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
        assert isinstance(pool, batch_models.BatchPoolCreateOptions)

    def test_configure_pool_rejects_cloud_service_config(self):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        with pytest.raises(ValueError, match="Cloud Service pools"):
            hook.configure_pool(
                pool_id="mypool",
                vm_size="test_vm_size",
                vm_node_agent_sku_id=self.test_vm_sku,
                target_dedicated_nodes=1,
                os_family="4",
            )

    def test_configure_pool_with_latest_vm(self):
        with mock.patch(
            "airflow.providers.microsoft.azure.hooks."
            "batch.AzureBatchHook._get_latest_verified_image_vm_and_sku"
        ) as mock_getvm:
            hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
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
            assert isinstance(pool, batch_models.BatchPoolCreateOptions)

    @mock.patch(f"{MODULE}.BatchClient")
    def test_create_pool_with_vm_config(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_create_pool = mock_batch.return_value.create_pool
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
        mock_create_pool.assert_called_once_with(pool)

    @mock.patch(f"{MODULE}.time.sleep", return_value=None)
    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_nodes_success_immediately(self, _mock_batch, mock_sleep):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)

        pool = mock.Mock()
        pool.id = "mypool"
        pool.target_dedicated_nodes = 2
        pool.resize_errors = None

        node_idle_1 = mock.Mock(state=batch_models.BatchNodeState.IDLE)
        node_idle_2 = mock.Mock(state=batch_models.BatchNodeState.IDLE)

        hook.connection.get_pool.return_value = pool
        hook.connection.list_nodes.return_value = [node_idle_1, node_idle_2]

        nodes = hook.wait_for_all_node_state(
            pool_id="mypool",
            node_state={batch_models.BatchNodeState.IDLE},
        )

        assert nodes == [node_idle_1, node_idle_2]
        hook.connection.get_pool.assert_called_once_with("mypool")
        hook.connection.list_nodes.assert_called_once_with("mypool")
        assert mock_sleep.call_count == 0

    @mock.patch(f"{MODULE}.time.sleep", return_value=None)
    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_nodes_waits_for_node_count(self, _mock_batch, mock_sleep):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)

        pool = mock.Mock()
        pool.id = "mypool"
        pool.target_dedicated_nodes = 2
        pool.resize_errors = None

        node_idle_1 = mock.Mock(state=batch_models.BatchNodeState.IDLE)
        node_idle_2 = mock.Mock(state=batch_models.BatchNodeState.IDLE)

        hook.connection.get_pool.return_value = pool

        # First call must return only 1 node.
        # Second call must return 2 nodes.
        hook.connection.list_nodes.side_effect = [
            [node_idle_1],
            [node_idle_1, node_idle_2],
        ]

        nodes = hook.wait_for_all_node_state(
            pool_id="mypool",
            node_state={batch_models.BatchNodeState.IDLE},
        )

        assert nodes == [node_idle_1, node_idle_2]
        assert hook.connection.list_nodes.call_count == 2
        assert mock_sleep.call_count == 1

    @mock.patch(f"{MODULE}.time.sleep", return_value=None)
    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_nodes_retries_until_ready(self, _mock_batch, mock_sleep):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)

        pool = mock.Mock()
        pool.id = "mypool"
        pool.target_dedicated_nodes = 2
        pool.resize_errors = None

        node_starting_1 = mock.Mock(state=batch_models.BatchNodeState.STARTING)
        node_starting_2 = mock.Mock(state=batch_models.BatchNodeState.STARTING)

        node_idle_1 = mock.Mock(state=batch_models.BatchNodeState.IDLE)
        node_idle_2 = mock.Mock(state=batch_models.BatchNodeState.IDLE)

        hook.connection.get_pool.return_value = pool

        # Nodes are not ready in the first poll.
        # Nodes are ready in the second poll.
        hook.connection.list_nodes.side_effect = [
            [node_starting_1, node_starting_2],
            [node_idle_1, node_idle_2],
        ]

        nodes = hook.wait_for_all_node_state(
            pool_id="mypool",
            node_state={batch_models.BatchNodeState.IDLE},
        )

        assert nodes == [node_idle_1, node_idle_2]
        assert hook.connection.list_nodes.call_count == 2
        assert mock_sleep.call_count == 1

    @mock.patch(f"{MODULE}.time.sleep", return_value=None)
    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_nodes_resize_error(self, _mock_batch, mock_sleep):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)

        pool = mock.Mock()
        pool.id = "mypool"
        pool.target_dedicated_nodes = 2
        pool.resize_errors = ["resize failed"]

        hook.connection.get_pool.return_value = pool

        with pytest.raises(RuntimeError, match="resize error encountered"):
            hook.wait_for_all_node_state(
                pool_id="mypool",
                node_state={batch_models.BatchNodeState.IDLE},
            )
        assert mock_sleep.call_count == 0

    @mock.patch(f"{MODULE}.BatchClient")
    def test_job_configuration_and_create_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_create_job = mock_batch.return_value.create_job
        job = hook.configure_job(job_id="myjob", pool_id="mypool")
        hook.create_job(job)
        assert isinstance(job, batch_models.BatchJobCreateOptions)
        mock_create_job.assert_called_once_with(job)

    @mock.patch(f"{MODULE}.BatchClient")
    def test_add_single_task_to_job(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        mock_create_task = mock_batch.return_value.create_task
        task = hook.configure_task(task_id="mytask", command_line="echo hello")
        hook.add_single_task_to_job(job_id="myjob", task=task)
        assert isinstance(task, batch_models.BatchTaskCreateOptions)
        mock_create_task.assert_called_once_with("myjob", task)

    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_task_to_complete_timeout(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        with pytest.raises(TimeoutError):
            hook.wait_for_job_tasks_to_complete("myjob", -1)

    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_task_to_complete_all_success(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        task1 = mock.Mock(
            id="mytask_1",
            state=batch_models.BatchTaskState.COMPLETED,
            execution_info=mock.Mock(result=batch_models.BatchTaskExecutionResult.SUCCESS),
        )
        task2 = mock.Mock(
            id="mytask_2",
            state=batch_models.BatchTaskState.COMPLETED,
            execution_info=mock.Mock(result=batch_models.BatchTaskExecutionResult.SUCCESS),
        )
        hook.connection.list_tasks.return_value = iter([task1, task2])

        results = hook.wait_for_job_tasks_to_complete("myjob", 60)
        assert results == []
        hook.connection.list_tasks.assert_called_once_with("myjob")

    @mock.patch(f"{MODULE}.BatchClient")
    def test_wait_for_all_task_to_complete_failures(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        task1 = mock.Mock(
            id="mytask_1",
            state=batch_models.BatchTaskState.COMPLETED,
            execution_info=mock.Mock(result=batch_models.BatchTaskExecutionResult.SUCCESS),
        )
        task2 = mock.Mock(
            id="mytask_2",
            state=batch_models.BatchTaskState.COMPLETED,
            execution_info=mock.Mock(result=batch_models.BatchTaskExecutionResult.FAILURE),
        )
        hook.connection.list_tasks.return_value = iter([task1, task2])

        results = hook.wait_for_job_tasks_to_complete("myjob", 60)
        assert results == [task2]
        hook.connection.list_tasks.assert_called_once_with("myjob")

    @mock.patch(f"{MODULE}.BatchClient")
    def test_connection_success(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        hook.connection.list_jobs.return_value = iter([])
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure Batch."

    @mock.patch(f"{MODULE}.BatchClient")
    def test_connection_failure(self, mock_batch):
        hook = AzureBatchHook(azure_batch_conn_id=self.test_vm_conn_id)
        hook.connection.list_jobs = PropertyMock(side_effect=Exception("Authentication failed."))
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."
