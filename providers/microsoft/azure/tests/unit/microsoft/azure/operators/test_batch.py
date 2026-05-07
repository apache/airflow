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

import json
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook
from airflow.providers.microsoft.azure.operators.batch import AzureBatchOperator

TASK_ID = "MyDag"
BATCH_POOL_ID = "MyPool"
BATCH_JOB_ID = "MyJob"
BATCH_TASK_ID = "MyTask"
BATCH_VM_SIZE = "Standard"
FORMULA = """$curTime = time();
             $workHours = $curTime.hour >= 8 && $curTime.hour < 18;
             $isWeekday = $curTime.weekday >= 1 && $curTime.weekday <= 5;
             $isWorkingWeekdayHour = $workHours && $isWeekday;
             $TargetDedicated = $isWorkingWeekdayHour ? 20:10;"""


@pytest.fixture
def mocked_batch_client():
    with mock.patch("airflow.providers.microsoft.azure.hooks.batch.BatchClient") as m:
        yield m


class TestAzureBatchOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, mocked_batch_client, create_mock_connections):
        self.batch_client = mock.MagicMock(name="FakeBatchClient")
        mocked_batch_client.return_value = self.batch_client

        self.test_vm_conn_id = "test_azure_batch_vm2"
        self.test_account_url = "http://test-endpoint:29000"
        self.test_vm_publisher = "test.vm.publisher"
        self.test_vm_offer = "test.vm.offer"
        self.test_vm_sku = "test-sku"
        self.test_node_agent_sku = "test-node-agent-sku"

        create_mock_connections(
            Connection(
                conn_id=self.test_vm_conn_id,
                conn_type="azure_batch",
                login="test_account_name",
                password="test_account_key",
                extra=json.dumps({"account_url": self.test_account_url}),
            ),
        )

        self.operator = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_publisher=self.test_vm_publisher,
            vm_offer=self.test_vm_offer,
            vm_sku=self.test_vm_sku,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            sku_starts_with=self.test_vm_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            target_dedicated_nodes=1,
            timeout=2,
        )
        self.operator_auto_scale = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_publisher=self.test_vm_publisher,
            vm_offer=self.test_vm_offer,
            vm_sku=self.test_vm_sku,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            sku_starts_with=self.test_vm_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            enable_auto_scale=True,
            auto_scale_formula=FORMULA,
            timeout=2,
        )
        self.operator_auto_scale_no_formula = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_publisher=self.test_vm_publisher,
            vm_offer=self.test_vm_offer,
            vm_sku=self.test_vm_sku,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            sku_starts_with=self.test_vm_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            enable_auto_scale=True,
            timeout=2,
        )
        self.operator_no_dedicated_no_auto = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_publisher=self.test_vm_publisher,
            vm_offer=self.test_vm_offer,
            vm_sku=self.test_vm_sku,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            sku_starts_with=self.test_vm_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            timeout=2,
        )
        self.operator_no_publisher = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            target_dedicated_nodes=1,
            timeout=2,
        )

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_execute_without_failures(self, wait_mock):
        wait_mock.return_value = True  # No wait
        self.operator.execute(None)
        # test pool/job/task creation use the new flat method names
        self.batch_client.create_pool.assert_called()
        self.batch_client.create_job.assert_called()
        self.batch_client.create_task.assert_called()

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_execute_with_auto_scale(self, wait_mock):
        wait_mock.return_value = True  # No wait
        self.operator_auto_scale.execute(None)
        self.batch_client.create_pool.assert_called()
        self.batch_client.create_job.assert_called()
        self.batch_client.create_task.assert_called()

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_execute_with_failures(self, wait_mock):
        wait_mock.return_value = True  # No wait
        # Remove pool id
        self.operator.batch_pool_id = None

        # test that it raises
        with pytest.raises(AirflowException):
            self.operator.execute(None)

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    @mock.patch.object(AzureBatchOperator, "clean_up")
    def test_execute_with_cleaning(self, mock_clean, wait_mock):
        wait_mock.return_value = True  # No wait
        self.operator.should_delete_job = True
        self.operator.execute(None)
        mock_clean.assert_called()
        mock_clean.assert_called_once_with(job_id=BATCH_JOB_ID)

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_no_dedicated_no_auto(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator_no_dedicated_no_auto.execute(None)
        assert (
            str(ctx.value) == "Either target_dedicated_nodes or enable_auto_scale must be set. None was set"
        )

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_no_formula(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator_auto_scale_no_formula.execute(None)
        assert str(ctx.value) == "The auto_scale_formula is required when enable_auto_scale is set"

    def test_operator_construction_rejects_os_family(self):
        with pytest.raises(ValueError, match="Cloud Service pools"):
            AzureBatchOperator(
                task_id=TASK_ID,
                batch_pool_id=BATCH_POOL_ID,
                batch_pool_vm_size=BATCH_VM_SIZE,
                batch_job_id=BATCH_JOB_ID,
                batch_task_id=BATCH_TASK_ID,
                vm_node_agent_sku_id=self.test_node_agent_sku,
                os_family="4",
                batch_task_command_line="echo hello",
                azure_batch_conn_id=self.test_vm_conn_id,
                target_dedicated_nodes=1,
                timeout=2,
            )

    def test_operator_construction_rejects_os_version(self):
        with pytest.raises(ValueError, match="Cloud Service pools"):
            AzureBatchOperator(
                task_id=TASK_ID,
                batch_pool_id=BATCH_POOL_ID,
                batch_pool_vm_size=BATCH_VM_SIZE,
                batch_job_id=BATCH_JOB_ID,
                batch_task_id=BATCH_TASK_ID,
                vm_node_agent_sku_id=self.test_node_agent_sku,
                os_version="2",
                batch_task_command_line="echo hello",
                azure_batch_conn_id=self.test_vm_conn_id,
                target_dedicated_nodes=1,
                timeout=2,
            )

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_no_publisher(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(ValueError, match="vm_publisher is required"):
            self.operator_no_publisher.execute(None)

    def test_cleaning_works(self):
        self.operator.clean_up(job_id="myjob")
        self.batch_client.begin_delete_job.assert_called_once_with("myjob")
        self.batch_client.begin_delete_job.return_value.result.assert_called_once()
        self.operator.clean_up("mypool")
        self.batch_client.begin_delete_pool.assert_called_once_with("mypool")
        self.batch_client.begin_delete_pool.return_value.result.assert_called_once()
