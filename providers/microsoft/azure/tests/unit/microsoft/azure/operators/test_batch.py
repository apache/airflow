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

from airflow.exceptions import TaskDeferred
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook
from airflow.providers.microsoft.azure.operators.batch import AzureBatchOperator
from airflow.providers.microsoft.azure.triggers.batch import AzureBatchJobTrigger

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
def mocked_batch_service_client():
    with mock.patch("airflow.providers.microsoft.azure.hooks.batch.BatchServiceClient") as m:
        yield m


class TestAzureBatchOperator:
    # set up the test environment
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, mocked_batch_service_client, create_mock_connections):
        # set up mocked Azure Batch client
        self.batch_client = mock.MagicMock(name="FakeBatchServiceClient")
        mocked_batch_service_client.return_value = self.batch_client

        # set up the test variable
        self.test_vm_conn_id = "test_azure_batch_vm2"
        self.test_cloud_conn_id = "test_azure_batch_cloud2"
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
                conn_type="azure_batch",
                extra=json.dumps({"account_url": self.test_account_url}),
            ),
            # connect with cloud service
            Connection(
                conn_id=self.test_cloud_conn_id,
                conn_type="azure_batch",
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
        self.operator2_pass = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            os_family="4",
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            enable_auto_scale=True,
            auto_scale_formula=FORMULA,
            timeout=2,
        )
        self.operator2_no_formula = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            os_family="4",
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            enable_auto_scale=True,
            timeout=2,
        )
        self.operator_fail = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            os_family="4",
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            timeout=2,
        )
        self.operator_mutual_exclusive = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=BATCH_POOL_ID,
            batch_pool_vm_size=BATCH_VM_SIZE,
            batch_job_id=BATCH_JOB_ID,
            batch_task_id=BATCH_TASK_ID,
            vm_publisher=self.test_vm_publisher,
            vm_offer=self.test_vm_offer,
            vm_sku=self.test_vm_sku,
            vm_node_agent_sku_id=self.test_node_agent_sku,
            os_family="5",
            sku_starts_with=self.test_vm_sku,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id,
            target_dedicated_nodes=1,
            timeout=2,
        )
        self.operator_invalid = AzureBatchOperator(
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
        # test pool creation
        self.batch_client.pool.add.assert_called()
        self.batch_client.job.add.assert_called()
        self.batch_client.task.add.assert_called()

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_execute_without_failures_2(self, wait_mock):
        wait_mock.return_value = True  # No wait
        self.operator2_pass.execute(None)
        # test pool creation
        self.batch_client.pool.add.assert_called()
        self.batch_client.job.add.assert_called()
        self.batch_client.task.add.assert_called()

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
        # Remove pool id
        self.operator.should_delete_job = True
        self.operator.execute(None)
        mock_clean.assert_called()
        mock_clean.assert_called_once_with(job_id=BATCH_JOB_ID)

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator_fail.execute(None)
        assert (
            str(ctx.value) == "Either target_dedicated_nodes or enable_auto_scale must be set. None was set"
        )

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_no_formula(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator2_no_formula.execute(None)
        assert str(ctx.value) == "The auto_scale_formula is required when enable_auto_scale is set"

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_mutual_exclusive(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator_mutual_exclusive.execute(None)
        assert (
            str(ctx.value) == "Cloud service configuration and virtual machine configuration "
            "are mutually exclusive. You must specify either of os_family and"
            " vm_publisher"
        )

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_operator_fails_invalid_args(self, wait_mock):
        wait_mock.return_value = True
        with pytest.raises(AirflowException) as ctx:
            self.operator_invalid.execute(None)
        assert str(ctx.value) == "You must specify either vm_publisher or os_family"

    def test_cleaning_works(self):
        self.operator.clean_up(job_id="myjob")
        self.batch_client.job.delete.assert_called_once_with("myjob")
        self.operator.clean_up("mypool")
        self.batch_client.pool.delete.assert_called_once_with("mypool")
        self.operator.clean_up("mypool", "myjob")
        self.batch_client.job.delete.assert_called_with("myjob")
        self.batch_client.pool.delete.assert_called_with("mypool")

    def test_execute_deferrable_defers_with_expected_trigger(self):
        operator = AzureBatchOperator(
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
            timeout=7,
            deferrable=True,
        )

        hook_mock = mock.MagicMock(spec=AzureBatchHook)
        hook_mock.connection = mock.MagicMock()
        hook_mock.connection.config = mock.MagicMock()
        hook_mock.connection.pool.get.return_value = mock.Mock(resize_errors=None)
        hook_mock.connection.compute_node.list.return_value = []
        hook_mock.configure_pool.return_value = mock.sentinel.pool
        hook_mock.configure_job.return_value = mock.sentinel.job
        hook_mock.configure_task.return_value = mock.sentinel.task
        operator.hook = hook_mock

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context={})

        assert deferred.value.method_name == "execute_complete"
        assert isinstance(deferred.value.trigger, AzureBatchJobTrigger)

        class_path, trigger_kwargs = deferred.value.trigger.serialize()
        assert class_path == "airflow.providers.microsoft.azure.triggers.batch.AzureBatchJobTrigger"
        assert trigger_kwargs == {
            "job_id": BATCH_JOB_ID,
            "azure_batch_conn_id": self.test_vm_conn_id,
            "timeout": 7,
            "poll_interval": 15,
        }

    def test_execute_complete_success(self):
        """Test execute_complete with success event returns batch_job_id."""
        operator = AzureBatchOperator(
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
            deferrable=True,
        )

        event = {"status": "success", "fail_task_ids": []}
        result = operator.execute_complete(context={}, event=event)

        assert result == BATCH_JOB_ID

    def test_execute_complete_failure(self):
        """Test execute_complete with failure event raises AirflowException."""
        operator = AzureBatchOperator(
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
            deferrable=True,
        )

        event = {"status": "failure", "fail_task_ids": ["task1", "task2"]}

        with pytest.raises(AirflowException) as ctx:
            operator.execute_complete(context={}, event=event)

        assert "Job failed. Failed tasks: ['task1', 'task2']" in str(ctx.value)

    def test_execute_complete_timeout(self):
        """Test execute_complete with timeout event raises AirflowException."""
        operator = AzureBatchOperator(
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
            deferrable=True,
        )

        event = {"status": "timeout", "message": "Timed out waiting for tasks"}

        with pytest.raises(AirflowException) as ctx:
            operator.execute_complete(context={}, event=event)

        assert "Timed out waiting for tasks" in str(ctx.value)

    def test_execute_complete_error(self):
        """Test execute_complete with error event raises AirflowException."""
        operator = AzureBatchOperator(
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
            deferrable=True,
        )

        event = {"status": "error", "message": "Azure API error"}

        with pytest.raises(AirflowException) as ctx:
            operator.execute_complete(context={}, event=event)

        assert "Azure API error" in str(ctx.value)

    def test_execute_complete_none_event(self):
        """Test execute_complete with None event raises AirflowException."""
        operator = AzureBatchOperator(
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
            deferrable=True,
        )

        with pytest.raises(AirflowException) as ctx:
            operator.execute_complete(context={}, event=None)

        assert "No event received in trigger callback" in str(ctx.value)

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    def test_cleanup_runs_once_on_success(self, wait_mock):
        """Test cleanup runs exactly once after successful execution."""
        wait_mock.return_value = True
        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
        )

        operator.execute(None)

        # Simulate post_execute hook call
        operator.post_execute(context={}, result=None)

        # Verify cleanup was called
        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

        # Call post_execute again to ensure cleanup doesn't run twice
        operator.post_execute(context={}, result=None)
        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    @mock.patch.object(AzureBatchHook, "wait_for_job_tasks_to_complete")
    def test_cleanup_runs_once_on_task_failure(self, wait_tasks_mock, wait_node_mock):
        """Test cleanup runs exactly once after task failure."""
        wait_node_mock.return_value = True
        wait_tasks_mock.return_value = ["failed-task"]

        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
        )

        with pytest.raises(AirflowException):
            operator.execute(None)

        # Cleanup should happen in post_execute
        operator.post_execute(context={}, result=None)

        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

        # Ensure no double cleanup
        operator.post_execute(context={}, result=None)
        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

    @mock.patch.object(AzureBatchHook, "create_pool")
    def test_cleanup_on_pool_creation_failure(self, create_pool_mock):
        """Test cleanup runs when pool creation fails."""
        create_pool_mock.side_effect = Exception("Pool creation failed")

        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
        )

        with pytest.raises(Exception, match="Pool creation failed"):
            operator.execute(None)

        # Cleanup should still run in post_execute
        operator.post_execute(context={}, result=None)

        # Pool deletion should be attempted (even though creation failed)
        assert self.batch_client.pool.delete.call_count == 1

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    @mock.patch.object(AzureBatchHook, "create_job")
    def test_cleanup_on_job_creation_failure(self, create_job_mock, wait_mock):
        """Test cleanup runs when job creation fails."""
        wait_mock.return_value = True
        create_job_mock.side_effect = Exception("Job creation failed")

        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
        )

        with pytest.raises(Exception, match="Job creation failed"):
            operator.execute(None)

        # Cleanup should run in post_execute
        operator.post_execute(context={}, result=None)

        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

    @mock.patch.object(AzureBatchHook, "wait_for_all_node_state")
    @mock.patch.object(AzureBatchHook, "add_single_task_to_job")
    def test_cleanup_on_task_addition_failure(self, add_task_mock, wait_mock):
        """Test cleanup runs when task addition fails."""
        wait_mock.return_value = True
        add_task_mock.side_effect = Exception("Task addition failed")

        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
        )

        with pytest.raises(Exception, match="Task addition failed"):
            operator.execute(None)

        # Cleanup should run in post_execute
        operator.post_execute(context={}, result=None)

        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

    def test_execute_deferrable_with_cleanup(self):
        """Test deferrable mode triggers cleanup after execute_complete."""
        operator = AzureBatchOperator(
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
            should_delete_job=True,
            should_delete_pool=True,
            deferrable=True,
        )

        # execute_complete should NOT run cleanup
        event = {"status": "success", "fail_task_ids": []}
        result = operator.execute_complete(context={}, event=event)
        assert result == BATCH_JOB_ID

        # Verify cleanup hasn't been triggered yet
        assert self.batch_client.job.delete.call_count == 0
        assert self.batch_client.pool.delete.call_count == 0

        # Cleanup should happen in post_execute
        operator.post_execute(context={}, result=result)

        assert self.batch_client.job.delete.call_count == 1
        assert self.batch_client.pool.delete.call_count == 1

    def test_poll_interval_passed_to_trigger(self):
        """Test poll_interval parameter is passed to trigger."""
        operator = AzureBatchOperator(
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
            timeout=10,
            poll_interval=30,
            deferrable=True,
        )

        hook_mock = mock.MagicMock(spec=AzureBatchHook)
        hook_mock.connection = mock.MagicMock()
        hook_mock.connection.config = mock.MagicMock()
        hook_mock.connection.pool.get.return_value = mock.Mock(resize_errors=None)
        hook_mock.connection.compute_node.list.return_value = []
        hook_mock.configure_pool.return_value = mock.sentinel.pool
        hook_mock.configure_job.return_value = mock.sentinel.job
        hook_mock.configure_task.return_value = mock.sentinel.task
        operator.hook = hook_mock

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context={})

        trigger = deferred.value.trigger
        class_path, trigger_kwargs = trigger.serialize()

        assert trigger_kwargs["poll_interval"] == 30
