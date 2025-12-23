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
from unittest.mock import patch

import pytest

from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator, EmrEksCreateClusterOperator
from airflow.providers.amazon.aws.triggers.emr import EmrContainerTrigger
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

SUBMIT_JOB_SUCCESS_RETURN = {
    "ResponseMetadata": {"HTTPStatusCode": 200},
    "id": "job123456",
    "virtualClusterId": "vc1234",
}

CREATE_EMR_ON_EKS_CLUSTER_RETURN = {"ResponseMetadata": {"HTTPStatusCode": 200}, "id": "vc1234"}

GENERATED_UUID = "800647a9-adda-4237-94e6-f542c85fa55b"


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.conn") as m:
        yield m


class TestEmrContainerOperator:
    def setup_method(self):
        self.emr_container = EmrContainerOperator(
            task_id="start_job",
            name="test_emr_job",
            virtual_cluster_id="vzw123456",
            execution_role_arn="arn:aws:somerole",
            release_label="6.3.0-latest",
            job_driver={},
            configuration_overrides={},
            poll_interval=0,
            client_request_token=GENERATED_UUID,
            tags={},
        )

    @mock.patch.object(EmrContainerHook, "submit_job")
    @mock.patch.object(EmrContainerHook, "check_query_status")
    def test_execute_without_failure(
        self,
        mock_check_query_status,
        mock_submit_job,
    ):
        mock_submit_job.return_value = "jobid_123456"
        mock_check_query_status.return_value = "COMPLETED"

        self.emr_container.execute(None)

        mock_submit_job.assert_called_once_with(
            "test_emr_job", "arn:aws:somerole", "6.3.0-latest", {}, {}, GENERATED_UUID, {}, None
        )
        mock_check_query_status.assert_called_once_with("jobid_123456")
        assert self.emr_container.release_label == "6.3.0-latest"

    @mock.patch.object(
        EmrContainerHook,
        "check_query_status",
        side_effect=["PENDING", "PENDING", "SUBMITTED", "RUNNING", "COMPLETED"],
    )
    def test_execute_with_polling(self, mock_check_query_status, mocked_hook_client):
        # Mock out the emr_client creator
        mocked_hook_client.start_job_run.return_value = SUBMIT_JOB_SUCCESS_RETURN
        assert self.emr_container.execute(None) == "job123456"
        assert mock_check_query_status.call_count == 5

    @mock.patch.object(EmrContainerHook, "submit_job")
    @mock.patch.object(EmrContainerHook, "check_query_status")
    @mock.patch.object(EmrContainerHook, "get_job_failure_reason")
    def test_execute_with_failure(
        self, mock_get_job_failure_reason, mock_check_query_status, mock_submit_job
    ):
        mock_submit_job.return_value = "jobid_123456"
        mock_check_query_status.return_value = "FAILED"
        mock_get_job_failure_reason.return_value = (
            "CLUSTER_UNAVAILABLE - Cluster EKS eks123456 does not exist."
        )
        with pytest.raises(AirflowException) as ctx:
            self.emr_container.execute(None)
        assert "EMR Containers job failed" in str(ctx.value)
        assert "Error: CLUSTER_UNAVAILABLE - Cluster EKS eks123456 does not exist." in str(ctx.value)

    @mock.patch.object(
        EmrContainerHook,
        "check_query_status",
        side_effect=["PENDING", "PENDING", "SUBMITTED", "RUNNING", "COMPLETED"],
    )
    def test_execute_with_polling_timeout(self, mock_check_query_status, mocked_hook_client):
        # Mock out the emr_client creator
        mocked_hook_client.start_job_run.return_value = SUBMIT_JOB_SUCCESS_RETURN

        timeout_container = EmrContainerOperator(
            task_id="start_job",
            name="test_emr_job",
            virtual_cluster_id="vzw123456",
            execution_role_arn="arn:aws:somerole",
            release_label="6.3.0-latest",
            job_driver={},
            configuration_overrides={},
            poll_interval=0,
            max_polling_attempts=3,
        )

        error_match = "Final state of EMR Containers job is SUBMITTED.*Max tries of poll status exceeded"
        with pytest.raises(AirflowException, match=error_match):
            timeout_container.execute(None)

        assert mock_check_query_status.call_count == 3

    @mock.patch.object(EmrContainerHook, "submit_job")
    @mock.patch.object(
        EmrContainerHook, "check_query_status", return_value=EmrContainerHook.INTERMEDIATE_STATES[0]
    )
    def test_operator_defer(self, mock_submit_job, mock_check_query_status):
        """Test the execute method raise TaskDeferred if running operator in deferrable mode"""
        self.emr_container.deferrable = True
        self.emr_container.wait_for_completion = False
        with pytest.raises(TaskDeferred) as exc:
            self.emr_container.execute(context=None)
        assert isinstance(exc.value.trigger, EmrContainerTrigger), (
            f"{exc.value.trigger} is not a EmrContainerTrigger"
        )

    @mock.patch.object(EmrContainerHook, "submit_job")
    @mock.patch.object(
        EmrContainerHook, "check_query_status", return_value=EmrContainerHook.INTERMEDIATE_STATES[0]
    )
    def test_operator_defer_with_timeout(self, mock_submit_job, mock_check_query_status):
        self.emr_container.deferrable = True
        self.emr_container.max_polling_attempts = 1000

        with pytest.raises(TaskDeferred) as e:
            self.emr_container.execute(context=None)

        trigger = e.value.trigger
        assert isinstance(trigger, EmrContainerTrigger), f"{trigger} is not a EmrContainerTrigger"
        assert trigger.waiter_delay == self.emr_container.poll_interval
        assert trigger.attempts == self.emr_container.max_polling_attempts


class TestEmrEksCreateClusterOperator:
    def setup_method(self):
        self.emr_container = EmrEksCreateClusterOperator(
            task_id="start_cluster",
            virtual_cluster_name="test_virtual_cluster",
            eks_cluster_name="test_eks_cluster",
            eks_namespace="test_eks_namespace",
            tags={},
        )

    @mock.patch.object(EmrContainerHook, "create_emr_on_eks_cluster")
    def test_emr_on_eks_execute_without_failure(self, mock_create_emr_on_eks_cluster):
        mock_create_emr_on_eks_cluster.return_value = "vc1234"

        self.emr_container.execute(None)

        mock_create_emr_on_eks_cluster.assert_called_once_with(
            "test_virtual_cluster", "test_eks_cluster", "test_eks_namespace", {}
        )
        assert self.emr_container.virtual_cluster_name == "test_virtual_cluster"

    @mock.patch.object(EmrContainerHook, "create_emr_on_eks_cluster")
    def test_emr_on_eks_execute_with_failure(self, mock_create_emr_on_eks_cluster):
        expected_exception_msg = (
            "An error occurred (ValidationException) when calling the "
            "CreateVirtualCluster "
            "operation:"
            "A virtual cluster already exists in the given namespace"
        )
        mock_create_emr_on_eks_cluster.side_effect = AirflowException(expected_exception_msg)
        with pytest.raises(AirflowException) as ctx:
            self.emr_container.execute(None)
        assert expected_exception_msg in str(ctx.value)

    def test_template_fields(self):
        validate_template_fields(self.emr_container)
