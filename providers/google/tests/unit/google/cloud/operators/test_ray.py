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

import pytest
from ray.dashboard.modules.job.common import JobStatus

from airflow.providers.common.compat.sdk import AirflowTaskTimeout
from airflow.providers.google.cloud.operators.ray import (
    RayDeleteJobOperator,
    RayGetJobInfoOperator,
    RayListJobsOperator,
    RayStopJobOperator,
    RaySubmitJobOperator,
)

TASK_ID = "test-task"
GCP_CONN_ID = "test-gcp-conn-id"
IMPERSONATION_CHAIN = "test-impersonation"
CLUSTER_ADDRESS = "ray-head-123.us-central1.ray.googleusercontent.com"
ENTRYPOINT = "python3 heavy.py"
SUBMISSION_ID = "submission-123"
JOB_ID = "job-123"

RAY_OP_PATH = "airflow.providers.google.cloud.operators.ray.{}"


class TestRaySubmitJobOperator:
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_submits_job_and_persists_link(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.submit_job.return_value = JOB_ID

        op = RaySubmitJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            wait_for_job_done=False,
            get_job_logs=False,
            runtime_env={"k": "v"},
            metadata={"m": "v"},
            submission_id=SUBMISSION_ID,
            entrypoint_num_cpus=1,
            entrypoint_num_gpus=0,
            entrypoint_memory=1024,
            entrypoint_resources={"CPU": 1.0},
        )

        ti_mock = mock.MagicMock()
        context = {"ti": ti_mock, "task": mock.MagicMock()}

        op.execute(context=context)

        mock_hook_cls.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.submit_job.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            runtime_env={"k": "v"},
            metadata={"m": "v"},
            submission_id=SUBMISSION_ID,
            entrypoint_num_cpus=1,
            entrypoint_num_gpus=0,
            entrypoint_memory=1024,
            entrypoint_resources={"CPU": 1.0},
        )
        ti_mock.xcom_push.assert_called_once_with(
            key="ray_job",
            value={
                "cluster_address": CLUSTER_ADDRESS,
                "job_id": JOB_ID,
            },
        )
        mock_hook.get_job_status.assert_not_called()
        mock_hook.get_job_logs.assert_not_called()

    def test_execute_raises_if_logs_without_wait(self):
        op = RaySubmitJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            wait_for_job_done=False,
            get_job_logs=True,
        )

        ti_mock = mock.MagicMock()
        context = {"ti": ti_mock, "task": mock.MagicMock()}

        with pytest.raises(ValueError, match="Retrieving Job logs can be possible only after Job completion"):
            op.execute(context=context)

    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_waits_for_job_and_gets_logs(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.submit_job.return_value = JOB_ID
        mock_hook.get_job_status.return_value = JobStatus.SUCCEEDED
        mock_hook.get_job_logs.return_value = "some logs"

        op = RaySubmitJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            wait_for_job_done=True,
            get_job_logs=True,
        )

        ti_mock = mock.MagicMock()
        context = {"ti": ti_mock, "task": mock.MagicMock()}

        op.execute(context=context)
        mock_hook.submit_job.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            runtime_env=None,
            metadata=None,
            submission_id=None,
            entrypoint_num_cpus=None,
            entrypoint_num_gpus=None,
            entrypoint_memory=None,
            entrypoint_resources=None,
        )
        mock_hook.get_job_status.assert_called_with(
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )

        mock_hook.get_job_logs.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )

        ti_mock.xcom_push.assert_called_once_with(
            key="ray_job",
            value={
                "cluster_address": CLUSTER_ADDRESS,
                "job_id": JOB_ID,
            },
        )

    @mock.patch(RAY_OP_PATH.format("time.sleep"))
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_check_job_status_reaches_terminal(self, mock_hook_cls, mock_sleep):
        mock_hook = mock_hook_cls.return_value
        mock_hook.stop_job.return_value = True

        operator = RaySubmitJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            wait_for_job_done=True,
            get_job_logs=True,
        )
        operator.hook.get_job_status = mock.MagicMock(
            side_effect=[JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.SUCCEEDED]
        )
        status = operator._check_job_status("addr", "job", polling_interval=1, timeout=100)

        assert status == JobStatus.SUCCEEDED
        assert mock_sleep.call_count == 2

    @mock.patch(RAY_OP_PATH.format("time.sleep"))
    @mock.patch(RAY_OP_PATH.format("time.monotonic"))
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_check_job_status_timeout(self, mock_hook_cls, mock_monotonic, mock_sleep):
        mock_hook = mock_hook_cls.return_value
        mock_hook.stop_job.return_value = True
        mock_monotonic.side_effect = [0, 10, 20, 1000]
        operator = RaySubmitJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            entrypoint=ENTRYPOINT,
            wait_for_job_done=True,
            get_job_logs=True,
        )
        operator.hook.get_job_status = mock.MagicMock(return_value=JobStatus.RUNNING)

        with pytest.raises(
            AirflowTaskTimeout, match=r"Timeout waiting for Ray Job job to finish. Last status: RUNNING"
        ):
            operator._check_job_status("addr", "job", polling_interval=1, timeout=30)


class TestRayStopJobOperator:
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_stops_job_successfully(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.stop_job.return_value = True

        op = RayStopJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )

        context = {"ti": mock.MagicMock(), "task": mock.MagicMock()}
        result = op.execute(context=context)

        mock_hook_cls.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.stop_job.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )
        assert result is None


class TestRayDeleteJobOperator:
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_deletes_job_successfully(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.delete_job.return_value = True

        op = RayDeleteJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )

        context = {"ti": mock.MagicMock(), "task": mock.MagicMock()}
        result = op.execute(context=context)

        mock_hook_cls.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.delete_job.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )
        assert result is None


class TestRayGetJobInfoOperator:
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_returns_serialized_job_info(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        fake_job_obj = object()
        fake_serialized = {"job_id": JOB_ID, "status": "SUCCEEDED"}

        mock_hook.get_job_info.return_value = fake_job_obj
        mock_hook.serialize_job_obj.return_value = fake_serialized

        op = RayGetJobInfoOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )

        context = {"ti": mock.MagicMock(), "task": mock.MagicMock()}
        result = op.execute(context=context)

        mock_hook_cls.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.get_job_info.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
            job_id=JOB_ID,
        )
        mock_hook.serialize_job_obj.assert_called_once_with(fake_job_obj)
        assert result == fake_serialized


class TestRayListJobsOperator:
    @mock.patch(RAY_OP_PATH.format("RayJobHook"))
    def test_execute_lists_and_serializes_jobs(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        job1 = object()
        job2 = object()
        mock_hook.list_jobs.return_value = [job1, job2]
        mock_hook.serialize_job_obj.side_effect = [
            {"job_id": "job-1"},
            {"job_id": "job-2"},
        ]

        op = RayListJobsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            cluster_address=CLUSTER_ADDRESS,
        )

        context = {"ti": mock.MagicMock(), "task": mock.MagicMock()}
        result = op.execute(context=context)

        mock_hook_cls.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.list_jobs.assert_called_once_with(
            cluster_address=CLUSTER_ADDRESS,
        )
        mock_hook.serialize_job_obj.assert_has_calls([mock.call(job1), mock.call(job2)])
        assert result == [
            {"job_id": "job-1"},
            {"job_id": "job-2"},
        ]
