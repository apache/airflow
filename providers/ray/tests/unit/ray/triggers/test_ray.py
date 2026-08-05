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

from unittest.mock import AsyncMock, patch

import pytest
from kubernetes.client.exceptions import ApiException
from ray.job_submission import JobStatus

from airflow.providers.ray.triggers.ray import RayJobTrigger
from airflow.triggers.base import TriggerEvent


class TestRayJobTrigger:
    @pytest.fixture
    def trigger(self):
        return RayJobTrigger(
            job_id="test_job_id",
            conn_id="test_conn",
            xcom_dashboard_url="http://test-dashboard.com",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
            poll_interval=1,
            fetch_logs=True,
        )

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        return_value=JobStatus.FAILED,
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_run_no_job_id(self, mock_hook, mock_job_status):
        trigger = RayJobTrigger(
            job_id="",
            poll_interval=1,
            conn_id="test",
            xcom_dashboard_url="test",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent(
            {"status": JobStatus.FAILED, "message": "Job  completed with status FAILED", "job_id": ""}
        )

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=[JobStatus.RUNNING, JobStatus.SUCCEEDED],
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_succeeded(self, mock_hook, mock_job_status):
        trigger = RayJobTrigger(
            job_id="test_job_id",
            poll_interval=1,
            conn_id="test",
            xcom_dashboard_url="test",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent(
            {
                "status": JobStatus.SUCCEEDED,
                "message": f"Job test_job_id completed with status {JobStatus.SUCCEEDED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=[JobStatus.RUNNING, JobStatus.STOPPED],
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_stopped(self, mock_hook, mock_job_status, trigger):
        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": JobStatus.STOPPED,
                "message": f"Job test_job_id completed with status {JobStatus.STOPPED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=[JobStatus.RUNNING, JobStatus.FAILED],
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_failed(self, mock_hook, mock_job_status, trigger):
        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": JobStatus.FAILED,
                "message": f"Job test_job_id completed with status {JobStatus.FAILED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=[JobStatus.RUNNING, JobStatus.SUCCEEDED],
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger._stream_logs")
    async def test_run_with_log_streaming(self, mock_stream_logs, mock_hook, mock_job_status, trigger):
        mock_stream_logs.return_value = None

        generator = trigger.run()
        event = await generator.asend(None)

        mock_stream_logs.assert_called_once()
        assert event == TriggerEvent(
            {
                "status": JobStatus.SUCCEEDED,
                "message": f"Job test_job_id completed with status {JobStatus.SUCCEEDED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_stream_logs(self, mock_hook, trigger):
        # Create a mock async iterator
        async def mock_async_iterator():
            for item in ["Log line 1\n", "Log line 2\n"]:
                yield item

        # Set up the mock to return an async iterator
        mock_hook.get_ray_tail_logs.return_value = mock_async_iterator()

        with patch("airflow.providers.ray.triggers.ray.RayJobTrigger.log") as mock_log:
            await trigger._stream_logs()

            mock_log.info.assert_any_call("::group::%s logs", "test_job_id")
            mock_log.info.assert_any_call("Log line 1")
            mock_log.info.assert_any_call("Log line 2")
            mock_log.info.assert_any_call("::endgroup::")

    def test_serialize(self, trigger):
        serialized = trigger.serialize()
        assert serialized == (
            "airflow.providers.ray.triggers.ray.RayJobTrigger",
            {
                "job_id": "test_job_id",
                "conn_id": "test_conn",
                "xcom_dashboard_url": "http://test-dashboard.com",
                "ray_cluster_yaml": "test.yaml",
                "gpu_device_plugin_yaml": "nvidia.yaml",
                "fetch_logs": True,
                "poll_interval": 1,
            },
        )

    @pytest.mark.asyncio
    @patch.object(RayJobTrigger, "hook")
    async def test_cleanup_with_cluster_yaml(self, mock_hook, trigger):
        await trigger.cleanup()

        mock_hook.delete_ray_cluster.assert_called_once_with("test.yaml", "nvidia.yaml")

    @pytest.mark.asyncio
    @patch.object(RayJobTrigger, "hook")
    async def test_cleanup_without_cluster_yaml(self, mock_hook):
        trigger = RayJobTrigger(
            job_id="test_job_id",
            conn_id="test_conn",
            xcom_dashboard_url="http://test-dashboard.com",
            ray_cluster_yaml=None,
            gpu_device_plugin_yaml="nvidia.yaml",
            poll_interval=1,
            fetch_logs=True,
        )

        await trigger.cleanup()

        mock_hook.delete_ray_cluster.assert_not_called()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=[JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.SUCCEEDED],
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    async def test_poll_status(self, mock_hook, mock_job_status, mock_sleep, trigger):
        await trigger._poll_status()

        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1)

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.ray.triggers.ray.RayJobTrigger.hook.get_ray_job_status",
        side_effect=ApiException("Failed to get job."),
    )
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.hook")
    @patch("airflow.providers.ray.triggers.ray.RayJobTrigger.cleanup")
    async def test_run_with_exception(self, mock_cleanup, mock_hook, mock_job_status, trigger):
        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": "EXCEPTION",
                "message": "(Failed to get job.)\nReason: None\n",
                "job_id": "test_job_id",
            }
        )
        mock_cleanup.assert_called_once()
