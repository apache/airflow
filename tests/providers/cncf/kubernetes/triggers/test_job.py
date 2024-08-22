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

from airflow.providers.cncf.kubernetes.triggers.job import KubernetesJobTrigger
from airflow.triggers.base import TriggerEvent

TRIGGER_PATH = "airflow.providers.cncf.kubernetes.triggers.job.{}"
TRIGGER_CLASS = TRIGGER_PATH.format("KubernetesJobTrigger")
HOOK_PATH = "airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook"
JOB_NAME = "test-job-name"
POD_NAME = "test-pod-name"
CONTAINER_NAME = "test-container-name"
NAMESPACE = "default"
CONN_ID = "test_kubernetes_conn_id"
POLL_INTERVAL = 2
CLUSTER_CONTEXT = "test-context"
CONFIG_FILE = "/path/to/config/file"
IN_CLUSTER = False
GET_LOGS = True
XCOM_PUSH = False


@pytest.fixture
def trigger():
    return KubernetesJobTrigger(
        job_name=JOB_NAME,
        job_namespace=NAMESPACE,
        pod_name=POD_NAME,
        pod_namespace=NAMESPACE,
        base_container_name=CONTAINER_NAME,
        kubernetes_conn_id=CONN_ID,
        poll_interval=POLL_INTERVAL,
        cluster_context=CLUSTER_CONTEXT,
        config_file=CONFIG_FILE,
        in_cluster=IN_CLUSTER,
        get_logs=GET_LOGS,
        do_xcom_push=XCOM_PUSH,
    )


class TestKubernetesJobTrigger:
    def test_serialize(self, trigger):
        classpath, kwargs_dict = trigger.serialize()

        assert classpath == TRIGGER_CLASS
        assert kwargs_dict == {
            "job_name": JOB_NAME,
            "job_namespace": NAMESPACE,
            "pod_name": POD_NAME,
            "pod_namespace": NAMESPACE,
            "base_container_name": CONTAINER_NAME,
            "kubernetes_conn_id": CONN_ID,
            "poll_interval": POLL_INTERVAL,
            "cluster_context": CLUSTER_CONTEXT,
            "config_file": CONFIG_FILE,
            "in_cluster": IN_CLUSTER,
            "get_logs": GET_LOGS,
            "do_xcom_push": XCOM_PUSH,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_CLASS}.hook")
    async def test_run_success(self, mock_hook, trigger):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = NAMESPACE
        mock_hook.wait_until_job_complete.side_effect = mock.AsyncMock(return_value=mock_job)

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = POD_NAME
        mock_pod.metadata.namespace = NAMESPACE
        mock_hook.get_pod.side_effect = mock.AsyncMock(return_value=mock_pod)

        mock_is_job_failed = mock_hook.is_job_failed
        mock_is_job_failed.return_value = False

        mock_job_dict = mock_job.to_dict.return_value

        event_actual = await trigger.run().asend(None)

        mock_hook.wait_until_job_complete.assert_called_once_with(name=JOB_NAME, namespace=NAMESPACE)
        mock_job.to_dict.assert_called_once()
        mock_is_job_failed.assert_called_once_with(job=mock_job)
        assert event_actual == TriggerEvent(
            {
                "name": JOB_NAME,
                "namespace": NAMESPACE,
                "pod_name": POD_NAME,
                "pod_namespace": NAMESPACE,
                "status": "success",
                "message": "Job completed successfully",
                "job": mock_job_dict,
                "xcom_result": None,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_CLASS}.hook")
    async def test_run_fail(self, mock_hook, trigger):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = NAMESPACE
        mock_hook.wait_until_job_complete.side_effect = mock.AsyncMock(return_value=mock_job)

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = POD_NAME
        mock_pod.metadata.namespace = NAMESPACE
        mock_hook.get_pod.side_effect = mock.AsyncMock(return_value=mock_pod)

        mock_is_job_failed = mock_hook.is_job_failed
        mock_is_job_failed.return_value = "Error"

        mock_job_dict = mock_job.to_dict.return_value

        event_actual = await trigger.run().asend(None)

        mock_hook.wait_until_job_complete.assert_called_once_with(name=JOB_NAME, namespace=NAMESPACE)
        mock_job.to_dict.assert_called_once()
        mock_is_job_failed.assert_called_once_with(job=mock_job)
        assert event_actual == TriggerEvent(
            {
                "name": JOB_NAME,
                "namespace": NAMESPACE,
                "pod_name": POD_NAME,
                "pod_namespace": NAMESPACE,
                "status": "error",
                "message": "Job failed with error: Error",
                "job": mock_job_dict,
                "xcom_result": None,
            }
        )

    @mock.patch(TRIGGER_PATH.format("AsyncKubernetesHook"))
    def test_hook(self, mock_hook, trigger):
        hook_expected = mock_hook.return_value

        hook_actual = trigger.hook

        mock_hook.assert_called_once_with(
            conn_id=CONN_ID,
            in_cluster=IN_CLUSTER,
            config_file=CONFIG_FILE,
            cluster_context=CLUSTER_CONTEXT,
        )
        assert hook_actual == hook_expected
