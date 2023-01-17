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

from datetime import datetime
from typing import Any

from airflow.providers.cncf.kubernetes.triggers.kubernetes_pod import KubernetesPodTrigger
from airflow.providers.google.cloud.hooks.kubernetes_engine import AsyncGKEPodHook


class GKEPodTrigger(KubernetesPodTrigger):
    """
    Trigger for checking pod status until it finishes its job.

    :param pod_name: The name of the pod.
    :param pod_namespace: The namespace of the pod.
    :param cluster_url: The URL pointed to the cluster.
    :param ssl_ca_cert: SSL certificate that is used for authentication to the pod.
    :param cluster_context: Context that points to kubernetes cluster.
    :param poll_interval: Polling period in seconds to check for the status.
    :param trigger_start_time: time in Datetime format when the trigger was started
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param should_delete_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param startup_timeout: timeout in seconds to start up the pod.
    """

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        cluster_url: str,
        ssl_ca_cert: str,
        trigger_start_time: datetime,
        cluster_context: str | None = None,
        poll_interval: float = 2,
        in_cluster: bool | None = None,
        should_delete_pod: bool = True,
        get_logs: bool = True,
        startup_timeout: int = 120,
        *args,
        **kwargs,
    ):
        super().__init__(
            pod_name,
            pod_namespace,
            trigger_start_time,
            *args,
            **kwargs,
        )
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.trigger_start_time = trigger_start_time
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster
        self.should_delete_pod = should_delete_pod
        self.get_logs = get_logs
        self.startup_timeout = startup_timeout

        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEPodTrigger",
            {
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "cluster_url": self._cluster_url,
                "ssl_ca_cert": self._ssl_ca_cert,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "in_cluster": self.in_cluster,
                "should_delete_pod": self.should_delete_pod,
                "get_logs": self.get_logs,
                "startup_timeout": self.startup_timeout,
                "trigger_start_time": self.trigger_start_time,
            },
        )

    def _get_async_hook(self) -> AsyncGKEPodHook:  # type: ignore[override]
        return AsyncGKEPodHook(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
        )
