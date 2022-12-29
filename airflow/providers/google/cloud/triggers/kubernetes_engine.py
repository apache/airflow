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

from typing import Any

from airflow.providers.cncf.kubernetes.triggers.kubernetes_pod import KubernetesPodTrigger
from airflow.providers.google.cloud.hooks.kubernetes_engine import AsyncGKEPodHook


class GKEPodTrigger(KubernetesPodTrigger):
    """Trigger for checking pod status until it finishes its job."""

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        cluster_url: str,
        ssl_ca_cert: str,
        *args,
        **kwargs,
    ):
        super().__init__(pod_name, pod_namespace, *args, **kwargs)
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
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
            },
        )

    def _get_async_hook(self) -> AsyncGKEPodHook:  # type: ignore[override]
        return AsyncGKEPodHook(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
        )
