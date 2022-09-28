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

import time

from kubernetes.client.models.v1_pod import V1Pod

from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodManager,
    PodPhase,
    container_is_completed,
    container_is_succeeded,
)


class IstioPodManager(PodManager):
    """
    Helper class for checking the status of base container in Kubernetes pods
    for use with the IstioKubernetesPodOperator
    """

    def container_is_completed(self, pod: V1Pod, container_name: str) -> bool:
        """Reads pod and checks if container is completed"""
        remote_pod = self.read_pod(pod)
        return container_is_completed(pod=remote_pod, container_name=container_name)

    def container_is_succeeded(self, pod: V1Pod, container_name: str) -> bool:
        """Reads pod and checks if container is succeeded"""
        remote_pod = self.read_pod(pod)
        return container_is_succeeded(pod=remote_pod, container_name=container_name)

    def await_pod_completion(self, pod: V1Pod) -> V1Pod:
        """
        Monitors a pod and returns the final state
        (neglect sidecar state e.g. istio-proxy, vault-agent)
        :param pod: pod spec that will be monitored
        :return:  Tuple[State, Optional[str]]
        """
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase in PodPhase.terminal_states:
                break
            if remote_pod.status.phase == PodPhase.RUNNING and self.container_is_completed(
                remote_pod, 'base'
            ):
                break
            self.log.info('Pod %s has phase %s', pod.metadata.name, remote_pod.status.phase)
            time.sleep(2)
        return remote_pod
