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
"""Executes task in a Kubernetes POD with Istio enabled"""
from __future__ import annotations

from kubernetes.client import models as k8s

from airflow import AirflowException
from airflow.compat.functools import cached_property
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator, _suppress
from airflow.providers.cncf.kubernetes.utils.istio_pod_manager import IstioPodManager
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase


class IstioKubernetesPodOperator(KubernetesPodOperator):
    """
    Execute a task in a Kubernetes Pod with Istio enabled
    All parameters are the same with KubernetesPodOperator, the only difference is the cleanup part

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesPodOperator`
    """

    @cached_property
    def pod_manager(self) -> IstioPodManager:
        return IstioPodManager(kube_client=self.client)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, 'status') else None
        if pod_phase != PodPhase.SUCCEEDED and not self.pod_manager.container_is_succeeded(pod, 'base'):
            if self.log_events_on_failure:
                with _suppress(Exception):
                    for event in self.pod_manager.read_pod_events(pod).items:
                        self.log.error("Pod Event: %s - %s", event.reason, event.message)
            if not self.is_delete_operator_pod:
                with _suppress(Exception):
                    self.patch_already_checked(pod)
            with _suppress(Exception):
                self.process_pod_deletion(pod)
            raise AirflowException(f'Pod {pod and pod.metadata.name} returned a failure: {remote_pod}')
        else:
            with _suppress(Exception):
                self.process_pod_deletion(pod)
