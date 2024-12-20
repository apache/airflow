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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, cast

from kubernetes import client

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RayKubernetesSensor(BaseSensorOperator):
    """
    Checks rayJob object in kubernetes cluster.

    .. seealso::
        For more detail about Ray Application Object have a look at the reference:
        https://docs.ray.io/en/master/cluster/kubernetes/getting-started/rayjob-quick-start.html

    :param application_name: ray job resource name
    :param namespace: the kubernetes namespace where the ray job reside in
    :param container_name: the kubernetes container name where the ray job reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: kubernetes api group of kuberay
    :param api_version: kubernetes api version of kuberay
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
        self,
        *,
        application_name: str,
        attach_log: bool = False,
        namespace: str | None = None,
        container_name: str = "base",
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = "ray.io",
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.container_name = container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.kubernetes_conn_id)

    def _log_submitter(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        labels = {"job-name": self.ray_obj_spec["metadata"]["name"]}
        pods: client.V1PodList = self.pod_manager._client.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=",".join([f"{k}={v}" for k, v in labels.items()]),
        )
        pod_list = cast(list[client.V1Pod], pods.items)
        if len(pod_list) == 0:
            raise AirflowException("Job has no driver pod")
        if len(pod_list) != 1:
            raise AirflowException("Job has multiple driver pod")
        submitter_pod = pod_list[0]
        submitter_pod_name = submitter_pod.metadata.name

        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(
                submitter_pod_name, namespace=namespace, container=self.container_name
            ):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your submitterPodTemplate spec.\n"
                "underlying exception: %s",
                submitter_pod_name,
                e,
            )

    def poke(self, context: Context) -> bool:
        self.log.info("Poking: %s", self.application_name)

        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="rayjobs",
            name=self.application_name,
            namespace=self.namespace,
        )

        try:
            application_state = response["status"]["jobStatus"]
        except KeyError:
            return False

        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_submitter(application_state, response)

        if application_state in self.FAILURE_STATES:
            message = f"Ray job failed with state: {application_state}"
            raise AirflowException(message)
        elif application_state in self.SUCCESS_STATES:
            self.log.info("Ray job ended successfully")
            return True
        else:
            self.log.info("Ray job is still in state: %s", application_state)
            return False
