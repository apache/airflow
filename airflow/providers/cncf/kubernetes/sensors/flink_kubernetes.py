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
from typing import TYPE_CHECKING, Optional, Sequence

from kubernetes import client

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FlinkKubernetesSensor(BaseSensorOperator):
    """
    Checks flinkDeployment object in kubernetes cluster:

    .. seealso::
        For more detail about Flink Deployment Object have a look at the reference:
        https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/reference/#flinkdeployment

    :param application_name: flink Application resource name
    :param namespace: the kubernetes namespace where the flinkDeployment reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: kubernetes api group of flinkDeployment
    :param api_version: kubernetes api version of flinkDeployment
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    FAILURE_STATES = ("MISSING", "ERROR")
    SUCCESS_STATES = ("READY",)

    def __init__(
        self,
        *,
        application_name: str,
        attach_log: bool = False,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = 'flink.apache.org',
        api_version: str = 'v1beta1',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version

    def _log_driver(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(driver_pod_name, namespace=namespace):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your flinkDeployment spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

    def poke(self, context: 'Context') -> bool:
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="flinkdeployments",
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["jobManagerDeploymentStatus"]
        except KeyError:
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(f"flink application failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info("flink application ended successfully")
            return True
        else:
            self.log.info("flink application is still in state: %s", application_state)
            return False
