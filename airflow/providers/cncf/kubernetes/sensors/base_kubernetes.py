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
from typing import Dict, Optional

from kubernetes import client

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator


class BaseKubernetesCustomAppSensor(BaseSensorOperator):
    """
    Kubernetes operators are derived from this class and inherit these attributes.

    :param application_name: K8s Operator Application resource name
    :type application_name:  str
    :param namespace: the kubernetes namespace where the Application like Spark/Flink reside in
    :type namespace: str
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :type kubernetes_conn_id: str
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :type attach_log: bool
    :param api_group: kubernetes api group of custom K8s Applications
    :type api_group: str
    :param api_version: kubernetes api version of custom K8s Applications
    :type api_version: str
    """

    application_type = "None"
    application_plural = "custom_applications"  # "sparkapplications"
    template_fields = ("application_name", "namespace")
    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
        self,
        *,
        application_name: str,
        api_group: str,
        api_version: str,
        attach_log: bool = False,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = "kubernetes_default",
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

    @property
    def app_name_log(self):
        return f'{self.application_type} application: {self.application_name} ' \
               f'in namespace: {self.namespace}'

    def _log_driver(self, application_state: str, response: dict) -> None:
        self.log.info(f"{self.app_name_log} logs >")
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
                "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

    def poke(self, context: Dict) -> bool:
        self.log.info(f"Poking: {self.application_name}")
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.application_plural,
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["applicationState"]["state"]
        except KeyError:
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(
                f"{self.app_name_log} failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info(f"{self.app_name_log} ended successfully")
            return True
        else:
            self.log.info(f"{self.app_name_log} is still in state: %s", application_state)
            return False
