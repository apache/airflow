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
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SparkKubernetesSensor(BaseSensorOperator):
    """
    Checks sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_name: spark Application resource name
    :type application_name:  str
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :type namespace: str
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :type kubernetes_conn_id: str
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :type attach_log: bool
    """

    template_fields = ("application_name", "namespace")
    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    @apply_defaults
    def __init__(
        self,
        *,
        application_name: str,
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

    def _log_driver(self, application_state: str) -> None:
        if not self.attach_log:
            return
        driver_pod_name = f"{self.application_name}-driver"
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(driver_pod_name):
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
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            plural="sparkapplications",
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["applicationState"]["state"]
        except KeyError:
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state)
        if application_state in self.FAILURE_STATES:
            raise AirflowException("Spark application failed with state: %s" % application_state)
        elif application_state in self.SUCCESS_STATES:
            self.log.info("Spark application ended successfully")
            return True
        else:
            self.log.info("Spark application is still in state: %s", application_state)
            return False
