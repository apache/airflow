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

from functools import cached_property
from typing import Sequence, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator
from kubernetes import client

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataprocGDCKrmSensor(BaseSensorOperator):
    """
    Checks dataproc sparkApplication object in GDC cluster using KRM APIs.

    :param application_name: spark Application resource name
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param api_group: DPGDC api group
    :param api_version: DPGDC api version
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    FAILURE_STATES = ("Failed", "Unknown")
    SUCCESS_STATES = ("Completed", "Succeeded")

    def __init__(
        self,
        *,
        application_name: str,
        attach_log: bool = False,
        namespace: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = "dataprocgdc.cloud.google.com",
        api_version: str = "v1alpha1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.kubernetes_conn_id)

    def _log_driver(self, application_state: str, application_name: str) -> None:
        if not self.attach_log:
            return

        driver_pod_name = "{}-{}-{}".format("dp-spark", application_name, "driver")

        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(
                driver_pod_name, namespace=self.namespace
            ):
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

    def poke(self, context: Context) -> bool:
        self.log.info("Poking: %s", self.application_name)

        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="sparkapplications",
            name=self.application_name,
            namespace=self.namespace,
        )

        try:
            application_state = response["status"]["state"]
        except KeyError:
            return False

        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, self.application_name)

        if application_state in self.FAILURE_STATES:
            message = f"Spark application failed with state: {application_state}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif application_state in self.SUCCESS_STATES:
            self.log.info("Spark application ended successfully")
            return True
        else:
            self.log.info("Spark application is still in state: %s", application_state)
            return False

# TODO : Add Sensor for DataProcGDC using OnePlatform API
