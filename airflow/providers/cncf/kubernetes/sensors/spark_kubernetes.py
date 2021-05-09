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
from typing import Optional

from airflow.providers.cncf.kubernetes.sensors.base_kubernetes import \
    BaseKubernetesCustomAppSensor


class SparkKubernetesSensor(BaseKubernetesCustomAppSensor):
    """
    Checks sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_name: spark Application resource name
    :type application_name:  str
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :type namespace: str
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :type kubernetes_conn_id: str
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :type attach_log: bool
    :param api_group: kubernetes api group of sparkApplication
    :type api_group: str
    :param api_version: kubernetes api version of sparkApplication
    :type api_version: str
    """
    application_type = "Spark"
    application_plural = "sparkapplications"

    def __init__(
        self,
        *,
        application_name: str,
        attach_log: bool = False,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        **kwargs,
    ) -> None:
        super().__init__(application_name=application_name,
                         api_group=api_group,
                         api_version=api_version,
                         attach_log=attach_log,
                         namespace=namespace,
                         kubernetes_conn_id=kubernetes_conn_id,
                         **kwargs)
