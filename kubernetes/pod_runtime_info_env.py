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
"""
Classes for interacting with Kubernetes API
"""
import copy

from kubernetes.client import models as k8s

from airflow.kubernetes.k8s_model import K8SModel


class PodRuntimeInfoEnv(K8SModel):
    """Defines Pod runtime information as environment variable"""

    def __init__(self, name, field_path):
        """
        Adds Kubernetes pod runtime information as environment variables such as namespace, pod IP, pod name.
        Full list of options can be found in kubernetes documentation.

        :param name: the name of the environment variable
        :type: name: str
        :param field_path: path to pod runtime info. Ex: metadata.namespace | status.podIP
        :type: field_path: str
        """
        self.name = name
        self.field_path = field_path

    def to_k8s_client_obj(self) -> k8s.V1EnvVar:
        """
        :return: kubernetes.client.models.V1EnvVar
        """
        return k8s.V1EnvVar(
            name=self.name,
            value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(
                    self.field_path
                )
            )
        )

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        cp_pod = copy.deepcopy(pod)
        env = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].env = cp_pod.spec.containers[0].env or []
        cp_pod.spec.containers[0].env.append(env)
        return cp_pod
