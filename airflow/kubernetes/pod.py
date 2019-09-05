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
import kubernetes.client.models as k8s
from airflow.kubernetes.k8s_model import K8SModel


class Resources(K8SModel):
    def __init__(
            self,
            request_memory=None,
            request_cpu=None,
            limit_memory=None,
            limit_cpu=None,
            limit_gpu=None):
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu
        self.limit_gpu = limit_gpu

    def is_empty_resource_request(self):
        return not self.has_limits() and not self.has_requests()

    def has_limits(self):
        return self.limit_cpu is not None or self.limit_memory is not None or self.limit_gpu is not None

    def has_requests(self):
        return self.request_cpu is not None or self.request_memory is not None

    def to_k8s_client_obj(self) -> k8s.V1ResourceRequirements:
        return k8s.V1ResourceRequirements(
            limits={'cpu': self.limit_cpu, 'memory': self.limit_memory, 'nvidia.com/gpu': self.limit_gpu},
            requests={'cpu': self.request_cpu, 'memory': self.request_memory}
        )

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        cp_pod = copy.deepcopy(pod)
        resources = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].resources = resources
        return cp_pod


class Port(K8SModel):
    def __init__(
            self,
            name=None,
            container_port=None):
        self.name = name
        self.container_port = container_port

    def to_k8s_client_obj(self) -> k8s.V1ContainerPort:
        return k8s.V1ContainerPort(name=self.name, container_port=self.container_port)

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        cp_pod = copy.deepcopy(pod)
        port = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].ports = cp_pod.spec.containers[0].ports or []
        cp_pod.spec.containers[0].ports.append(port)
        return cp_pod
