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


class Resources(K8SModel):
    __slots__ = ('request_memory',
                 'request_cpu',
                 'limit_memory',
                 'limit_cpu',
                 'limit_gpu',
                 'request_ephemeral_storage',
                 'limit_ephemeral_storage')

    """
    :param request_memory: requested memory
    :type request_memory: str
    :param request_cpu: requested CPU number
    :type request_cpu: float | str
    :param request_ephemeral_storage: requested ephemeral storage
    :type request_ephemeral_storage: str
    :param limit_memory: limit for memory usage
    :type limit_memory: str
    :param limit_cpu: Limit for CPU used
    :type limit_cpu: float | str
    :param limit_gpu: Limits for GPU used
    :type limit_gpu: int
    :param limit_ephemeral_storage: Limit for ephemeral storage
    :type limit_ephemeral_storage: float | str
    """

    def __init__(
        self,
        request_memory=None,
        request_cpu=None,
        request_ephemeral_storage=None,
        limit_memory=None,
        limit_cpu=None,
        limit_gpu=None,
        limit_ephemeral_storage=None
    ):
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.request_ephemeral_storage = request_ephemeral_storage
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu
        self.limit_gpu = limit_gpu
        self.limit_ephemeral_storage = limit_ephemeral_storage

    def is_empty_resource_request(self):
        """Whether resource is empty"""
        return not self.has_limits() and not self.has_requests()

    def has_limits(self):
        """Whether resource has limits"""
        return self.limit_cpu is not None or \
            self.limit_memory is not None or \
            self.limit_gpu is not None or \
            self.limit_ephemeral_storage is not None

    def has_requests(self):
        """Whether resource has requests"""
        return self.request_cpu is not None or \
            self.request_memory is not None or \
            self.request_ephemeral_storage is not None

    def to_k8s_client_obj(self):
        limits_raw = {
            'cpu': self.limit_cpu,
            'memory': self.limit_memory,
            'nvidia.com/gpu': self.limit_gpu,
            'ephemeral-storage': self.limit_ephemeral_storage
        }
        requests_raw = {
            'cpu': self.request_cpu,
            'memory': self.request_memory,
            'ephemeral-storage': self.request_ephemeral_storage
        }

        limits = {k: v for k, v in limits_raw.items() if v}
        requests = {k: v for k, v in requests_raw.items() if v}
        resource_req = k8s.V1ResourceRequirements(
            limits=limits,
            requests=requests
        )
        return resource_req

    def attach_to_pod(self, pod):
        cp_pod = copy.deepcopy(pod)
        resources = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].resources = resources
        return cp_pod


class Port(K8SModel):
    """POD port"""
    __slots__ = ('name', 'container_port')

    def __init__(
        self,
        name=None,
        container_port=None
    ):
        """Creates port"""
        self.name = name
        self.container_port = container_port

    def to_k8s_client_obj(self):
        return k8s.V1ContainerPort(name=self.name, container_port=self.container_port)

    def attach_to_pod(self, pod):
        cp_pod = copy.deepcopy(pod)
        port = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].ports = cp_pod.spec.containers[0].ports or []
        cp_pod.spec.containers[0].ports.append(port)
        return cp_pod
