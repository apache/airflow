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
Classes for interacting with Kubernetes API.

This module is deprecated. Please use :mod:`kubernetes.client.models.V1ResourceRequirements`
and :mod:`kubernetes.client.models.V1ContainerPort`.
"""
from __future__ import annotations

import warnings

from kubernetes.client import models as k8s

warnings.warn(
    (
        "This module is deprecated. Please use `kubernetes.client.models.V1ResourceRequirements`"
        " and `kubernetes.client.models.V1ContainerPort`."
    ),
    DeprecationWarning,
    stacklevel=2,
)


class Resources:
    """backwards compat for Resources."""

    __slots__ = (
        'request_memory',
        'request_cpu',
        'limit_memory',
        'limit_cpu',
        'limit_gpu',
        'request_ephemeral_storage',
        'limit_ephemeral_storage',
    )

    """
    :param request_memory: requested memory
    :param request_cpu: requested CPU number
    :param request_ephemeral_storage: requested ephemeral storage
    :param limit_memory: limit for memory usage
    :param limit_cpu: Limit for CPU used
    :param limit_gpu: Limits for GPU used
    :param limit_ephemeral_storage: Limit for ephemeral storage
    """

    def __init__(
        self,
        request_memory=None,
        request_cpu=None,
        request_ephemeral_storage=None,
        limit_memory=None,
        limit_cpu=None,
        limit_gpu=None,
        limit_ephemeral_storage=None,
    ):
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.request_ephemeral_storage = request_ephemeral_storage
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu
        self.limit_gpu = limit_gpu
        self.limit_ephemeral_storage = limit_ephemeral_storage

    def to_k8s_client_obj(self):
        """
        Converts to k8s object.

        @rtype: object
        """
        limits_raw = {
            'cpu': self.limit_cpu,
            'memory': self.limit_memory,
            'nvidia.com/gpu': self.limit_gpu,
            'ephemeral-storage': self.limit_ephemeral_storage,
        }
        requests_raw = {
            'cpu': self.request_cpu,
            'memory': self.request_memory,
            'ephemeral-storage': self.request_ephemeral_storage,
        }

        limits = {k: v for k, v in limits_raw.items() if v}
        requests = {k: v for k, v in requests_raw.items() if v}
        resource_req = k8s.V1ResourceRequirements(limits=limits, requests=requests)
        return resource_req


class Port:
    """POD port"""

    __slots__ = ('name', 'container_port')

    def __init__(self, name=None, container_port=None):
        """Creates port"""
        self.name = name
        self.container_port = container_port

    def to_k8s_client_obj(self):
        """
        Converts to k8s object.

        :rtype: object
        """
        return k8s.V1ContainerPort(name=self.name, container_port=self.container_port)
