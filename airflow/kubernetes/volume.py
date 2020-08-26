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
from typing import Dict

from kubernetes.client import models as k8s

from airflow.kubernetes.k8s_model import K8SModel


class Volume(K8SModel):
    """
    Adds Kubernetes Volume to pod. allows pod to access features like ConfigMaps
    and Persistent Volumes

    :param name: the name of the volume mount
    :type name: str
    :param configs: dictionary of any features needed for volume.
        We purposely keep this vague since there are multiple volume types with changing
        configs.
    :type configs: dict
    """
    def __init__(self, name, configs):
        self.name = name
        self.configs = configs

    def to_k8s_client_obj(self) -> Dict[str, str]:
        """Converts to k8s object"""
        return {
            'name': self.name,
            **self.configs
        }

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        cp_pod = copy.deepcopy(pod)
        volume = self.to_k8s_client_obj()
        cp_pod.spec.volumes = pod.spec.volumes or []
        cp_pod.spec.volumes.append(volume)
        return cp_pod
