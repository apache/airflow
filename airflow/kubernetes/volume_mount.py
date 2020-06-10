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


class VolumeMount(K8SModel):
    """
    Initialize a Kubernetes Volume Mount. Used to mount pod level volumes to
    running container.

    :param name: the name of the volume mount
    :type name: str
    :param mount_path:
    :type mount_path: str
    :param sub_path: subpath within the volume mount
    :type sub_path: Optional[str]
    :param read_only: whether to access pod with read-only mode
    :type read_only: bool
    """
    def __init__(self, name, mount_path, sub_path, read_only):
        self.name = name
        self.mount_path = mount_path
        self.sub_path = sub_path
        self.read_only = read_only

    def to_k8s_client_obj(self) -> k8s.V1VolumeMount:
        """
        Converts to k8s object.

        :return Volume Mount k8s object

        """
        return k8s.V1VolumeMount(
            name=self.name,
            mount_path=self.mount_path,
            sub_path=self.sub_path,
            read_only=self.read_only
        )

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        """
        Attaches to pod

        :return Copy of the Pod object

        """
        cp_pod = copy.deepcopy(pod)
        volume_mount = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].volume_mounts = pod.spec.containers[0].volume_mounts or []
        cp_pod.spec.containers[0].volume_mounts.append(volume_mount)
        return cp_pod
