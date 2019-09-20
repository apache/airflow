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


class InitContainer(K8SModel):
    """
    Defines Kubernetes InitContainer
    """

    def __init__(
            self,
            name,
            image,
            init_environment=None,
            volume_mounts=None,
            cmds=None,
            args=None):
        """ Adds Kubernetes InitContainer to pod.
        :param name: the name of the init-container
        :type name: str
        :param image: The docker image for the init-container
        :type image: str
        :param init_environment: A dict containing the environment variables
        :type init_environment: dict
        :param volume_mounts: volumeMounts for launched pod
        :type volume_mounts: list[airflow.kubernetes.models.volume_mount.VolumeMount]
        :param cmds: The command to be run on the init-container
        :type cmds: list[str]
        :param args: The arguments for the command to be run on the init-container
        :type args: list[str]
        """
        self.name = name
        self.image = image
        self.init_environment = init_environment or {}
        self.volume_mounts = volume_mounts or []
        self.cmds = cmds or []
        self.args = args or []

    def to_k8s_client_obj(self) -> k8s.V1Container:
        """
        Converts to k8s object.

        :return Container k8s object

        """

        volume_mounts = []
        for mount in self.volume_mounts:
            volume_mounts.append(mount.to_k8s_client_obj())

        init_environment = []
        if self.init_environment and isinstance(self.init_environment, dict):
            for key, val in self.init_environment.items():
                init_environment.append(k8s.V1EnvVar(
                    name=key,
                    value=val
                ))

        return k8s.V1Container(
            name=self.name,
            image=self.image,
            env=init_environment,
            volume_mounts=volume_mounts,
            command=self.cmds,
            args=self.args
        )

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        """
        Attaches to pod

        :return Copy of the Pod object

        """
        cp_pod = copy.deepcopy(pod)
        init_container = self.to_k8s_client_obj()
        cp_pod.spec.init_containers[0] = pod.spec.init_containers[0] or []
        cp_pod.spec.init_containers[0].append(init_container)
        return cp_pod
