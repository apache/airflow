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

from airflow.contrib.kubernetes.pod import Pod, Port
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
import uuid


class PodGenerator:
    """Contains Kubernetes Airflow Worker configuration logic"""

    def __init__(self, kube_config=None):
        self.kube_config = kube_config
        self.ports = []
        self.volumes = []
        self.volume_mounts = []
        self.init_containers = []

    def add_init_container(self,
                           name,
                           image,
                           security_context,
                           init_environment,
                           volume_mounts
                           ):
        """

        Adds an init container to the launched pod. useful for pre-

        Args:
            name (str):
            image (str):
            security_context (dict):
            init_environment (dict):
            volume_mounts (dict):

        Returns:

        """
        self.init_containers.append(
            {
                'name': name,
                'image': image,
                'securityContext': security_context,
                'env': init_environment,
                'volumeMounts': volume_mounts
            }
        )

    def _get_init_containers(self):
        return self.init_containers

    def add_port(self, port):  # type: (Port) -> None
        """
        Adds a Port to the generator

        :param port: ports for generated pod
        :type port: airflow.contrib.kubernetes.pod.Port
        """
        self.ports.append({'name': port.name, 'containerPort': port.container_port})

    def add_volume(self, volume):  # type: (Volume) -> None
        """
        Adds a Volume to the generator

        :param volume: volume for generated pod
        :type volume: airflow.contrib.kubernetes.volume.Volume
        """

        self._add_volume(name=volume.name, configs=volume.configs)

    def _add_volume(self, name, configs):
        """

        Args:
            name (str):
            configs (dict): Configurations for the volume.
            Could be used to define PersistentVolumeClaim, ConfigMap, etc...

        Returns:

        """
        volume_map = {'name': name}
        for k, v in configs.items():
            volume_map[k] = v

        self.volumes.append(volume_map)

    def add_volume_with_configmap(self, name, config_map):
        self.volumes.append(
            {
                'name': name,
                'configMap': config_map
            }
        )

    def _add_mount(self,
                   name,
                   mount_path,
                   sub_path,
                   read_only):
        """

        Args:
            name (str):
            mount_path (str):
            sub_path (str):
            read_only:

        Returns:

        """

        self.volume_mounts.append({
            'name': name,
            'mountPath': mount_path,
            'subPath': sub_path,
            'readOnly': read_only
        })

    def add_mount(self,
                  volume_mount,  # type: VolumeMount
                  ):
        """
        Adds a VolumeMount to the generator

        :param volume_mount: volume for generated pod
        :type volume_mount: airflow.contrib.kubernetes.volume_mount.VolumeMount
        """
        self._add_mount(
            name=volume_mount.name,
            mount_path=volume_mount.mount_path,
            sub_path=volume_mount.sub_path,
            read_only=volume_mount.read_only
        )

    def _get_volumes_and_mounts(self):
        return self.volumes, self.volume_mounts

    def _get_image_pull_secrets(self):
        """Extracts any image pull secrets for fetching container(s)"""
        if not self.kube_config.image_pull_secrets:
            return []
        return self.kube_config.image_pull_secrets.split(',')

    def make_pod(self, namespace, image, pod_id, cmds, arguments, labels):
        volumes, volume_mounts = self._get_volumes_and_mounts()
        worker_init_container_spec = self._get_init_containers()

        return Pod(
            namespace=namespace,
            name=pod_id + "-" + str(uuid.uuid4())[:8],
            image=image,
            cmds=cmds,
            args=arguments,
            labels=labels,
            envs={},
            secrets=[],
            # service_account_name=self.kube_config.worker_service_account_name,
            # image_pull_secrets=self.kube_config.image_pull_secrets,
            init_containers=worker_init_container_spec,
            ports=self.ports,
            volumes=volumes,
            volume_mounts=volume_mounts,
            resources=None
        )
