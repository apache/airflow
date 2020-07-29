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

import kubernetes.client.models as k8s  # noqa

from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.pod import Port
from airflow.kubernetes_deprecated.pod import Pod
from airflow.kubernetes.pod_generator import PodGenerator
import kubernetes.client.models as k8s  # noqa


def convert_to_airflow_pod(pod):
    base_container = pod.spec.containers[0]  # type: k8s.V1Container

    dummy_pod = Pod(image=base_container.image,
                    envs=_extract_env_vars(base_container.env),
                    volumes=_extract_volumes(pod.spec.volumes),
                    volume_mounts=_extract_volume_mounts(base_container.volume_mounts),
                    labels=pod.metadata.labels,
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    image_pull_policy=base_container.image_pull_policy or 'IfNotPresent',
                    cmds=[],
                    ports=_extract_ports(base_container.ports)
                    )
    return dummy_pod

def _extract_env_vars(env_vars):
    """

    @param env_vars:
    @type env_vars: list
    @return: result
    @rtype: dict
    """
    result = {}
    env_vars = env_vars or []
    for e in env_vars:
        env_var = e  # type: k8s.V1EnvVar
        result[env_var.name] = env_var.value
    return result


def _extract_volumes(volumes):
    result = []
    volumes = volumes or []
    for v in volumes:
        volume = v  # type: k8s.V1Volume
        result.append(Volume(name=volume.name, configs=volume.__dict__))
    return result


def _extract_volume_mounts(volume_mounts):
    result = []
    volume_mounts = volume_mounts or []
    for v in volume_mounts:
        volume = v  # type: k8s.V1VolumeMount
        result.append(VolumeMount(name=volume.name,
                                  mount_path=volume.mount_path,
                                  sub_path=volume.sub_path,
                                  read_only=volume.read_only))

    return result


def _extract_ports(ports):
    result = []
    ports = ports or []
    for p in ports:
        port = p  # type: k8s.V1ContainerPort
        result.append(Port(name=port.name, container_port=port.container_port))
    return result
