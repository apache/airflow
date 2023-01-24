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
from typing import Dict, List, Tuple

from kubernetes.client import models as k8s


def convert_configmap(configmap_name) -> k8s.V1EnvFromSource:
    """
    Converts a str into an k8s.V1EnvFromSource

    :param configmap_name: config map name
    :return:
    """
    return k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))


def convert_configmap_to_volume(
    configmap_info: Dict[str, str]
) -> Tuple[List[k8s.V1Volume], List[k8s.V1VolumeMount]]:
    """
    Converts a dictionary of config_map_name and mounth_path into k8s.V1VolumeMount and k8s.V1Volume

    :param configmap_info: a dictionary of {config_map_name: mount_path}
    :return:
    """
    volume_mounts = []
    volumes = []
    for config_name, mount_path in configmap_info.items():
        volume_mounts.append(k8s.V1VolumeMount(mount_path=mount_path, name=config_name))
        volumes.append(
            k8s.V1Volume(
                name=config_name,
                config_map=k8s.V1ConfigMapVolumeSource(name=config_name),
            )
        )

    return volumes, volume_mounts
