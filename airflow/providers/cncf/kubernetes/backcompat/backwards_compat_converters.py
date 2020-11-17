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
"""Executes task in a Kubernetes POD"""

from typing import List

from kubernetes.client import models as k8s

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.backcompat.pod import Port, Resources
from airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.providers.cncf.kubernetes.backcompat.volume_mount import VolumeMount


def _convert_kube_model_object(obj, old_class, new_class):
    convert_op = getattr(obj, "to_k8s_client_obj", None)
    if callable(convert_op):
        return obj.to_k8s_client_obj()
    elif isinstance(obj, new_class):
        return obj
    else:
        raise AirflowException(f"Expected {old_class} or {new_class}, got {type(obj)}")


def convert_volume(volume) -> k8s.V1Volume:
    """
    Converts an airflow Volume object into a k8s.V1Volume

    :param volume:
    :return: k8s.V1Volume
    """
    return _convert_kube_model_object(volume, Volume, k8s.V1Volume)


def convert_volume_mount(volume_mount) -> k8s.V1VolumeMount:
    """
    Converts an airflow VolumeMount object into a k8s.V1VolumeMount

    :param volume_mount:
    :return: k8s.V1VolumeMount
    """
    return _convert_kube_model_object(volume_mount, VolumeMount, k8s.V1VolumeMount)


def convert_resources(resources) -> k8s.V1ResourceRequirements:
    """
    Converts an airflow Resources object into a k8s.V1ResourceRequirements

    :param resources:
    :return: k8s.V1ResourceRequirements
    """
    if isinstance(resources, dict):
        resources = Resources(**resources)
    return _convert_kube_model_object(resources, Resources, k8s.V1ResourceRequirements)


def convert_port(port) -> k8s.V1ContainerPort:
    """
    Converts an airflow Port object into a k8s.V1ContainerPort

    :param port:
    :return: k8s.V1ContainerPort
    """
    return _convert_kube_model_object(port, Port, k8s.V1ContainerPort)


def convert_env_vars(env_vars) -> List[k8s.V1EnvVar]:
    """
    Converts a dictionary into a list of env_vars

    :param env_vars:
    :return:
    """
    if isinstance(env_vars, dict):
        res = []
        for k, v in env_vars.items():
            res.append(k8s.V1EnvVar(name=k, value=v))
        return res
    elif isinstance(env_vars, list):
        return env_vars
    else:
        raise AirflowException(f"Expected dict or list, got {type(env_vars)}")


def convert_pod_runtime_info_env(pod_runtime_info_envs) -> k8s.V1EnvVar:
    """
    Converts a PodRuntimeInfoEnv into an k8s.V1EnvVar

    :param pod_runtime_info_envs:
    :return:
    """
    return _convert_kube_model_object(pod_runtime_info_envs, PodRuntimeInfoEnv, k8s.V1EnvVar)


def convert_image_pull_secrets(image_pull_secrets) -> List[k8s.V1LocalObjectReference]:
    """
    Converts a PodRuntimeInfoEnv into an k8s.V1EnvVar

    :param image_pull_secrets:
    :return:
    """
    if isinstance(image_pull_secrets, str):
        secrets = image_pull_secrets.split(",")
        return [k8s.V1LocalObjectReference(name=secret) for secret in secrets]
    else:
        return image_pull_secrets


def convert_configmap(configmaps) -> k8s.V1EnvFromSource:
    """
    Converts a str into an k8s.V1EnvFromSource

    :param configmaps:
    :return:
    """
    return k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmaps))
