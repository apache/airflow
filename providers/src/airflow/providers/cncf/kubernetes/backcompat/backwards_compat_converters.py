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
"""Executes task in a Kubernetes POD."""

from __future__ import annotations

from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowException


def _convert_kube_model_object(obj, new_class):
    convert_op = getattr(obj, "to_k8s_client_obj", None)
    if callable(convert_op):
        return obj.to_k8s_client_obj()
    elif isinstance(obj, new_class):
        return obj
    else:
        raise AirflowException(f"Expected {new_class}, got {type(obj)}")


def _convert_from_dict(obj, new_class):
    if isinstance(obj, new_class):
        return obj
    elif isinstance(obj, dict):
        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(obj, new_class)
    else:
        raise AirflowException(f"Expected dict or {new_class}, got {type(obj)}")


def convert_volume(volume) -> k8s.V1Volume:
    """
    Convert an airflow Volume object into a k8s.V1Volume.

    :param volume:
    """
    return _convert_kube_model_object(volume, k8s.V1Volume)


def convert_volume_mount(volume_mount) -> k8s.V1VolumeMount:
    """
    Convert an airflow VolumeMount object into a k8s.V1VolumeMount.

    :param volume_mount:
    """
    return _convert_kube_model_object(volume_mount, k8s.V1VolumeMount)


def convert_port(port) -> k8s.V1ContainerPort:
    """
    Convert an airflow Port object into a k8s.V1ContainerPort.

    :param port:
    """
    return _convert_kube_model_object(port, k8s.V1ContainerPort)


def convert_env_vars(env_vars: list[k8s.V1EnvVar] | dict[str, str]) -> list[k8s.V1EnvVar]:
    """
    Coerce env var collection for kubernetes.

    If the collection is a str-str dict, convert it into a list of ``V1EnvVar``s.
    """
    if isinstance(env_vars, dict):
        return [k8s.V1EnvVar(name=k, value=v) for k, v in env_vars.items()]
    return env_vars


def convert_env_vars_or_raise_error(
    env_vars: list[k8s.V1EnvVar] | dict[str, str],
) -> list[k8s.V1EnvVar]:
    """
    Separate function to convert env var collection for kubernetes and then raise an error if it is still the wrong type.

    This is used after the template strings have been rendered.
    """
    env_vars = convert_env_vars(env_vars)
    if isinstance(env_vars, list):
        return env_vars
    raise AirflowException(f"Expected dict or list, got {type(env_vars)}")


def convert_pod_runtime_info_env(pod_runtime_info_envs) -> k8s.V1EnvVar:
    """
    Convert a PodRuntimeInfoEnv into an k8s.V1EnvVar.

    :param pod_runtime_info_envs:
    """
    return _convert_kube_model_object(pod_runtime_info_envs, k8s.V1EnvVar)


def convert_image_pull_secrets(image_pull_secrets) -> list[k8s.V1LocalObjectReference]:
    """
    Convert a PodRuntimeInfoEnv into an k8s.V1EnvVar.

    :param image_pull_secrets:
    """
    if isinstance(image_pull_secrets, str):
        secrets = image_pull_secrets.split(",")
        return [k8s.V1LocalObjectReference(name=secret) for secret in secrets]
    else:
        return image_pull_secrets


def convert_configmap(configmaps) -> k8s.V1EnvFromSource:
    """
    Convert a str into an k8s.V1EnvFromSource.

    :param configmaps:
    """
    return k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmaps))


def convert_affinity(affinity) -> k8s.V1Affinity:
    """Convert a dict into an k8s.V1Affinity."""
    return _convert_from_dict(affinity, k8s.V1Affinity)


def convert_toleration(toleration) -> k8s.V1Toleration:
    """Convert a dict into an k8s.V1Toleration."""
    return _convert_from_dict(toleration, k8s.V1Toleration)
