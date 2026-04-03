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

import warnings
from typing import Any

from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException


def _convert_kube_model_object(obj, new_class):
    convert_op = getattr(obj, "to_k8s_client_obj", None)
    if callable(convert_op):
        return obj.to_k8s_client_obj()
    if isinstance(obj, new_class):
        return obj
    raise AirflowException(f"Expected {new_class}, got {type(obj)}")


def _convert_from_dict(obj, new_class):
    if isinstance(obj, new_class):
        return obj
    if isinstance(obj, dict):
        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(obj, new_class)
    raise AirflowException(f"Expected dict or {new_class}, got {type(obj)}")


def _env_var_dict_to_v1(item: dict[str, Any], idx: int) -> k8s.V1EnvVar:
    """Build ``V1EnvVar`` from a dict (simple name/value or Kubernetes client dict)."""
    name = item.get("name")
    if isinstance(name, str) and name and "value" in item:
        return k8s.V1EnvVar(name=name, value=item["value"])
    if "name" in item and not isinstance(item["name"], str):
        raise AirflowException(
            f"Invalid env_vars[{idx}]: `name` must be a string, got {type(item['name']).__name__}."
        )
    try:
        obj = _convert_from_dict(item, k8s.V1EnvVar)
    except AirflowException as e:
        raise AirflowException(
            f"Invalid env_vars[{idx}]: expected a dict with non-empty string 'name' and a "
            f"'value' key, or a Kubernetes V1EnvVar-compatible dict. {e}"
        ) from e
    except (TypeError, ValueError) as e:
        raise AirflowException(
            f"Invalid env_vars[{idx}]: could not build V1EnvVar from dict. {e}"
        ) from e
    if obj.value is None and obj.value_from is None:
        raise AirflowException(
            f"Invalid env_vars[{idx}]: env var must set `value`, `valueFrom`/`value_from`, "
            "or use non-empty string `name` with a `value` key for plain literals."
        )
    if not isinstance(obj.name, str) or not obj.name:
        raise AirflowException(
            f"Invalid env_vars[{idx}]: `name` must be a non-empty string."
        )
    return obj


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


def convert_env_vars(
    env_vars: list[k8s.V1EnvVar] | list[dict[str, Any]] | dict[str, str],
) -> list[k8s.V1EnvVar]:
    """
    Coerce env var collection for kubernetes.

    Supported shapes:

    * ``dict[str, str]``: mapping of env name to literal value (documented API style).
    * ``list[k8s.V1EnvVar]``: pass through.
    * ``list[dict]``: each element is either a minimal ``{"name": str, "value": ...}`` mapping
      or a Kubernetes-API-shaped dict deserialized to ``V1EnvVar`` (e.g. with ``valueFrom``).

    The list-of-plain-dicts form was never described as a stable public contract in historical
    Airflow docs; it appears in the wild from templated YAML/JSON and older examples. It is
    therefore deprecated and may be removed in a future major ``cncf.kubernetes`` release—prefer
    ``dict[str, str]`` or ``list[V1EnvVar]``.
    """
    if isinstance(env_vars, dict):
        return [k8s.V1EnvVar(name=k, value=v) for k, v in env_vars.items()]
    if not isinstance(env_vars, list):
        return env_vars  # type: ignore[return-value]
    if not env_vars:
        return []
    if all(isinstance(x, k8s.V1EnvVar) for x in env_vars):
        return env_vars
    if all(isinstance(x, dict) for x in env_vars):
        warnings.warn(
            "Passing env_vars as a list of {'name': ..., 'value': ...} dicts is deprecated; "
            "this shape was not a documented first-class API. Use dict[str, str] "
            "(environment name to value) or a list of k8s.V1EnvVar. "
            "Support may be removed in a future major apache-airflow-providers-cncf-kubernetes release.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return [_env_var_dict_to_v1(d, i) for i, d in enumerate(env_vars)]
    raise ValueError(
        "env_vars list must contain only V1EnvVar instances or only dicts, not a mixture of types."
    )


def convert_env_vars_or_raise_error(
    env_vars: list[k8s.V1EnvVar] | list[dict[str, Any]] | dict[str, str],
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
