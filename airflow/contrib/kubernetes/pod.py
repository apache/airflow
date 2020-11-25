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
"""This module is deprecated. Please use `airflow.kubernetes.pod`."""

import warnings

# pylint: disable=unused-import
from typing import List, Union

from kubernetes.client import models as k8s

from airflow.kubernetes.pod import Port, Resources  # noqa
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.secret import Secret

from kubernetes.client.api_client import ApiClient

api_client = ApiClient()


class Pod(object):
    """
    Represents a kubernetes pod and manages execution of a single pod.

    :param image: The docker image
    :type image: str
    :param envs: A dict containing the environment variables
    :type envs: dict
    :param cmds: The command to be run on the pod
    :type cmds: list[str]
    :param secrets: Secrets to be launched to the pod
    :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
    :param result: The result that will be returned to the operator after
        successful execution of the pod
    :type result: any
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a comma separated list:
        secret_a,secret_b
    :type image_pull_secrets: str
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: dict
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: list
    :param security_context: A dict containing the security context for the pod
    :type security_context: dict
    :param configmaps: A list containing names of configmaps object
        mounting env variables to the pod
    :type configmaps: list[str]
    :param pod_runtime_info_envs: environment variables about
                                  pod runtime information (ip, namespace, nodeName, podName)
    :type pod_runtime_info_envs: list[PodRuntimeEnv]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    """
    def __init__(
            self,
            image,
            envs,
            cmds,
            args=None,
            secrets=None,
            labels=None,
            node_selectors=None,
            name=None,
            ports=None,
            volumes=None,
            volume_mounts=None,
            namespace='default',
            result=None,
            image_pull_policy='IfNotPresent',
            image_pull_secrets=None,
            init_containers=None,
            service_account_name=None,
            resources=None,
            annotations=None,
            affinity=None,
            hostnetwork=False,
            tolerations=None,
            security_context=None,
            configmaps=None,
            pod_runtime_info_envs=None,
            dnspolicy=None
    ):
        warnings.warn(
            "Using `airflow.contrib.kubernetes.pod.Pod` is deprecated. Please use `k8s.V1Pod`.",
            DeprecationWarning, stacklevel=2
        )
        self.image = image
        self.envs = envs or {}
        self.cmds = cmds
        self.args = args or []
        self.secrets = secrets or []
        self.result = result
        self.labels = labels or {}
        self.name = name
        self.ports = ports or []
        self.volumes = volumes or []
        self.volume_mounts = volume_mounts or []
        self.node_selectors = node_selectors or {}
        self.namespace = namespace
        self.image_pull_policy = image_pull_policy
        self.image_pull_secrets = image_pull_secrets or ""
        self.init_containers = init_containers
        self.service_account_name = service_account_name
        self.resources = resources or Resources()
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.hostnetwork = hostnetwork or False
        self.tolerations = tolerations or []
        self.security_context = security_context or {}
        self.configmaps = configmaps or []
        self.pod_runtime_info_envs = pod_runtime_info_envs or []
        self.dnspolicy = dnspolicy

    def to_v1_kubernetes_pod(self):
        """
        Convert to support k8s V1Pod

        :return: k8s.V1Pod
        """
        import kubernetes.client.models as k8s
        meta = k8s.V1ObjectMeta(
            labels=self.labels,
            name=self.name,
            namespace=self.namespace,
            annotations=self.annotations,
        )
        if self.image_pull_secrets:
            image_pull_secrets = [k8s.V1LocalObjectReference(i)
                                  for i in self.image_pull_secrets.split(",")]
        else:
            image_pull_secrets = []
        spec = k8s.V1PodSpec(
            init_containers=self.init_containers,
            containers=[
                k8s.V1Container(
                    image=self.image,
                    command=self.cmds,
                    env_from=[],
                    name="base",
                    env=[k8s.V1EnvVar(name=key, value=val) for key, val in self.envs.items()],
                    args=self.args,
                    image_pull_policy=self.image_pull_policy,
                )
            ],
            image_pull_secrets=image_pull_secrets,
            service_account_name=self.service_account_name,
            node_selector=self.node_selectors,
            dns_policy=self.dnspolicy,
            host_network=self.hostnetwork,
            tolerations=self.tolerations,
            affinity=self.affinity,
            security_context=self.security_context,
        )

        pod = k8s.V1Pod(
            spec=spec,
            metadata=meta,
        )
        for configmap_name in self.configmaps:
            env_var = k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=configmap_name,
                )
            )
            pod.spec.containers[0].env_from.append(env_var)

        for port in _extract_ports(self.ports):
            pod = port.attach_to_pod(pod)
        volumes = _extract_volumes(self.volumes)
        for volume in volumes:
            pod = volume.attach_to_pod(pod)
        for volume_mount in _extract_volume_mounts(self.volume_mounts):
            pod = volume_mount.attach_to_pod(pod)
        for secret in self.secrets:
            pod = secret.attach_to_pod(pod)
        for runtime_info in self.pod_runtime_info_envs:
            pod = runtime_info.attach_to_pod(pod)
        pod = _extract_resources(self.resources).attach_to_pod(pod)
        return pod

    def as_dict(self):
        res = self.__dict__
        res['resources'] = res['resources'].as_dict()
        res['ports'] = [port.as_dict() for port in res['ports']]
        res['volume_mounts'] = [volume_mount.as_dict() for volume_mount in res['volume_mounts']]
        res['volumes'] = [volume.as_dict() for volume in res['volumes']]

        return res


def _extract_env_vars_and_secrets(env_vars):
    """
    Extracts environment variables and Secret objects from V1Pod Environment
    """
    result = {}
    env_vars = env_vars or []  # type: List[Union[k8s.V1EnvVar, dict]]
    secrets = []
    for env_var in env_vars:
        if isinstance(env_var, k8s.V1EnvVar):
            secret = _extract_env_secret(env_var)
            if secret:
                secrets.append(secret)
                continue
            env_var = api_client.sanitize_for_serialization(env_var)
        result[env_var.get("name")] = env_var.get("value")
    return result, secrets


def _extract_env_secret(env_var):
    if env_var.value_from and env_var.value_from.secret_key_ref:
        secret = env_var.value_from.secret_key_ref  # type: k8s.V1SecretKeySelector
        name = secret.name
        key = secret.key
        return Secret("env", deploy_target=env_var.name, secret=name, key=key)
    return None


def _extract_ports(ports):
    result = []
    ports = ports or []  # type: List[Union[k8s.V1ContainerPort, dict]]
    for port in ports:
        if isinstance(port, k8s.V1ContainerPort):
            port = api_client.sanitize_for_serialization(port)
            port = Port(name=port.get("name"), container_port=port.get("containerPort"))
        elif not isinstance(port, Port):
            port = Port(name=port.get("name"), container_port=port.get("containerPort"))
        result.append(port)
    return result


def _extract_resources(resources):
    if isinstance(resources, k8s.V1ResourceRequirements):
        requests = resources.requests or {}
        limits = resources.limits or {}
        return Resources(
            request_memory=requests.get('memory', None),
            request_cpu=requests.get('cpu', None),
            request_ephemeral_storage=requests.get('ephemeral-storage', None),
            limit_memory=limits.get('memory', None),
            limit_cpu=limits.get('cpu', None),
            limit_ephemeral_storage=limits.get('ephemeral-storage', None),
            limit_gpu=limits.get('nvidia.com/gpu')
        )
    elif isinstance(resources, Resources):
        return resources


def _extract_security_context(security_context):
    if isinstance(security_context, k8s.V1PodSecurityContext):
        security_context = api_client.sanitize_for_serialization(security_context)
    return security_context


def _extract_volume_mounts(volume_mounts):
    result = []
    volume_mounts = volume_mounts or []  # type: List[Union[k8s.V1VolumeMount, dict]]
    for volume_mount in volume_mounts:
        if isinstance(volume_mount, k8s.V1VolumeMount):
            volume_mount = api_client.sanitize_for_serialization(volume_mount)
            volume_mount = VolumeMount(
                name=volume_mount.get("name"),
                mount_path=volume_mount.get("mountPath"),
                sub_path=volume_mount.get("subPath"),
                read_only=volume_mount.get("readOnly")
            )
        elif not isinstance(volume_mount, VolumeMount):
            volume_mount = VolumeMount(
                name=volume_mount.get("name"),
                mount_path=volume_mount.get("mountPath"),
                sub_path=volume_mount.get("subPath"),
                read_only=volume_mount.get("readOnly")
            )
        result.append(volume_mount)
    return result


def _extract_volumes(volumes):
    result = []
    volumes = volumes or []  # type: List[Union[k8s.V1Volume, dict]]
    for volume in volumes:
        if isinstance(volume, k8s.V1Volume):
            volume = api_client.sanitize_for_serialization(volume)
            volume = Volume(name=volume.get("name"), configs=volume)
        if not isinstance(volume, Volume):
            volume = Volume(name=volume.get("name"), configs=volume)
        result.append(volume)
    return result
