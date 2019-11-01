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
Functions that are responsible for enriching the pod
"""
import copy
import uuid

import kubernetes.client.models as k8s

from airflow.settings import pod_mutation_hook
from airflow.version import version as airflow_version

_AIRFLOW_VERSION = "v" + airflow_version.replace(".", "-").replace("+", "-")


class XcomSidecarConfig:
    """
    Static constants for the XCOM Sidecar
    """

    MOUNT_PATH = "/airflow/xcom"
    SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"
    CMD = 'trap "exit 0" INT; while true; do sleep 30; done;'
    VOLUME_MOUNT = k8s.V1VolumeMount(name="xcom", mount_path=MOUNT_PATH)
    VOLUME = k8s.V1Volume(name="xcom", empty_dir=k8s.V1EmptyDirVolumeSource())
    CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=["sh", "-c", CMD],
        image="alpine",
        volume_mounts=[VOLUME_MOUNT],
        resources=k8s.V1ResourceRequirements(requests={"cpu": "1m"}),
    )


def _add_sidecar_container(pod: k8s.V1Pod) -> None:
    """Adds sidecar container. It is used to get xcom values."""
    if pod.spec.volumes is None:
        pod.spec.volumes = []
    pod.spec.volumes.insert(0, XcomSidecarConfig.VOLUME)
    if pod.spec.containers[0].volume_mounts is None:
        pod.spec.containers[0].volume_mounts = []
    pod.spec.containers[0].volume_mounts.insert(0, XcomSidecarConfig.VOLUME_MOUNT)
    pod.spec.containers.append(XcomSidecarConfig.CONTAINER)


def _add_labels(pod: k8s.V1Pod) -> None:
    """Add labels with the Airflow version. This is useful when debugging issues."""
    if not pod.metadata:
        pod.metadata = k8s.V1ObjectMeta(labels={})
    if not pod.metadata.labels:
        pod.metadata.labels = {}
    pod.metadata.labels["airflow-version"] = _AIRFLOW_VERSION


def _set_default_namespace(pod: k8s.V1Pod) -> None:
    """Sets the default namespace if it is not set."""
    if not pod.metadata:
        pod.metadata = k8s.V1ObjectMeta(labels={})

    if not pod.metadata.namespace:
        pod.metadata.namespace = "default"


def _add_random_name_prefix(pod: k8s.V1Pod) -> None:
    """
    To enable the same task to be run multiple times, but for a different date, a
    random value must be added to name.
    """
    if not pod.metadata:
        pod.metadata = k8s.V1ObjectMeta(labels={})

    if pod.metadata.name:
        pod.metadata.name = pod.metadata.name + "-" + str(uuid.uuid4())[:8]


def refine_pod(pod: k8s.V1Pod, extract_xcom: bool = False):
    """
    It introduces modifications to Pod to ensure better integration of Airflow with Kubernets.
    """
    result_pod = copy.deepcopy(pod)

    if extract_xcom:
        _add_sidecar_container(result_pod)

    _add_labels(result_pod)
    _set_default_namespace(result_pod)
    _add_random_name_prefix(result_pod)

    pod_mutation_hook(result_pod)

    return result_pod
