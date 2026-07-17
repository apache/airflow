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
"""Attach a sidecar container that blocks the pod from completing until Airflow pulls result data."""

from __future__ import annotations

import copy
from typing import Any

from kubernetes.client import ApiClient, models as k8s

# Pinned alpine version for the xcom sidecar default. Pinning (rather than
# using the implicit `:latest`) makes kubelet's default imagePullPolicy
# `IfNotPresent` instead of `Always`, so a node that has the image cached
# does not re-pull on every task — protecting CI and disconnected
# deployments from Docker Hub anonymous-pull rate limits. Tracked by
# scripts/ci/prek/upgrade_important_versions.py.
XCOM_SIDECAR_IMAGE = "alpine:3.24.1"


class PodDefaults:
    """Static defaults for Pods."""

    XCOM_MOUNT_PATH = "/airflow/xcom"
    SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 1; done;'
    VOLUME_MOUNT_NAME = "xcom"
    VOLUME_MOUNT = k8s.V1VolumeMount(name=VOLUME_MOUNT_NAME, mount_path=XCOM_MOUNT_PATH)
    XCOM_SIDECAR_COMMAND = ["sh", "-c", XCOM_CMD]
    VOLUME = k8s.V1Volume(name=VOLUME_MOUNT_NAME, empty_dir=k8s.V1EmptyDirVolumeSource())
    SIDECAR_CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=["sh", "-c", XCOM_CMD],
        image=XCOM_SIDECAR_IMAGE,
        volume_mounts=[VOLUME_MOUNT],
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
                "memory": "10Mi",
            },
        ),
    )


def add_xcom_sidecar(
    pod: k8s.V1Pod,
    *,
    sidecar_container_image: str | None = None,
    sidecar_container_resources: k8s.V1ResourceRequirements | dict[str, Any] | None = None,
) -> k8s.V1Pod:
    """Add sidecar."""
    pod_cp = copy.deepcopy(pod)
    pod_cp.spec.volumes = pod_cp.spec.volumes or []
    pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
    pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
    pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
    sidecar = copy.deepcopy(PodDefaults.SIDECAR_CONTAINER)
    sidecar.image = sidecar_container_image or PodDefaults.SIDECAR_CONTAINER.image
    if sidecar_container_resources:
        sidecar.resources = sidecar_container_resources
    pod_cp.spec.containers.append(sidecar)

    return pod_cp


def add_sidecar_to_spark_operator_pod_spec(
    spec: dict[str, Any],
    sidecar_container_image: str | None = None,
    sidecar_container_resources: k8s.V1ResourceRequirements | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Add xcom sidecar to a SparkApplication driver spec dict."""
    # The Spark Operator expects a custom SparkApplication object, which is different from the standard Kubernetes Pod model
    driver_template = copy.deepcopy(spec)
    xcom_volume_mount = ApiClient().sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
    driver_template["volumes"] = driver_template.get("volumes") or []
    driver_template["volumes"].insert(0, PodDefaults.VOLUME.to_dict())
    driver_template["driver"]["volumeMounts"] = driver_template["driver"].get("volumeMounts") or []
    driver_template["driver"]["volumeMounts"].insert(0, xcom_volume_mount)
    sidecar = {
        "name": PodDefaults.SIDECAR_CONTAINER_NAME,
        "command": PodDefaults.XCOM_SIDECAR_COMMAND,
        "image": sidecar_container_image or PodDefaults.SIDECAR_CONTAINER.image,
        "volumeMounts": [xcom_volume_mount],
    }
    if sidecar_container_resources:
        sidecar["resources"] = sidecar_container_resources
    driver_template["driver"]["sidecars"] = driver_template["driver"].get("sidecars") or []
    driver_template["driver"]["sidecars"].append(sidecar)
    return driver_template
