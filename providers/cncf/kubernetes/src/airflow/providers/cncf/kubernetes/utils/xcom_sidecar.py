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
from typing import cast

from kubernetes.client import models as k8s

# Pinned alpine version for the xcom sidecar default. Pinning (rather than
# using the implicit `:latest`) makes kubelet's default imagePullPolicy
# `IfNotPresent` instead of `Always`, so a node that has the image cached
# does not re-pull on every task — protecting CI and disconnected
# deployments from Docker Hub anonymous-pull rate limits. Tracked by
# scripts/ci/prek/upgrade_important_versions.py.
XCOM_SIDECAR_IMAGE = "alpine:3.24.4"


class PodDefaults:
    """Static defaults for Pods."""

    XCOM_MOUNT_PATH = "/airflow/xcom"
    SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 1; done;'
    VOLUME_MOUNT = k8s.V1VolumeMount(name="xcom", mount_path=XCOM_MOUNT_PATH)
    VOLUME = k8s.V1Volume(name="xcom", empty_dir=k8s.V1EmptyDirVolumeSource())
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
    sidecar_container_resources: k8s.V1ResourceRequirements | dict | None = None,
    sidecar_container_security_context: k8s.V1SecurityContext | dict | None = None,
) -> k8s.V1Pod:
    """Add sidecar."""
    pod_cp = copy.deepcopy(pod)
    pod_spec = cast("k8s.V1PodSpec", pod_cp.spec)
    pod_spec.volumes = pod_spec.volumes or []
    pod_spec.volumes.insert(0, copy.deepcopy(PodDefaults.VOLUME))
    pod_spec.containers[0].volume_mounts = pod_spec.containers[0].volume_mounts or []
    pod_spec.containers[0].volume_mounts.insert(0, copy.deepcopy(PodDefaults.VOLUME_MOUNT))
    sidecar = copy.deepcopy(PodDefaults.SIDECAR_CONTAINER)
    sidecar.image = sidecar_container_image or PodDefaults.SIDECAR_CONTAINER.image
    if sidecar_container_resources:
        sidecar.resources = k8s.V1ResourceRequirements.model_validate(sidecar_container_resources)
    if sidecar_container_security_context is not None:
        sidecar.security_context = k8s.V1SecurityContext.model_validate(sidecar_container_security_context)
    pod_spec.containers.append(sidecar)

    return pod_cp
