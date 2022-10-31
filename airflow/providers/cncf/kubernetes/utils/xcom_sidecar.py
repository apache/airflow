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
This module handles all xcom functionality for the KubernetesPodOperator
by attaching a sidecar container that blocks the pod from completing until
Airflow has pulled result data into the worker for xcom serialization.
"""
from __future__ import annotations

import copy

from kubernetes.client import models as k8s


class PodDefaults:
    """Static defaults for Pods"""

    XCOM_MOUNT_PATH = "/airflow/xcom"
    SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 1; done;'
    VOLUME_MOUNT = k8s.V1VolumeMount(name="xcom", mount_path=XCOM_MOUNT_PATH)
    VOLUME = k8s.V1Volume(name="xcom", empty_dir=k8s.V1EmptyDirVolumeSource())
    SIDECAR_CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=["sh", "-c", XCOM_CMD],
        image="alpine",
        volume_mounts=[VOLUME_MOUNT],
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
            }
        ),
    )


def add_xcom_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
    """Adds sidecar"""
    pod_cp = copy.deepcopy(pod)
    pod_cp.spec.volumes = pod.spec.volumes or []
    pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
    pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
    pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
    pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

    return pod_cp
