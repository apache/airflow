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
from __future__ import annotations

import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import (
    PodDefaults,
    add_xcom_sidecar,
)

EXISTING_VOLUME_NAME = "config"
EXISTING_MOUNT_PATH = "/config"


def _existing_volume() -> k8s.V1Volume:
    return k8s.V1Volume(name=EXISTING_VOLUME_NAME, empty_dir=k8s.V1EmptyDirVolumeSource())


def _existing_mount() -> k8s.V1VolumeMount:
    return k8s.V1VolumeMount(name=EXISTING_VOLUME_NAME, mount_path=EXISTING_MOUNT_PATH)


def _make_pod(
    *, volumes: list[k8s.V1Volume] | None = None, volume_mounts: list[k8s.V1VolumeMount] | None = None
):
    return k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="ubuntu:16.04",
                    volume_mounts=volume_mounts,
                )
            ],
            volumes=volumes,
        )
    )


def test_add_xcom_sidecar_adds_empty_dir_volume_when_none():
    pod = _make_pod()

    result = add_xcom_sidecar(pod)

    assert len(result.spec.volumes) == len(pod.spec.volumes or []) + 1
    assert result.spec.volumes[0].name == PodDefaults.VOLUME.name
    assert result.spec.volumes[0].empty_dir is not None


def test_add_xcom_sidecar_mounts_xcom_on_first_container():
    pod = _make_pod()

    result = add_xcom_sidecar(pod)

    mounts = result.spec.containers[0].volume_mounts
    assert len(mounts) == len(pod.spec.containers[0].volume_mounts or []) + 1
    assert mounts[0].name == PodDefaults.VOLUME_MOUNT.name
    assert mounts[0].mount_path == PodDefaults.XCOM_MOUNT_PATH


def test_add_xcom_sidecar_appends_sidecar_container():
    pod = _make_pod()

    result = add_xcom_sidecar(pod)

    assert len(result.spec.containers) == len(pod.spec.containers) + 1
    assert result.spec.containers[0].name == pod.spec.containers[0].name
    assert result.spec.containers[1].name == PodDefaults.SIDECAR_CONTAINER_NAME


def test_add_xcom_sidecar_uses_custom_image():
    pod = _make_pod()
    custom_image = "private.repo/alpine:3.16"

    result = add_xcom_sidecar(pod, sidecar_container_image=custom_image)

    assert result.spec.containers[1].image == custom_image


def test_add_xcom_sidecar_none_image_uses_default():
    pod = _make_pod()

    result = add_xcom_sidecar(pod, sidecar_container_image=None)

    assert result.spec.containers[1].image == PodDefaults.SIDECAR_CONTAINER.image


@pytest.mark.parametrize(
    "resources",
    [
        {
            "requests": {"cpu": "1m", "memory": "10Mi"},
            "limits": {"cpu": "10m", "memory": "50Mi"},
        },
        k8s.V1ResourceRequirements(
            requests={"cpu": "2m", "memory": "20Mi"},
            limits={"cpu": "20m", "memory": "100Mi"},
        ),
    ],
    ids=["dict", "v1_resource_requirements"],
)
def test_add_xcom_sidecar_uses_custom_resources(resources):
    pod = _make_pod()

    result = add_xcom_sidecar(pod, sidecar_container_resources=resources)

    assert result.spec.containers[1].resources == resources


def test_add_xcom_sidecar_prepends_xcom_before_existing_volumes_and_mounts():
    existing_volume = _existing_volume()
    existing_mount = _existing_mount()
    pod = _make_pod(volumes=[existing_volume], volume_mounts=[existing_mount])

    result = add_xcom_sidecar(pod)

    assert len(result.spec.volumes) == len(pod.spec.volumes) + 1
    assert result.spec.volumes[0] is PodDefaults.VOLUME
    assert result.spec.volumes[1].name == EXISTING_VOLUME_NAME
    assert len(result.spec.containers[0].volume_mounts) == len(pod.spec.containers[0].volume_mounts) + 1
    assert result.spec.containers[0].volume_mounts[0] is PodDefaults.VOLUME_MOUNT
    assert result.spec.containers[0].volume_mounts[1].name == EXISTING_VOLUME_NAME
    assert result.spec.containers[0].volume_mounts[1].mount_path == EXISTING_MOUNT_PATH
    assert len(result.spec.containers[1].volume_mounts) == 1
    assert result.spec.containers[1].volume_mounts[0] == PodDefaults.VOLUME_MOUNT
