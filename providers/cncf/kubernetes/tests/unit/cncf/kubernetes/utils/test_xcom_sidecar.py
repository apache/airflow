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

from typing import Any

import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import (
    PodDefaults,
    add_sidecar_to_spark_operator_pod_spec,
)

EXISTING_VOLUME_NAME = "config"
EXISTING_MOUNT_PATH = "/config"
XCOM_MOUNT = ApiClient().sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)


def _make_spark_spec(
    *,
    volumes: list[dict[str, Any]] | None = None,
    driver_volume_mounts: list[dict[str, Any]] | None = None,
    driver_sidecars: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "type": "Python",
        "driver": {"cores": 1, "memory": "512m"},
        "executor": {"instances": 1, "cores": 1, "memory": "512m"},
    }
    if volumes is not None:
        spec["volumes"] = volumes
    if driver_volume_mounts is not None:
        spec["driver"]["volumeMounts"] = driver_volume_mounts
    if driver_sidecars is not None:
        spec["driver"]["sidecars"] = driver_sidecars
    return spec


def test_add_sidecar_to_spark_operator_pod_spec_prepends_xcom_before_existing_volumes_and_mounts():
    existing_sidecar = {"name": "other-sidecar", "image": "busybox"}
    existing_volume = {"name": EXISTING_VOLUME_NAME, "emptyDir": {}}
    existing_mount = {"name": EXISTING_VOLUME_NAME, "mountPath": EXISTING_MOUNT_PATH}
    spec = _make_spark_spec(
        volumes=[existing_volume],
        driver_volume_mounts=[existing_mount],
        driver_sidecars=[existing_sidecar],
    )

    result = add_sidecar_to_spark_operator_pod_spec(spec)

    assert len(result["volumes"]) == len(spec["volumes"]) + 1
    assert result["volumes"][0] == PodDefaults.VOLUME.to_dict()
    assert result["volumes"][1:] == spec["volumes"]
    assert len(result["driver"]["volumeMounts"]) == len(spec["driver"]["volumeMounts"]) + 1
    assert result["driver"]["volumeMounts"][0] == XCOM_MOUNT
    assert result["driver"]["volumeMounts"][1:] == spec["driver"]["volumeMounts"]
    assert len(result["driver"]["sidecars"]) == len(spec["driver"]["sidecars"]) + 1
    assert result["driver"]["sidecars"][:-1] == spec["driver"]["sidecars"]
    assert result["driver"]["sidecars"][-1]["name"] == PodDefaults.SIDECAR_CONTAINER_NAME


def test_add_sidecar_to_spark_operator_pod_spec_wires_shared_volume_between_driver_and_sidecar():
    existing_volume = {"name": EXISTING_VOLUME_NAME, "emptyDir": {}}
    existing_mount = {"name": EXISTING_VOLUME_NAME, "mountPath": EXISTING_MOUNT_PATH}
    spec = _make_spark_spec(
        volumes=[existing_volume],
        driver_volume_mounts=[existing_mount],
    )

    result = add_sidecar_to_spark_operator_pod_spec(spec)

    xcom_volume_name = result["volumes"][0]["name"]
    assert xcom_volume_name == PodDefaults.VOLUME_MOUNT_NAME
    assert result["driver"]["volumeMounts"][0]["name"] == xcom_volume_name
    assert result["driver"]["sidecars"][-1]["volumeMounts"][0]["name"] == xcom_volume_name
    assert result["driver"]["volumeMounts"][0]["mountPath"] == PodDefaults.XCOM_MOUNT_PATH
    assert result["driver"]["sidecars"][-1]["volumeMounts"][0]["mountPath"] == PodDefaults.XCOM_MOUNT_PATH


@pytest.mark.parametrize(
    ("sidecar_container_image", "expected_image"),
    [
        ("private.repo/alpine:3.16", "private.repo/alpine:3.16"),
        (None, PodDefaults.SIDECAR_CONTAINER.image),
    ],
    ids=["custom", "default_when_none"],
)
def test_add_sidecar_to_spark_operator_pod_spec_uses_sidecar_container_image(
    sidecar_container_image, expected_image
):
    spec = _make_spark_spec()

    result = add_sidecar_to_spark_operator_pod_spec(spec, sidecar_container_image=sidecar_container_image)

    assert result["driver"]["sidecars"][-1]["image"] == expected_image


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
def test_add_sidecar_to_spark_operator_pod_spec_uses_custom_resources(resources):
    spec = _make_spark_spec()

    result = add_sidecar_to_spark_operator_pod_spec(spec, sidecar_container_resources=resources)

    assert result["driver"]["sidecars"][-1]["resources"] == resources
