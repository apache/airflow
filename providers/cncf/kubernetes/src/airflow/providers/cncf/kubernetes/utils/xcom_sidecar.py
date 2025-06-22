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

from kubernetes.client import models as k8s


class PodDefaults:
    """Static defaults for Pods."""

    XCOM_MOUNT_PATH = "/airflow/xcom"
    SIDECAR_CONTAINER_NAME = "airflow-xcom-sidecar"
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 1; done;'
    VOLUME_MOUNT_NAME= "xcom"
    VOLUME_MOUNT = k8s.V1VolumeMount(name=VOLUME_MOUNT_NAME, mount_path=XCOM_MOUNT_PATH)
    VOLUME = k8s.V1Volume(name=VOLUME_MOUNT_NAME, empty_dir=k8s.V1EmptyDirVolumeSource())
    SIDECAR_CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=["sh", "-c", XCOM_CMD],
        image="alpine",
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
) -> k8s.V1Pod:
    """Add sidecar."""
    pod_cp = copy.deepcopy(pod)
    pod_cp.spec.volumes = pod.spec.volumes or []
    pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
    pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
    pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
    sidecar = copy.deepcopy(PodDefaults.SIDECAR_CONTAINER)
    sidecar.image = sidecar_container_image or PodDefaults.SIDECAR_CONTAINER.image
    if sidecar_container_resources:
        sidecar.resources = sidecar_container_resources
    pod_cp.spec.containers.append(sidecar)

    return pod_cp

def remove_none(d):
    if isinstance(d, dict):
        return {k: remove_none(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [remove_none(v) for v in d if v is not None]
    else:
        return d

def add_sidecar(spec,sidecar_container_image: str | None = None, sidecar_container_resources= None):
    driver_template = copy.deepcopy(spec)
    print("zmira")
    # print(sidecar.to_dict())
    # driver_template["sidecars"] = sidecar.to_dict()
    #TODO: i checked this but check again if adding the dict using the object v1 and so on will work
    driver_template["volumes"]= [
    {
        "name": "xcom",
        "emptyDir": {}
    }
]
    driver_template["driver"]["volumeMounts"]= [
                {
                    "name": PodDefaults.VOLUME_MOUNT_NAME,
                    "mountPath": PodDefaults.XCOM_MOUNT_PATH
                }]
    driver_template["driver"]["sidecars"] = [
        {
            "name": PodDefaults.SIDECAR_CONTAINER_NAME,
            "command": ["sh", "-c", PodDefaults.XCOM_CMD],
            "image": sidecar_container_image or PodDefaults.SIDECAR_CONTAINER.image,
            "volumeMounts": [
                {
                    "name": PodDefaults.VOLUME_MOUNT_NAME,
                    "mountPath": PodDefaults.XCOM_MOUNT_PATH
                }
            ],
            "env": [
                {"name": "MY_ENV_VAR", "value": "value"}
            ]
        }
    ]
    # driver_template["sidecars"] = remove_none(PodDefaultsfaults.SIDECAR_CONTAINER.to_dict())
    print(driver_template)
    return driver_template
