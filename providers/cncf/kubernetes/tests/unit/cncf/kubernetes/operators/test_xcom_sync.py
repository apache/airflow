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

from unittest.mock import patch

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesHook")
def test_xcom_sync_volume_added_when_do_xcom_push_true(mock_hook):
    """Test that shared synchronization volume is added when do_xcom_push is True."""
    # Mock the hook to avoid loading real kubeconfig
    mock_hook_instance = mock_hook.return_value
    mock_hook_instance.get_xcom_sidecar_container_image.return_value = "image"
    mock_hook_instance.get_xcom_sidecar_container_resources.return_value = None
    mock_hook_instance.get_xcom_sidecar_container_security_context.return_value = None

    k = KubernetesPodOperator(
        task_id="test_xcom_sync",
        do_xcom_push=True,
        image="ubuntu:latest",
    )
    pod = k.build_pod_request_obj({})

    # Check if the shared volume is present in the pod spec
    volumes = {v.name: v for v in pod.spec.volumes}
    assert "airflow-xcom-sync" in volumes
    assert isinstance(volumes["airflow-xcom-sync"].empty_dir, k8s.V1EmptyDirVolumeSource)

    # Check if the volume is mounted in the base container
    base_container = pod.spec.containers[0]
    mounts = {m.name: m for m in base_container.volume_mounts}
    assert "airflow-xcom-sync" in mounts
    assert mounts["airflow-xcom-sync"].mount_path == "/tmp/airflow-xcom-sync"

    # Check if the volume is mounted in the xcom sidecar container
    # Sidecar is the last container added
    sidecar = pod.spec.containers[-1]
    sidecar_mounts = {m.name: m for m in sidecar.volume_mounts}
    assert "airflow-xcom-sync" in sidecar_mounts
    assert sidecar_mounts["airflow-xcom-sync"].mount_path == "/tmp/airflow-xcom-sync"


def test_xcom_sync_volume_not_added_when_do_xcom_push_false():
    """Test that shared synchronization volume is NOT added when do_xcom_push is False."""
    k = KubernetesPodOperator(
        task_id="test_no_xcom_sync",
        do_xcom_push=False,
        image="ubuntu:latest",
    )
    pod = k.build_pod_request_obj({})

    # Check that the shared volume is NOT present
    if pod.spec.volumes:
        volume_names = [v.name for v in pod.spec.volumes]
        assert "airflow-xcom-sync" not in volume_names

    # Check that the volume is NOT mounted in the base container
    base_container = pod.spec.containers[0]
    if base_container.volume_mounts:
        mount_names = [m.name for m in base_container.volume_mounts]
        assert "airflow-xcom-sync" not in mount_names
