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
import unittest
from tests.compat import mock
from kubernetes.client import ApiClient
import kubernetes.client.models as k8s
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod import Resources
from airflow.contrib.kubernetes.pod import _extract_resources
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.k8s_model import append_to_pod


class TestPod(unittest.TestCase):
    def test_extract_resources(self):
        res = _extract_resources(k8s.V1ResourceRequirements())
        self.assertEqual(
            res.to_k8s_client_obj().to_dict(), Resources().to_k8s_client_obj().to_dict()
        )
        res = _extract_resources(k8s.V1ResourceRequirements(limits={"memory": "1G"}))
        self.assertEqual(
            res.to_k8s_client_obj().to_dict(),
            Resources(limit_memory="1G").to_k8s_client_obj().to_dict(),
        )
        res = _extract_resources(k8s.V1ResourceRequirements(requests={"memory": "1G"}))
        self.assertEqual(
            res.to_k8s_client_obj().to_dict(),
            Resources(request_memory="1G").to_k8s_client_obj().to_dict(),
        )
        res = _extract_resources(
            k8s.V1ResourceRequirements(
                limits={"memory": "1G"}, requests={"memory": "1G"}
            )
        )
        self.assertEqual(
            res.to_k8s_client_obj().to_dict(),
            Resources(limit_memory="1G", request_memory="1G")
            .to_k8s_client_obj()
            .to_dict(),
        )

    def test_port_to_k8s_client_obj(self):
        port = Port("http", 80)
        self.assertEqual(
            port.to_k8s_client_obj(),
            k8s.V1ContainerPort(name="http", container_port=80),
        )

    @mock.patch("uuid.uuid4")
    def test_port_attach_to_pod(self, mock_uuid):
        import uuid

        static_uuid = uuid.UUID("cf4a56d2-8101-4217-b027-2af6216feb48")
        mock_uuid.return_value = static_uuid
        pod = PodGenerator(image="airflow-worker:latest", name="base").gen_pod()
        ports = [Port("https", 443), Port("http", 80)]
        k8s_client = ApiClient()
        result = append_to_pod(pod, ports)
        result = k8s_client.sanitize_for_serialization(result)
        self.assertEqual(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "base-" + static_uuid.hex},
                "spec": {
                    "containers": [
                        {
                            "args": [],
                            "command": [],
                            "env": [],
                            "envFrom": [],
                            "image": "airflow-worker:latest",
                            "name": "base",
                            "ports": [
                                {"name": "https", "containerPort": 443},
                                {"name": "http", "containerPort": 80},
                            ],
                            "volumeMounts": [],
                        }
                    ],
                    "hostNetwork": False,
                    "imagePullSecrets": [],
                    "volumes": [],
                },
            },
            result,
        )

    @mock.patch("uuid.uuid4")
    def test_to_v1_pod(self, mock_uuid):
        from airflow.contrib.kubernetes.pod import Pod as DeprecatedPod
        from airflow.kubernetes.volume import Volume
        from airflow.kubernetes.volume_mount import VolumeMount
        from airflow.kubernetes.secret import Secret
        from airflow.kubernetes.pod import Resources
        import uuid

        static_uuid = uuid.UUID("cf4a56d2-8101-4217-b027-2af6216feb48")
        mock_uuid.return_value = static_uuid

        pod = DeprecatedPod(
            image="foo",
            name="bar",
            namespace="baz",
            image_pull_policy="Never",
            envs={"test_key": "test_value"},
            cmds=["airflow"],
            resources=Resources(
                request_memory="1G", request_cpu="100Mi", limit_gpu="100G"
            ),
            init_containers=k8s.V1Container(
                name="test-container",
                volume_mounts=k8s.V1VolumeMount(
                    mount_path="/foo/bar", name="init-volume-secret"
                ),
            ),
            volumes=[
                Volume(name="foo", configs={}),
                {"name": "bar", "secret": {"secretName": "volume-secret"}},
            ],
            secrets=[
                Secret("volume", None, "init-volume-secret"),
                Secret("env", "AIRFLOW_SECRET", "secret_name", "airflow_config"),
                Secret("volume", "/opt/airflow", "volume-secret", "secret-key"),
            ],
            volume_mounts=[
                VolumeMount(name="foo", mount_path="/mnt", sub_path="/", read_only=True)
            ],
        )

        k8s_client = ApiClient()

        result = pod.to_v1_kubernetes_pod()
        result = k8s_client.sanitize_for_serialization(result)

        expected = {
            "metadata": {
                "annotations": {},
                "labels": {},
                "name": "bar",
                "namespace": "baz",
            },
            "spec": {
                "affinity": {},
                "containers": [
                    {
                        "args": [],
                        "command": ["airflow"],
                        "env": [
                            {"name": "test_key", "value": "test_value"},
                            {
                                "name": "AIRFLOW_SECRET",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "key": "airflow_config",
                                        "name": "secret_name",
                                    }
                                },
                            },
                        ],
                        "envFrom": [],
                        "image": "foo",
                        "imagePullPolicy": "Never",
                        "name": "base",
                        "resources": {
                            "limits": {"nvidia.com/gpu": "100G"},
                            "requests": {"cpu": "100Mi", "memory": "1G"},
                        },
                        "volumeMounts": [
                            {
                                "mountPath": "/mnt",
                                "name": "foo",
                                "readOnly": True,
                                "subPath": "/",
                            },
                            {
                                "mountPath": "/opt/airflow",
                                "name": "secretvol" + str(static_uuid),
                                "readOnly": True,
                            },
                        ],
                    }
                ],
                "hostNetwork": False,
                "imagePullSecrets": [],
                "initContainers": {
                    "name": "test-container",
                    "volumeMounts": {
                        "mountPath": "/foo/bar",
                        "name": "init-volume-secret",
                    },
                },
                "nodeSelector": {},
                "securityContext": {},
                "tolerations": [],
                "volumes": [
                    {"name": "foo"},
                    {"name": "bar", "secret": {"secretName": "volume-secret"}},
                    {
                        "name": "secretvol" + str(static_uuid),
                        "secret": {"secretName": "init-volume-secret"},
                    },
                    {
                        "name": "secretvol" + str(static_uuid),
                        "secret": {"secretName": "volume-secret"},
                    },
                ],
            },
        }
        self.maxDiff = None
        self.assertEqual(expected, result)
