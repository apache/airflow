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
import mock
from kubernetes.client import models as k8s

from requests.exceptions import BaseHTTPError

from airflow import AirflowException
from airflow.contrib.kubernetes.pod import Pod
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_launcher import PodLauncher, _convert_to_airflow_pod
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume_mount import VolumeMount


class TestPodLauncher(unittest.TestCase):
    def setUp(self):
        self.mock_kube_client = mock.Mock()
        self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(mock.sentinel.logs, logs)

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.logs,
        ]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        self.assertEqual(mock.sentinel.logs, logs)
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=10,
                ),
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=10,
                ),
            ]
        )

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        self.assertRaises(
            AirflowException, self.pod_launcher.read_pod_logs, mock.sentinel
        )

    def test_read_pod_logs_successfully_with_tail_lines(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel, 100)
        self.assertEqual(mock.sentinel.logs, logs)
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=100,
                ),
            ]
        )

    def test_read_pod_events_successfully_returns_events(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.return_value = mock.sentinel.events
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        self.assertEqual(mock.sentinel.events, events)

    def test_read_pod_events_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.events,
        ]
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        self.assertEqual(mock.sentinel.events, events)
        self.mock_kube_client.list_namespaced_event.assert_has_calls(
            [
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector="involvedObject.name={}".format(
                        mock.sentinel.metadata.name
                    ),
                ),
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector="involvedObject.name={}".format(
                        mock.sentinel.metadata.name
                    ),
                ),
            ]
        )

    def test_read_pod_events_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        self.assertRaises(
            AirflowException, self.pod_launcher.read_pod_events, mock.sentinel
        )

    def test_read_pod_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)

    def test_read_pod_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.pod_info,
        ]
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        self.assertEqual(mock.sentinel.pod_info, pod_info)
        self.mock_kube_client.read_namespaced_pod.assert_has_calls(
            [
                mock.call(
                    mock.sentinel.metadata.name, mock.sentinel.metadata.namespace
                ),
                mock.call(
                    mock.sentinel.metadata.name, mock.sentinel.metadata.namespace
                ),
            ]
        )

    def test_read_pod_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        self.assertRaises(AirflowException, self.pod_launcher.read_pod, mock.sentinel)


class TestPodLauncherHelper(unittest.TestCase):
    def test_convert_to_airflow_pod(self):
        input_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo", namespace="bar", annotations={"foo": "bar"}
            ),
            spec=k8s.V1PodSpec(
                affinity=k8s.V1Affinity(
                    pod_anti_affinity=k8s.V1PodAntiAffinity(
                        required_during_scheduling_ignored_during_execution=[
                            k8s.V1WeightedPodAffinityTerm(
                                weight=1,
                                pod_affinity_term=k8s.V1PodAffinityTerm(
                                    label_selector=k8s.V1LabelSelector(
                                        match_expressions=[
                                            k8s.V1LabelSelectorRequirement(
                                                key="security",
                                                operator="In",
                                                values="S1",
                                            )
                                        ]
                                    ),
                                    topology_key="failure-domain.beta.kubernetes.io/zone",
                                ),
                            )
                        ]
                    )
                ),
                init_containers=[
                    k8s.V1Container(
                        name="init-container",
                        volume_mounts=[
                            k8s.V1VolumeMount(mount_path="/tmp", name="init-secret")
                        ],
                    )
                ],
                containers=[
                    k8s.V1Container(
                        name="base",
                        command=["foo"],
                        image="myimage",
                        env=[
                            k8s.V1EnvVar(
                                name="AIRFLOW_SECRET",
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(
                                        name="ai", key="secret_key"
                                    )
                                ),
                            )
                        ],
                        ports=[
                            k8s.V1ContainerPort(name="myport", container_port=8080,)
                        ],
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="myvolume",
                                mount_path="/tmp/mount",
                                read_only="True",
                            ),
                            k8s.V1VolumeMount(
                                name="airflow-config",
                                mount_path="/config",
                                sub_path="airflow.cfg",
                                read_only=True,
                            ),
                            k8s.V1VolumeMount(
                                name="airflow-secret",
                                mount_path="/opt/mount",
                                read_only=True,
                            ),
                        ],
                    )
                ],
                security_context=k8s.V1PodSecurityContext(run_as_user=0, fs_group=0,),
                image_pull_secrets=[k8s.V1LocalObjectReference("my-secret")],
                volumes=[
                    k8s.V1Volume(name="myvolume"),
                    k8s.V1Volume(
                        name="airflow-config",
                        config_map=k8s.V1ConfigMap(data="airflow-data"),
                    ),
                    k8s.V1Volume(
                        name="airflow-secret",
                        secret=k8s.V1SecretVolumeSource(secret_name="secret-name",),
                    ),
                    k8s.V1Volume(
                        name="init-secret",
                        secret=k8s.V1SecretVolumeSource(secret_name="init-secret",),
                    ),
                ],
            ),
        )
        result_pod = _convert_to_airflow_pod(input_pod)

        self.assertEqual(type(result_pod.affinity), dict)

        expected = Pod(
            name="foo",
            namespace="bar",
            annotations={"foo": "bar"},
            envs={},
            init_containers=[
                {
                    "name": "init-container",
                    "volumeMounts": [{"mountPath": "/tmp", "name": "init-secret"}],
                }
            ],
            cmds=["foo"],
            image="myimage",
            ports=[Port(name="myport", container_port=8080)],
            volume_mounts=[
                VolumeMount(
                    name="myvolume",
                    mount_path="/tmp/mount",
                    sub_path=None,
                    read_only="True",
                ),
                VolumeMount(
                    name="airflow-config",
                    read_only=True,
                    mount_path="/config",
                    sub_path="airflow.cfg",
                ),
                VolumeMount(
                    name="airflow-secret",
                    mount_path="/opt/mount",
                    sub_path=None,
                    read_only=True,
                ),
            ],
            image_pull_secrets="my-secret",
            affinity={
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "podAffinityTerm": {
                                "labelSelector": {
                                    "matchExpressions": [
                                        {
                                            "key": "security",
                                            "operator": "In",
                                            "values": "S1",
                                        }
                                    ]
                                },
                                "topologyKey": "failure-domain.beta.kubernetes.io/zone",
                            },
                            "weight": 1,
                        }
                    ]
                }
            },
            secrets=[Secret("env", "AIRFLOW_SECRET", "ai", "secret_key")],
            security_context={"fsGroup": 0, "runAsUser": 0},
            volumes=[
                Volume(name="myvolume", configs={"name": "myvolume"}),
                Volume(
                    name="airflow-config",
                    configs={
                        "configMap": {"data": "airflow-data"},
                        "name": "airflow-config",
                    },
                ),
                Volume(
                    name="airflow-secret",
                    configs={
                        "name": "airflow-secret",
                        "secret": {"secretName": "secret-name"},
                    },
                ),
                Volume(
                    name="init-secret",
                    configs={
                        "name": "init-secret",
                        "secret": {"secretName": "init-secret"},
                    },
                ),
            ],
        )
        expected_dict = expected.as_dict()
        result_dict = result_pod.as_dict()
        print(result_pod.volume_mounts)
        parsed_configs = self.pull_out_volumes(result_dict)
        result_dict["volumes"] = parsed_configs
        self.assertEqual(result_dict["secrets"], expected_dict["secrets"])
        self.assertDictEqual(expected_dict, result_dict)

    @staticmethod
    def pull_out_volumes(result_dict):
        parsed_configs = []
        for volume in result_dict["volumes"]:
            vol = {"name": volume["name"]}
            confs = {}
            for k, v in volume["configs"].items():
                if v and k[0] != "_":
                    confs[k] = v
            vol["configs"] = confs
            parsed_configs.append(vol)
        return parsed_configs
