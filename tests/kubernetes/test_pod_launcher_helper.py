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

from requests.exceptions import BaseHTTPError

from airflow import AirflowException
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.pod import Port,Resources
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.pod_launcher_helper import convert_to_airflow_pod
from airflow.kubernetes_deprecated.pod import Pod
import kubernetes.client.models as k8s


class TestPodLauncherHelper(unittest.TestCase):
    def test_convert_to_airflow_pod(self):
        input_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
              name="foo",
              namespace="bar"
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        command="foo",
                        image="myimage",
                        ports=[
                            k8s.V1ContainerPort(
                                name="myport",
                                container_port=8080,
                            )
                        ],
                        volume_mounts=[k8s.V1VolumeMount(
                            name="mymount",
                            mount_path="/tmp/mount",
                            read_only="True"
                        )]
                    )
                ]
            )
        )
        result_pod = convert_to_airflow_pod(input_pod)

        expected = Pod(
            name="foo",
            namespace="bar",
            envs={},
            cmds=[],
            image="myimage",
            ports=[
                Port(name="myport", container_port=8080)
            ],
            volume_mounts= [VolumeMount(
                name="mymount",
                mount_path="/tmp/mount",
                sub_path=None,
                read_only="True"
            )]
        )
        expected_dict = expected.as_dict()
        result_dict = result_pod.as_dict()
        self.maxDiff = None

        self.assertDictEqual(expected_dict, result_dict)
