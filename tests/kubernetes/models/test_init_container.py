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
import unittest.mock as mock
from kubernetes.client import ApiClient
import kubernetes.client.models as k8s
from airflow.kubernetes.init_container import InitContainer
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.volume_mount import VolumeMount


class TestPod(unittest.TestCase):

    def test_init_container_to_k8s_client_obj(self):

        volume_mount = VolumeMount('test-volume',
                                   mount_path='/etc/foo',
                                   sub_path=None,
                                   read_only=True)

        init_container = InitContainer(
            name="init-container",
            image="ubuntu:16.04",
            init_environment={"key1": "value1", "key2": "value2"},
            volume_mounts=[volume_mount],
            cmds=["bash", "-cx"],
            args=["echo 10"])

        self.assertEqual(
            init_container.to_k8s_client_obj(),
            k8s.V1Container(
                name='init-container',
                image='ubuntu:16.04',
                env=[k8s.V1EnvVar(name='key1', value='value1'),
                     k8s.V1EnvVar(name='key2', value='value2')],
                volume_mounts=[k8s.V1VolumeMount(
                    name='test-volume',
                    mount_path='/etc/foo',
                    sub_path=None,
                    read_only=True)],
                command=['bash', '-cx'],
                args=['echo 10']
            )
        )

    @mock.patch('uuid.uuid4')
    def test_init_container_attach_to_pod(self, mock_uuid):
        mock_uuid.return_value = '0'
        pod = PodGenerator(image='airflow-worker:latest', name='base').gen_pod()

        volume_mount = VolumeMount('test-volume',
                                   mount_path='/etc/foo',
                                   sub_path=None,
                                   read_only=True)

        init_containers = [InitContainer(
            name="init-container-1",
            image="ubuntu:16.04",
            init_environment={"key1": "value1", "key2": "value2"},
            volume_mounts=[volume_mount],
            cmds=["bash", "-cx"],
            args=["echo 10"]),
            InitContainer(
            name="init-container-2",
            image="ubuntu:16.04",
            init_environment={"key3": "value3"},
            cmds=["bash", "-cx"],
            args=["echo 35"])
        ]
        k8s_client = ApiClient()
        result = append_to_pod(pod, init_containers)
        result = k8s_client.sanitize_for_serialization(result)
        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'base-0'},
            'spec': {
                'containers': [{
                    'args': [],
                    'command': [],
                    'env': [],
                    'envFrom': [],
                    'image': 'airflow-worker:latest',
                    'imagePullPolicy': 'IfNotPresent',
                    'name': 'base',
                    'ports': [],
                    'volumeMounts': [],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'initContainers': [{
                    'name': 'init-container-1',
                    'image': 'ubuntu:16.04',
                    'command': ['bash', '-cx'],
                    'args': ['echo 10'],
                    'env': [{
                        'name': 'key1',
                        'value': 'value1'
                    }, {
                        'name': 'key2',
                        'value': 'value2'
                    }],
                    'volumeMounts': [{
                        'mountPath': '/etc/foo',
                        'name': 'test-volume',
                        'readOnly': True
                    }],
                }, {
                    'name': 'init-container-2',
                    'image': 'ubuntu:16.04',
                    'command': ['bash', '-cx'],
                    'args': ['echo 35'],
                    'env': [{
                        'name': 'key3',
                        'value': 'value3'
                    }]
                }],
                'restartPolicy': 'Never',
                'volumes': []
            }
        }, result)
