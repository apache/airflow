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

import kubernetes.client.models as k8s
from kubernetes.client import ApiClient

from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_generator import PodGenerator


class TestPod(unittest.TestCase):

    def test_port_to_k8s_client_obj(self):
        port = Port('http', 80)
        self.assertEqual(
            port.to_k8s_client_obj(),
            k8s.V1ContainerPort(
                name='http',
                container_port=80
            )
        )

    @mock.patch('uuid.uuid4')
    def test_port_attach_to_pod(self, mock_uuid):
        mock_uuid.return_value = '0'
        pod = PodGenerator(image='airflow-worker:latest', name='base').gen_pod()
        ports = [
            Port('https', 443),
            Port('http', 80)
        ]
        k8s_client = ApiClient()
        result = append_to_pod(pod, ports)
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
                    'ports': [{
                        'name': 'https',
                        'containerPort': 443
                    }, {
                        'name': 'http',
                        'containerPort': 80
                    }],
                    'volumeMounts': [],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'restartPolicy': 'Never',
                'volumes': []
            }
        }, result)
