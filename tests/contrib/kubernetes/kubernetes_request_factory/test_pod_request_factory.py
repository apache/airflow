# -*- coding: utf-8 -*-
#
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

from airflow.contrib.kubernetes.kubernetes_request_factory.\
    pod_request_factory import SimplePodRequestFactory, \
    ExtractXcomPodRequestFactory
from airflow.contrib.kubernetes.pod import Pod
from mock import ANY
import unittest


class TestSimplePodRequestFactory(unittest.TestCase):

    def setUp(self):
        self.simple_pod_request_factory = SimplePodRequestFactory()
        self.xcom_pod_request_factory = ExtractXcomPodRequestFactory()
        self.pod = Pod(
            image='busybox',
            envs={
                'ENVIRONMENT': 'prod',
                'LOG_LEVEL': 'warning'
            },
            name='myapp-pod',
            cmds=['sh', '-c', 'echo Hello Kubernetes!'],
            labels={'app': 'myapp'},
            image_pull_secrets='pull_secret_a,pull_secret_b',
            configmaps=['configmap_a', 'configmap_b'],
        )
        self.maxDiff = None
        self.expected_result = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'myapp-pod',
                'labels': {'app': 'myapp'},
                'annotations': {}},
            'spec': {
                'containers': [{
                    'name': 'base',
                    'image': 'busybox',
                    'command': [
                        'sh', '-c', 'echo Hello Kubernetes!'
                    ],
                    'imagePullPolicy': 'IfNotPresent',
                    'args': [],
                    'env': [{
                        'name': 'ENVIRONMENT',
                        'value': 'prod'
                    }, {
                        'name': 'LOG_LEVEL',
                        'value': 'warning'
                    }],
                    'envFrom': [{
                        'configMapRef': {
                            'name': 'configmap_a'
                        }
                    }, {
                        'configMapRef': {
                            'name': 'configmap_b'
                        }
                    }]
                }],
                'restartPolicy': 'Never',
                'nodeSelector': {},
                'volumes': [],
                'imagePullSecrets': [
                    {'name': 'pull_secret_a'},
                    {'name': 'pull_secret_b'}
                ],
                'affinity': {}
            }
        }

    def test_simple_pod_request_factory_create(self):
        result = self.simple_pod_request_factory.create(self.pod)
        self.assertDictEqual(result, self.expected_result)

    def test_xcom_pod_request_factory_create(self):
        result = self.xcom_pod_request_factory.create(self.pod)
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': 'python:3.5-alpine',
            'command': ['python', '-c', ANY],
            'volumeMounts': [{
                'name': 'xcom', 'mountPath': '/airflow/xcom'
                }]
        }
        volume_mount = [{
            'name': 'xcom',
            'mountPath': '/airflow/xcom'
        }]
        expected_result = self.expected_result.copy()
        expected_result['spec']['containers'].append(container_two)
        expected_result['spec']['containers'][0]['volumeMounts'] = volume_mount
        expected_result['spec']['volumes'] = [
            {'name': 'xcom', 'emptyDir': {}}
        ]
        self.assertDictEqual(result, expected_result)
