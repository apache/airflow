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
            env_from_configmap_ref='env_from_configmap',
            env_from_secret_ref='env_from_secret_a,env_from_secret_b',
            image_pull_secrets='pull_secret_a,pull_secret_b'
        )
        self.maxDiff = None

    def test_simple_pod_request_factory_create(self):
        expected_result = {
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
                            'name': 'env_from_configmap'
                        }
                    }, {
                        'secretRef': {
                            'name': 'env_from_secret_a'
                        }
                    }, {
                        'secretRef': {
                            'name': 'env_from_secret_b'
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
        result = self.simple_pod_request_factory.create(self.pod)
        self.assertDictEqual(result, expected_result)

    def test_xcom_pod_request_factory_create(self):
        result = self.xcom_pod_request_factory.create(self.pod)
        expected_result = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'myapp-pod',
                'labels': {'app': 'myapp'},
                'annotations': {}
            }, 'spec': {
                'volumes': [{'name': 'xcom', 'emptyDir': {}}],
                'containers': [{
                    'name': 'base',
                    'image': 'busybox',
                    'command': [
                        'sh', '-c', 'echo Hello Kubernetes!'
                    ],
                    'volumeMounts': [{
                        'name': 'xcom',
                        'mountPath': '/airflow/xcom'
                    }],
                    'imagePullPolicy': 'IfNotPresent',
                    'args': [],
                    'env': [
                        {'name': 'ENVIRONMENT', 'value': 'prod'},
                        {'name': 'LOG_LEVEL', 'value': 'warning'}
                    ], 'envFrom': [{
                        'configMapRef': {
                            'name': 'env_from_configmap'
                        }
                    }, {
                        'secretRef': {
                            'name': 'env_from_secret_a'
                        }
                    }, {
                        'secretRef': {
                            'name': 'env_from_secret_b'
                        }
                    }]
                }, {
                    'name': 'airflow-xcom-sidecar',
                    'image': 'python:3.5-alpine',
                    'command': ['python', '-c', ANY],
                    'volumeMounts': [{
                        'name': 'xcom', 'mountPath': '/airflow/xcom'
                    }]
                }],
                'restartPolicy': 'Never',
                'nodeSelector': {},
                'imagePullSecrets': [
                    {'name': 'pull_secret_a'},
                    {'name': 'pull_secret_b'}],
                'affinity': {}
            }
        }
        self.assertDictEqual(result, expected_result)
