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

from airflow.kubernetes.kubernetes_request_factory.pod_request_factory import SimplePodRequestFactory, \
    ExtractXcomPodRequestFactory
from airflow.kubernetes.pod import Pod
from airflow.kubernetes.secret import Secret
from airflow.exceptions import AirflowConfigException
import unittest

XCOM_CMD = """import time
while True:
    try:
        time.sleep(3600)
    except KeyboardInterrupt:
        exit(0)
"""


class TestPodRequestFactory(unittest.TestCase):

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
            secrets=[
                # This should be a secretRef
                Secret('env', None, 'secret_a'),
                # This should be a single secret mounted in volumeMounts
                Secret('volume', '/etc/foo', 'secret_b'),
                # This should produce a single secret mounted in env
                Secret('env', 'TARGET', 'secret_b', 'source_b'),
            ],
            security_context={
                'runAsUser': 1000,
                'fsGroup': 2000,
            }
        )
        self.maxDiff = None
        self.expected = {
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
                    }, {
                        'name': 'TARGET',
                        'valueFrom': {
                            'secretKeyRef': {
                                'name': 'secret_b',
                                'key': 'source_b'
                            }
                        }
                    }],
                    'envFrom': [{
                        'secretRef': {
                            'name': 'secret_a'
                        }
                    }, {
                        'configMapRef': {
                            'name': 'configmap_a'
                        }
                    }, {
                        'configMapRef': {
                            'name': 'configmap_b'
                        }
                    }],
                    'volumeMounts': [{
                        'mountPath': '/etc/foo',
                        'name': 'secretvol0',
                        'readOnly': True
                    }]
                }],
                'restartPolicy': 'Never',
                'nodeSelector': {},
                'volumes': [{
                    'name': 'secretvol0',
                    'secret': {
                        'secretName': 'secret_b'
                    }
                }],
                'imagePullSecrets': [
                    {'name': 'pull_secret_a'},
                    {'name': 'pull_secret_b'}
                ],
                'affinity': {},
                'securityContext': {
                    'runAsUser': 1000,
                    'fsGroup': 2000,
                },
            }
        }

    def test_secret_throws(self):
        with self.assertRaises(AirflowConfigException):
            Secret('volume', None, 'secret_a', 'key')

    def test_simple_pod_request_factory_create(self):
        result = self.simple_pod_request_factory.create(self.pod)
        # sort
        result['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertEqual(result, self.expected)

    def test_xcom_pod_request_factory_create(self):
        result = self.xcom_pod_request_factory.create(self.pod)
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': 'python:3.5-alpine',
            'command': ['python', '-c', XCOM_CMD],
            'volumeMounts': [
                {
                    'name': 'xcom',
                    'mountPath': '/airflow/xcom'
                }
            ]
        }
        self.expected['spec']['containers'].append(container_two)
        self.expected['spec']['containers'][0]['volumeMounts'].insert(0, {
            'name': 'xcom',
            'mountPath': '/airflow/xcom'
        })
        self.expected['spec']['volumes'].insert(0, {
            'name': 'xcom', 'emptyDir': {}
        })
        result['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertEqual(result, self.expected)
