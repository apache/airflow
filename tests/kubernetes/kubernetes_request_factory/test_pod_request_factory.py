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
import kubernetes.client.models as k8s
from kubernetes.client import ApiClient
import unittest


class TestPodRequestFactory(unittest.TestCase):

    def setUp(self):
        self.k8s_client = ApiClient()
        self.simple_pod_request_factory = SimplePodRequestFactory()
        self.xcom_pod_request_factory = ExtractXcomPodRequestFactory()
        self.pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={'app': 'myapp'},
                name='myapp-pod',
            ),
            spec=k8s.V1PodSpec(
                image_pull_secrets=[
                    k8s.V1LocalObjectReference('pull_secret_a'),
                    k8s.V1LocalObjectReference('pull_secret_b')
                ],
                containers=[k8s.V1Container(
                    image='busybox',
                    name='base',
                    env=[
                        k8s.V1EnvVar(name='ENVIRONMENT', value='prod'),
                        k8s.V1EnvVar(name='LOG_LEVEL', value='warning'),
                        k8s.V1EnvVar(name='TARGET', value_from=k8s.V1EnvVarSource(
                            secret_key_ref=k8s.V1SecretKeySelector(
                                key='source_b',
                                name='secret_b',
                            )
                        ))
                    ],
                    command=['sh', '-c', 'echo Hello Kubernetes!'],
                    env_from=[
                        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource('secret_a')),
                        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource('configmap_a')),
                        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource('configmap_b')),
                    ],
                    ports=[k8s.V1ContainerPort(name='foo', container_port=1234)],
                    volume_mounts=[k8s.V1VolumeMount(
                        mount_path='/etc/foo',
                        name='secretvol0',
                        read_only=True
                    )]
                )],
                security_context=k8s.V1PodSecurityContext(
                    run_as_user=1000,
                    fs_group=2000,
                ),
                volumes=[k8s.V1Volume(
                    name='secretvol0',
                    secret=k8s.V1SecretVolumeSource(secret_name='secret_b')
                )]
            ),
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
                    'ports': [{'name': 'foo', 'containerPort': 1234}],
                    'volumeMounts': [{
                        'mountPath': '/etc/foo',
                        'name': 'secretvol0',
                        'readOnly': True
                    }]
                }],
                'restartPolicy': 'Never',
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
                'securityContext': {
                    'runAsUser': 1000,
                    'fsGroup': 2000,
                },
            }
        }

    def test_simple_pod_request_factory_create(self):
        result = self.simple_pod_request_factory.create(self.pod)
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        # sort
        result_dict['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertDictEqual(result_dict, self.expected)

    def test_xcom_pod_request_factory_create(self):
        result = self.xcom_pod_request_factory.create(self.pod)
        result_dict = self.k8s_client.sanitize_for_serialization((result))
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': 'python:3.5-alpine',
            'command': ['python', '-c', self.xcom_pod_request_factory.XCOM_CMD],
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
        result_dict['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertEqual(result_dict, self.expected)
