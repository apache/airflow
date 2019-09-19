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
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod_generator import PodGenerator, PodDefaults
from airflow.kubernetes.pod import Resources
from airflow.kubernetes.k8s_model import append_to_pod


class TestPodGenerator(unittest.TestCase):

    def setUp(self):
        self.envs = {
            'ENVIRONMENT': 'prod',
            'LOG_LEVEL': 'warning'
        }
        self.secrets = [
            # This should be a secretRef
            Secret('env', None, 'secret_a'),
            # This should be a single secret mounted in volumeMounts
            Secret('volume', '/etc/foo', 'secret_b'),
            # This should produce a single secret mounted in env
            Secret('env', 'TARGET', 'secret_b', 'source_b'),
        ]
        self.resources = Resources('1Gi', 1, '2Gi', 2, 1)
        self.k8s_client = ApiClient()
        self.expected = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'myapp-pod-0',
                'labels': {'app': 'myapp'},
                'namespace': 'default'
            },
            'spec': {
                'containers': [{
                    'name': 'base',
                    'image': 'busybox',
                    'args': [],
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
                        'configMapRef': {
                            'name': 'configmap_a'
                        }
                    }, {
                        'configMapRef': {
                            'name': 'configmap_b'
                        }
                    }, {
                        'secretRef': {
                            'name': 'secret_a'
                        }
                    }],
                    'resources': {
                        'requests': {
                            'memory': '1Gi',
                            'cpu': 1
                        },
                        'limits': {
                            'memory': '2Gi',
                            'cpu': 2,
                            'nvidia.com/gpu': 1
                        },
                    },
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
                'hostNetwork': False,
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

    @mock.patch('uuid.uuid4')
    def test_gen_pod(self, mock_uuid):
        mock_uuid.return_value = '0'
        pod_generator = PodGenerator(
            labels={'app': 'myapp'},
            name='myapp-pod',
            image_pull_secrets='pull_secret_a,pull_secret_b',
            image='busybox',
            envs=self.envs,
            cmds=['sh', '-c', 'echo Hello Kubernetes!'],
            security_context=k8s.V1PodSecurityContext(
                run_as_user=1000,
                fs_group=2000,
            ),
            namespace='default',
            ports=[k8s.V1ContainerPort(name='foo', container_port=1234)],
            configmaps=['configmap_a', 'configmap_b']
        )
        result = pod_generator.gen_pod()
        result = append_to_pod(result, self.secrets)
        result = self.resources.attach_to_pod(result)
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        # sort
        result_dict['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        result_dict['spec']['containers'][0]['envFrom'].sort(
            key=lambda x: list(x.values())[0]['name']
        )
        self.assertDictEqual(result_dict, self.expected)

    @mock.patch('uuid.uuid4')
    def test_gen_pod_extract_xcom(self, mock_uuid):
        mock_uuid.return_value = '0'
        pod_generator = PodGenerator(
            labels={'app': 'myapp'},
            name='myapp-pod',
            image_pull_secrets='pull_secret_a,pull_secret_b',
            image='busybox',
            envs=self.envs,
            cmds=['sh', '-c', 'echo Hello Kubernetes!'],
            namespace='default',
            security_context=k8s.V1PodSecurityContext(
                run_as_user=1000,
                fs_group=2000,
            ),
            ports=[k8s.V1ContainerPort(name='foo', container_port=1234)],
            configmaps=['configmap_a', 'configmap_b']
        )
        pod_generator.extract_xcom = True
        result = pod_generator.gen_pod()
        result = append_to_pod(result, self.secrets)
        result = self.resources.attach_to_pod(result)
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': "alpine",
            'command': ['sh', '-c', PodDefaults.XCOM_CMD],
            'volumeMounts': [
                {
                    'name': 'xcom',
                    'mountPath': '/airflow/xcom'
                }
            ],
            'resources': {'requests': {'cpu': '1m'}},
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

    @mock.patch('uuid.uuid4')
    def test_from_obj(self, mock_uuid):
        mock_uuid.return_value = '0'
        result = PodGenerator.from_obj({
            "KubernetesExecutor": {
                "annotations": {"test": "annotation"},
                "volumes": [
                    {
                        "name": "example-kubernetes-test-volume",
                        "hostPath": {"path": "/tmp/"},
                    },
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/foo/",
                        "name": "example-kubernetes-test-volume",
                    },
                ],
                "securityContext": {
                    "runAsUser": 1000
                }
            }
        })
        result = self.k8s_client.sanitize_for_serialization(result)

        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'annotations': {'test': 'annotation'},
            },
            'spec': {
                'containers': [{
                    'args': [],
                    'command': [],
                    'env': [],
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'volumeMounts': [{
                        'mountPath': '/foo/',
                        'name': 'example-kubernetes-test-volume'
                    }],
                }],
                'imagePullSecrets': [],
                'volumes': [{
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume'
                }],
            }
        }, result)

    def test_reconcile_pods(self):
        with mock.patch('uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = '0'
            base_pod = PodGenerator(
                image='image1',
                name='name1',
                envs={'key1': 'val1'},
                cmds=['/bin/command1.sh', 'arg1'],
                ports=k8s.V1ContainerPort(name='port', container_port=2118),
                volumes=[{
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume1'
                }],
                volume_mounts=[{
                    'mountPath': '/foo/',
                    'name': 'example-kubernetes-test-volume1'
                }],
            ).gen_pod()

            mutator_pod = PodGenerator(
                envs={'key2': 'val2'},
                image='',
                name='name2',
                cmds=['/bin/command2.sh', 'arg2'],
                volumes=[{
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume2'
                }],
                volume_mounts=[{
                    'mountPath': '/foo/',
                    'name': 'example-kubernetes-test-volume2'
                }]
            ).gen_pod()

            result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
            result = self.k8s_client.sanitize_for_serialization(result)
            self.assertEqual(result, {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {'name': 'name2-0'},
                'spec': {
                    'containers': [{
                        'args': [],
                        'command': ['/bin/command1.sh', 'arg1'],
                        'env': [
                            {'name': 'key1', 'value': 'val1'},
                            {'name': 'key2', 'value': 'val2'}
                        ],
                        'envFrom': [],
                        'image': 'image1',
                        'imagePullPolicy': 'IfNotPresent',
                        'name': 'base',
                        'ports': {
                            'containerPort': 2118,
                            'name': 'port',
                        },
                        'volumeMounts': [{
                            'mountPath': '/foo/',
                            'name': 'example-kubernetes-test-volume1'
                        }, {
                            'mountPath': '/foo/',
                            'name': 'example-kubernetes-test-volume2'
                        }]
                    }],
                    'hostNetwork': False,
                    'imagePullSecrets': [],
                    'restartPolicy': 'Never',
                    'volumes': [{
                        'hostPath': {'path': '/tmp/'},
                        'name': 'example-kubernetes-test-volume1'
                    }, {
                        'hostPath': {'path': '/tmp/'},
                        'name': 'example-kubernetes-test-volume2'
                    }]
                }
            })
