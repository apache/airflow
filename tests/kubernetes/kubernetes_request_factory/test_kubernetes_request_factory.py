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

from airflow.kubernetes.kubernetes_request_factory.kubernetes_request_factory import KubernetesRequestFactory
from airflow.kubernetes.pod import Pod, Resources
from airflow.kubernetes.secret import Secret
from parameterized import parameterized
import unittest
import copy


class TestKubernetesRequestFactory(unittest.TestCase):

    def setUp(self):

        self.expected = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'name'
            },
            'spec': {
                'containers': [{
                    'name': 'base',
                    'image': 'airflow-worker:latest',
                    'command': [
                        "/usr/local/airflow/entrypoint.sh",
                        "/bin/bash sleep 25"
                    ],
                }],
                'restartPolicy': 'Never'
            }
        }
        self.input_req = copy.deepcopy(self.expected)

    def test_extract_image(self):
        image = 'v3.14'
        pod = Pod(image, {}, [])
        KubernetesRequestFactory.extract_image(pod, self.input_req)
        self.expected['spec']['containers'][0]['image'] = image
        self.assertEqual(self.input_req, self.expected)

    def test_extract_image_pull_policy(self):
        # Test when pull policy is not none
        pull_policy = 'IfNotPresent'
        pod = Pod('v3.14', {}, [], image_pull_policy=pull_policy)

        KubernetesRequestFactory.extract_image_pull_policy(pod, self.input_req)
        self.expected['spec']['containers'][0]['imagePullPolicy'] = pull_policy
        self.assertEqual(self.input_req, self.expected)

    def test_add_secret_to_env(self):
        secret = Secret('env', 'target', 'my-secret', 'KEY')
        secret_list = []
        self.expected = [{
            'name': 'TARGET',
            'valueFrom': {
                'secretKeyRef': {
                    'name': 'my-secret',
                    'key': 'KEY'
                }
            }
        }]
        KubernetesRequestFactory.add_secret_to_env(secret_list, secret)
        self.assertListEqual(secret_list, self.expected)

    def test_extract_labels(self):
        # Test when labels are not empty
        labels = {'label_a': 'val_a', 'label_b': 'val_b'}
        pod = Pod('v3.14', {}, [], labels=labels)
        self.expected['metadata']['labels'] = labels
        KubernetesRequestFactory.extract_labels(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_annotations(self):
        # Test when annotations are not empty
        annotations = {'annot_a': 'val_a', 'annot_b': 'val_b'}
        pod = Pod('v3.14', {}, [], annotations=annotations)
        self.expected['metadata']['annotations'] = annotations
        KubernetesRequestFactory.extract_annotations(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_affinity(self):
        # Test when affinity is not empty
        affinity = {'podAffinity': 'requiredDuringSchedulingIgnoredDuringExecution'}
        pod = Pod('v3.14', {}, [], affinity=affinity)
        self.expected['spec']['affinity'] = affinity
        KubernetesRequestFactory.extract_affinity(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_node_selector(self):
        # Test when affinity is not empty
        node_selectors = {'disktype': 'ssd', 'accelerator': 'nvidia-tesla-p100'}
        pod = Pod('v3.14', {}, [], node_selectors=node_selectors)
        self.expected['spec']['nodeSelector'] = node_selectors
        KubernetesRequestFactory.extract_node_selector(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_cmds(self):
        cmds = ['test-cmd.sh']
        pod = Pod('v3.14', {}, cmds)
        KubernetesRequestFactory.extract_cmds(pod, self.input_req)
        self.expected['spec']['containers'][0]['command'] = cmds
        self.assertEqual(self.input_req, self.expected)

    def test_extract_args(self):
        args = ['test_arg.sh']
        pod = Pod('v3.14', {}, [], args=args)
        KubernetesRequestFactory.extract_args(pod, self.input_req)
        self.expected['spec']['containers'][0]['args'] = args
        self.assertEqual(self.input_req, self.expected)

    def test_attach_volumes(self):
        # Test when volumes is not empty list
        volumes = ['vol_a', 'vol_b']
        pod = Pod('v3.14', {}, [], volumes=volumes)
        self.expected['spec']['volumes'] = volumes
        KubernetesRequestFactory.attach_volumes(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_attach_volume_mounts(self):
        # Test when volumes is not empty list
        volume_mounts = ['vol_a', 'vol_b']
        pod = Pod('v3.14', {}, [], volume_mounts=volume_mounts)
        self.expected['spec']['containers'][0]['volumeMounts'] = volume_mounts
        KubernetesRequestFactory.attach_volume_mounts(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_name(self):
        name = 'pod-name'
        pod = Pod('v3.14', {}, [], name=name)
        self.expected['metadata']['name'] = name
        KubernetesRequestFactory.extract_name(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_volume_secrets(self):
        # Test when secrets is not empty
        secrets = [
            Secret('volume', 'KEY1', 's1', 'key-1'),
            Secret('env', 'KEY2', 's2'),
            Secret('volume', 'KEY3', 's3', 'key-2')
        ]
        pod = Pod('v3.14', {}, [], secrets=secrets)
        self.expected['spec']['containers'][0]['volumeMounts'] = [{
            'mountPath': 'KEY1',
            'name': 'secretvol0',
            'readOnly': True
        }, {
            'mountPath': 'KEY3',
            'name': 'secretvol1',
            'readOnly': True
        }]
        self.expected['spec']['volumes'] = [{
            'name': 'secretvol0',
            'secret': {
                'secretName': 's1'
            }
        }, {
            'name': 'secretvol1',
            'secret': {
                'secretName': 's3'
            }
        }]
        KubernetesRequestFactory.extract_volume_secrets(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_env_and_secrets(self):
        # Test when secrets and envs are not empty
        secrets = [
            Secret('env', None, 's1'),
            Secret('volume', 'KEY2', 's2', 'key-2'),
            Secret('env', None, 's3')
        ]
        envs = {
            'ENV1': 'val1',
            'ENV2': 'val2'
        }
        configmaps = ['configmap_a', 'configmap_b']
        pod = Pod('v3.14', envs, [], secrets=secrets, configmaps=configmaps)
        self.expected['spec']['containers'][0]['env'] = [
            {'name': 'ENV1', 'value': 'val1'},
            {'name': 'ENV2', 'value': 'val2'},
        ]
        self.expected['spec']['containers'][0]['envFrom'] = [{
            'secretRef': {
                'name': 's1'
            }
        }, {
            'secretRef': {
                'name': 's3'
            }
        }, {
            'configMapRef': {
                'name': 'configmap_a'
            }
        }, {
            'configMapRef': {
                'name': 'configmap_b'
            }
        }]

        KubernetesRequestFactory.extract_env_and_secrets(pod, self.input_req)
        self.input_req['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertEqual(self.input_req, self.expected)

    def test_extract_resources(self):
        # Test when resources is not empty
        resources = Resources('1Gi', 1, '2Gi', 2)
        pod = Pod('v3.14', {}, [], resources=resources)
        self.expected['spec']['containers'][0]['resources'] = {
            'requests': {
                'memory': '1Gi',
                'cpu': 1
            },
            'limits': {
                'memory': '2Gi',
                'cpu': 2
            },
        }
        KubernetesRequestFactory.extract_resources(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_init_containers(self):
        init_container = 'init_container'
        pod = Pod('v3.14', {}, [], init_containers=init_container)
        self.expected['spec']['initContainers'] = init_container
        KubernetesRequestFactory.extract_init_containers(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_service_account_name(self):
        service_account_name = 'service_account_name'
        pod = Pod('v3.14', {}, [], service_account_name=service_account_name)
        self.expected['spec']['serviceAccountName'] = service_account_name
        KubernetesRequestFactory.extract_service_account_name(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_hostnetwork(self):
        hostnetwork = True
        pod = Pod('v3.14', {}, [], hostnetwork=hostnetwork)
        self.expected['spec']['hostNetwork'] = hostnetwork
        KubernetesRequestFactory.extract_hostnetwork(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_image_pull_secrets(self):
        image_pull_secrets = 'secret_a,secret_b,secret_c'
        pod = Pod('v3.14', {}, [], image_pull_secrets=image_pull_secrets)
        self.expected['spec']['imagePullSecrets'] = [
            {'name': 'secret_a'},
            {'name': 'secret_b'},
            {'name': 'secret_c'},
        ]
        KubernetesRequestFactory.extract_image_pull_secrets(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_extract_tolerations(self):
        tolerations = [{
            'key': 'key',
            'operator': 'Equal',
            'value': 'value',
            'effect': 'NoSchedule'
        }]
        pod = Pod('v3.14', {}, [], tolerations=tolerations)
        self.expected['spec']['tolerations'] = tolerations
        KubernetesRequestFactory.extract_tolerations(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    def test_security_context(self):
        security_context = {
            'runAsUser': 1000,
            'fsGroup': 2000
        }
        pod = Pod('v3.14', {}, [], security_context=security_context)
        self.expected['spec']['securityContext'] = security_context
        KubernetesRequestFactory.extract_security_context(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)

    @parameterized.expand([
        'extract_resources',
        'extract_init_containers',
        'extract_service_account_name',
        'extract_hostnetwork',
        'extract_image_pull_secrets',
        'extract_tolerations',
        'extract_security_context',
        'extract_volume_secrets'
    ])
    def test_identity(self, name):
        kube_request_factory_func = getattr(KubernetesRequestFactory, name)
        pod = Pod('v3.14', {}, [])
        kube_request_factory_func(pod, self.input_req)
        self.assertEqual(self.input_req, self.expected)
